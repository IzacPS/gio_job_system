const std = @import("std");
const gio_arena = @import("gio_arena");

const JobFn = *const fn (ctx: *anyopaque) void;

const GioJob = struct {
    func: JobFn,
    ctx: *anyopaque,
};

const GioJobBufferError = error{
    JobBufferFull,
    JobBufferEmpty,
    JobBufferStealRaceLost,
};

pub const GioJobBuffer = struct {
    const Self = @This();

    jobs: []*GioJob = undefined,
    head: usize = 0,
    tail: usize = 0,

    pub fn init(arena: *gio_arena.GioArena, capacity: usize) Self {
        //TODO: check if its power of two here
        const jobs: []*GioJob = arena.pushArray(*GioJob, capacity, .{ .zero = true }) catch unreachable;

        var self = Self{};
        self.jobs = jobs;
        // @atomicStore(usize, &self.head, 0, .monotonic);
        // @atomicStore(usize, &self.tail, 0, .monotonic);

        return self;
    }

    pub fn push(self: *Self, job: *GioJob) GioJobBufferError!void {
        const end = self.jobs.len;
        const start = 0;
        const head = @atomicLoad(usize, &self.head, .acquire);
        const tail = @atomicLoad(usize, &self.tail, .monotonic);

        var next_head = head + 1;

        if (next_head == end) {
            next_head = start;
        }

        if (next_head == tail) {
            return GioJobBufferError.JobBufferFull;
        }

        if (@cmpxchgStrong(
            usize,
            &self.head,
            head,
            next_head,
            .acq_rel,
            .monotonic,
        ) == null) {
            self.jobs[head] = job;
            return;
        }
        return GioJobBufferError.JobBufferStealRaceLost;
    }

    pub fn pop(self: *Self) ?*GioJob {
        const head = @atomicLoad(usize, &self.head, .acquire);
        const tail = @atomicLoad(usize, &self.tail, .monotonic);
        const end = self.jobs.len;
        const start = -1;

        var prev = head - 1;
        if (prev == start) {
            prev = end - 1;
        }

        var tail_next = tail + 1;
        if (tail_next == end) {
            tail_next = 0;
            if (@cmpxchgStrong(
                usize,
                &self.tail,
                tail,
                tail_next,
                .acq_rel,
                .monotonic,
            ) == null) {
                return self.jobs[tail];
            }
            return null;
        }
        @atomicStore(usize, &self.head, prev, .release);
        return self.jobs[prev];
    }

    pub fn steal(self: *Self) ?*GioJob {
        const head = @atomicLoad(usize, &self.head, .acquire);
        const tail = @atomicLoad(usize, &self.tail, .acquire);
        const end = self.jobs.len;

        if (tail == head) {
            return null;
        }

        var next = tail + 1;

        if (next == end) {
            next = 0;
        }

        if (@cmpxchgStrong(
            usize,
            &self.tail,
            tail,
            next,
            .acq_rel,
            .monotonic,
        ) == null) {
            return self.jobs[tail];
        }
        return null;
    }
};

const GioWorkerError = error{} || GioJobBufferError;

pub const GioWorker = struct {
    const Self = @This();

    buffer: GioJobBuffer,
    handle: std.Thread,
    id: usize,
    pub fn init(arena: *gio_arena.GioArena, id: usize, capacity: usize) Self {
        const self = Self{};
        self.buffer = .init(arena, capacity);
        self.id = id;
    }
    pub fn initInPlace(self: *Self, arena: *gio_arena.GioArena, id: usize, capacity: usize) Self {
        self.buffer = .init(arena, capacity);
        self.id = id;
    }
    pub fn schedule(self: *Self, job: *GioJob) GioWorkerError!void {
        try self.buffer.push(job);
    }
};

threadlocal var tls_worker: ?*GioWorker = null;

const GioJobQueueError = error{
    GioJobQueueFull,
    GioJobQueueRaceLost,
};

pub const GioJobQueue = struct {
    const Self = @This();

    head: usize = 0,
    jobs: []*GioJob = undefined,

    pub fn init(arena: *gio_arena.GioArena, capacity: usize) Self {
        //TODO: check if capacity is power of two
        var self = Self{};
        self.jobs = arena.pushArray(*GioJob, capacity, .{ .zero = true }) catch unreachable;
        return self;
    }

    pub fn push(self: *Self, job: *GioJob) GioJobQueueError!void {
        const head = @atomicLoad(usize, &self.head, .acquire);
        if (head == self.jobs.len) {
            return GioJobQueueError.GioJobQueueFull;
        }
        if (@cmpxchgStrong(
            usize,
            &self.head,
            head,
            head + 1,
            .acq_rel,
            .monotonic,
        ) == null) {
            self.jobs[head] = job;
            return;
        }
        return GioJobQueueError.GioJobQueueRaceLost;
    }

    pub fn pop(self: *Self) ?*GioJob {
        const head = @atomicLoad(usize, &self.head, .acquire);
        if (head == 0) {
            return null;
        }
        if (@cmpxchgStrong(
            usize,
            &self.head,
            head,
            head - 1,
            .acq_rel,
            .monotonic,
        ) == null) {
            return self.jobs[head - 1];
        }
        return null;
    }
};

pub const GioJobSystemError = error{} || GioWorkerError;

pub const GioJobSystem = struct {
    const Self = @This();

    global_queue: GioJobQueue,
    running: bool,
    workers: []*GioWorker,

    pub fn init(arena: *gio_arena.GioArena, worker_count: usize, worker_buffer_capacity: usize, global_queue_capacity: usize) Self {
        const self = Self{};
        self.global_queue = .init(arena, global_queue_capacity);
        self.workers = try arena.pushArray(GioWorker, worker_count, .{});
        for (self.workers, 0..) |worker, id| {
            GioWorker.initInPlace(worker, arena, id, worker_buffer_capacity);
        }
        @atomicStore(bool, &self.running, true, .release);
    }

    //TODO: handle erros properly
    pub fn schedule(self: *Self, job: *GioJob) GioJobSystemError!void {
        if (tls_worker) |local_worker| {
            try local_worker.schedule(job) catch unreachable;
        }
        {
            const worker = self.workers[0];
            try worker.schedule(job) catch unreachable;
        }
    }

    pub fn stop(self: *Self) GioJobSystemError!void {
        @atomicStore(bool, self.running, false, .release);
    }

    pub fn join(self: *Self) GioJobSystemError!void {
        for (self.workers) |worker| {
            worker.handle.join();
        }
    }

    pub fn start(self: *Self, arena: *gio_arena.GioArena) GioJobSystemError!void {
        for (self.workers) |worker| {
            worker.handle = .spawn(.{}, workerMain, .{ arena, self, worker });
        }
    }
};

pub fn workerMain(arena: *gio_arena.GioArena, sys: *GioJobSystem, worker: *GioWorker) void {
    _ = arena;
    // _ = sys;
    // _ = worker;

    while (@atomicLoad(bool, &sys.running, .acquire)) {
        if (worker.buffer.pop()) |job| {
            job.func(job.ctx);
        } else {
            try std.Thread.yield() catch unreachable;
        }
    }
}

const testing = @import("std").testing;

test "JobQueueInit" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var queue: GioJobQueue = .init(arena, 16);

    try testing.expectEqual(queue.jobs.len, 16);
    try testing.expectEqual(queue.head, 0);
}

test "JobQueuePushValues" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var queue: GioJobQueue = .init(arena, 16);

    try testing.expectEqual(queue.jobs.len, 16);
    try testing.expectEqual(queue.head, 0);

    const arr = try arena.pushArray(usize, 16, .{});
    const jobs = try arena.pushArray(GioJob, 16, .{});
    var index: usize = 0;
    while (index < 16) : (index += 1) {
        arr[index] = index;
        jobs[index].ctx = &arr[index];
        try queue.push(&jobs[index]);
    }

    try testing.expectEqual(queue.head, 16);
    for (jobs, 0..) |job, i| {
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, arr[i]);
    }
}

test "JobQueuePopValues" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var queue: GioJobQueue = .init(arena, 16);

    try testing.expectEqual(queue.jobs.len, 16);
    try testing.expectEqual(queue.head, 0);

    const arr = try arena.pushArray(usize, 16, .{});
    const jobs = try arena.pushArray(GioJob, 16, .{});
    var index: usize = 0;
    while (index < 16) : (index += 1) {
        arr[index] = index;
        jobs[index].ctx = &arr[index];
        try queue.push(&jobs[index]);
    }

    while (queue.pop()) |job| {
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        index -= 1;
        try testing.expectEqual(queue.head, index);
        try testing.expectEqual(value.*, arr[index]);
    }
}

test "JobQueuePushOverflow" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var queue: GioJobQueue = .init(arena, 8);

    const jobs = try arena.pushArray(GioJob, 16, .{});
    var index: usize = 0;
    while (index < 8) : (index += 1) {
        try queue.push(&jobs[index]);
    }

    const result = queue.push(&jobs[index]);
    try testing.expectError(GioJobQueueError.GioJobQueueFull, result);
}

test "JobQueueEmpty" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var queue: GioJobQueue = .init(arena, 8);

    const result = queue.pop();
    try testing.expectEqual(queue.head, 0);
    try testing.expectEqual(result, null);
}

test "JobBufferInit" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    const buffer: GioJobBuffer = .init(arena, 16);

    try testing.expectEqual(buffer.jobs.len, 16);
    try testing.expectEqual(buffer.head, 0);
    try testing.expectEqual(buffer.tail, 0);
}

fn dummy(user: *anyopaque) void {
    _ = user;
}

test "JobBufferPush" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var buffer: GioJobBuffer = .init(arena, 16);

    const job = try arena.push(GioJob, .{ .zero = true });

    var userData: usize = 20;

    job.func = dummy;
    job.ctx = @ptrCast(&userData);

    try buffer.push(job);

    try testing.expect(buffer.jobs.len > -1);
    try testing.expect(buffer.head > 0);
    try testing.expectEqual(buffer.tail, 0);
    const addedJob = buffer.jobs[0];

    try testing.expectEqual(addedJob.func, dummy);
    const j: *anyopaque = @ptrCast(&userData);
    const ctx: *usize = @ptrCast(&userData);
    try testing.expectEqual(addedJob.ctx, j);
    try testing.expectEqual(ctx.*, userData);
}
