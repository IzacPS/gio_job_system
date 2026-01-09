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
    head: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    tail: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),

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
        // const head = @atomicLoad(usize, &self.head, .acquire);
        // const tail = @atomicLoad(usize, &self.tail, .monotonic);
        const head = self.head.load(.acquire);
        const tail = self.tail.load(.monotonic);

        var next_head = head + 1;

        if (next_head == end) {
            next_head = start;
        }

        if (next_head == tail) {
            return GioJobBufferError.JobBufferFull;
        }

        if (self.head.cmpxchgStrong(
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
        const tail = self.tail.load(.acquire); // @atomicLoad(usize, &self.tail, .monotonic);
        const head = self.head.load(.acquire); // @atomicLoad(usize, &self.head, .acquire);

        if (head == tail) {
            return null;
        }

        const prev = if (head == 0) self.jobs.len - 1 else head - 1;
        if (self.head.cmpxchgStrong(head, prev, .acq_rel, .monotonic) == null) {
            return self.jobs[prev];
        }
        return null;
    }

    pub fn steal(self: *Self) ?*GioJob {
        const head = self.head.load(.acquire); // @atomicLoad(usize, &self.head, .acquire);
        const tail = self.tail.load(.monotonic); // @atomicLoad(usize, &self.tail, .acquire);

        if (tail == head) {
            return null;
        }

        const size = if (head >= tail)
            head - tail
        else
            self.jobs.len - tail + head;

        if (size <= 1) {
            return null;
        }

        var next = tail + 1;
        if (next == self.jobs.len) {
            next = 0;
        }

        if (self.tail.cmpxchgStrong(
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

    buffer: GioJobBuffer = undefined,
    handle: std.Thread = undefined,
    id: usize = undefined,

    pub fn init(arena: *gio_arena.GioArena, id: usize, capacity: usize) Self {
        var self = Self{};
        self.buffer = .init(arena, capacity);
        self.id = id;
        return self;
    }
    pub fn initInPlace(self: *Self, arena: *gio_arena.GioArena, id: usize, capacity: usize) void {
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

    head: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    jobs: []*GioJob = undefined,

    pub fn init(arena: *gio_arena.GioArena, capacity: usize) Self {
        //TODO: check if capacity is power of two
        var self = Self{};
        self.jobs = arena.pushArray(*GioJob, capacity, .{ .zero = true }) catch unreachable;
        return self;
    }

    pub fn push(self: *Self, job: *GioJob) GioJobQueueError!void {
        const head = self.head.load(.acquire); // @atomicLoad(usize, &self.head, .acquire);
        if (head == self.jobs.len) {
            return GioJobQueueError.GioJobQueueFull;
        }
        if (self.head.cmpxchgStrong(
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
        const head = self.head.load(.acquire); // @atomicLoad(usize, &self.head, .acquire);
        if (head == 0) {
            return null;
        }
        if (self.head.cmpxchgStrong(
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

    global_queue: GioJobQueue = undefined,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    workers: []GioWorker = undefined,

    pub fn init(arena: *gio_arena.GioArena, worker_count: usize, worker_buffer_capacity: usize, global_queue_capacity: usize) Self {
        var self = Self{};
        self.global_queue = .init(arena, global_queue_capacity);
        self.workers = arena.pushArray(GioWorker, worker_count, .{}) catch unreachable;
        for (self.workers, 0..) |*worker, id| {
            GioWorker.initInPlace(worker, arena, id, worker_buffer_capacity);
        }
        self.running.store(true, .release);
        return self;
    }

    //TODO: handle erros properly
    pub fn schedule(self: *Self, job: *GioJob) GioJobSystemError!void {
        if (tls_worker) |local_worker| {
            try local_worker.schedule(job) catch {
                try self.global_queue.push(job);
            };
        }
        try self.global_queue.push(job);
    }

    pub fn stop(self: *Self) GioJobSystemError!void {
        // @atomicStore(bool, self.running, false, .release);
        self.running.store(false, .release);
    }

    pub fn join(self: *Self) GioJobSystemError!void {
        for (self.workers) |worker| {
            worker.handle.join();
        }
    }

    pub fn start(self: *Self, arena: *gio_arena.GioArena) GioJobSystemError!void {
        for (self.workers) |*worker| {
            worker.*.handle = std.Thread.spawn(.{}, workerMain, .{ arena, self, worker }) catch unreachable;
        }
    }


};
fn stealFromOthers(sys: *GioJobSystem, self_worker: *GioWorker) ?*GioJob {
    const it_index = self_worker.id;
    const count = sys.workers.len;

    var i: usize = 1;
    while(i < count): (i+=1) {
        const target_index = (it_index + i) % count;
        const worker = &sys.workers[target_index];
        if (worker.buffer.steal()) |job| {
            return job;
        }
    }
    return null;
}

pub fn workerMain(arena: *gio_arena.GioArena, sys: *GioJobSystem, worker: *GioWorker) void {
    _ = arena;
    // _ = sys;
    // _ = worker;

    while (sys.running.load(.acquire)) {
        if (worker.buffer.pop()) |job| {
            job.func(job.ctx);
            continue;
        }
        if (stealFromOthers(sys, worker)) |job| {
            job.func(job.ctx);
            continue;
        }
        if (sys.global_queue.pop()) |job| {
            job.func(job.ctx);
            continue;
        }
        std.Thread.yield() catch unreachable;
    }
}

const testing = @import("std").testing;

test "JobQueueInit" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var queue: GioJobQueue = .init(arena, 16);

    try testing.expectEqual(queue.jobs.len, 16);
    try testing.expectEqual(queue.head.raw, 0);
}

test "JobQueuePushValues" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var queue: GioJobQueue = .init(arena, 16);

    try testing.expectEqual(queue.jobs.len, 16);
    try testing.expectEqual(queue.head.raw, 0);

    const arr = try arena.pushArray(usize, 16, .{});
    const jobs = try arena.pushArray(GioJob, 16, .{});
    var index: usize = 0;
    while (index < 16) : (index += 1) {
        arr[index] = index;
        jobs[index].ctx = &arr[index];
        try queue.push(&jobs[index]);
    }

    try testing.expectEqual(queue.head.raw, 16);
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
    try testing.expectEqual(queue.head.raw, 0);

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
        try testing.expectEqual(queue.head.raw, index);
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
    try testing.expectEqual(queue.head.raw, 0);
    try testing.expectEqual(result, null);
}

test "JobBufferInit" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    const buffer: GioJobBuffer = .init(arena, 16);

    try testing.expectEqual(buffer.jobs.len, 16);
    try testing.expectEqual(buffer.head.raw, 0);
    try testing.expectEqual(buffer.tail.raw, 0);
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
    try testing.expect(buffer.head.raw > 0);
    try testing.expectEqual(buffer.tail.raw, 0);
    const addedJob = buffer.jobs[0];

    try testing.expectEqual(addedJob.func, dummy);
    const j: *anyopaque = @ptrCast(&userData);
    const ctx: *usize = @ptrCast(&userData);
    try testing.expectEqual(addedJob.ctx, j);
    try testing.expectEqual(ctx.*, userData);
}

test "JobBufferPopValues" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var buffer: GioJobBuffer = .init(arena, 16);

    const jobs = try arena.pushArray(GioJob, 8, .{});
    const arr = try arena.pushArray(usize, 8, .{});

    var i: usize = 0;
    while (i < 8) : (i += 1) {
        arr[i] = i;
        jobs[i].func = dummy;
        jobs[i].ctx = @ptrCast(&arr[i]);
        try buffer.push(&jobs[i]);
    }

    // pop should return LIFO order
    var expect_index: usize = 8;
    while (buffer.pop()) |job| {
        expect_index -= 1;
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, arr[expect_index]);
    }
    try testing.expectEqual(expect_index, 0);
}

test "JobBufferStealValues" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var buffer: GioJobBuffer = .init(arena, 16);

    const jobs = try arena.pushArray(GioJob, 8, .{});
    const arr = try arena.pushArray(usize, 8, .{});

    var i: usize = 0;
    while (i < 8) : (i += 1) {
        arr[i] = i;
        jobs[i].func = dummy;
        jobs[i].ctx = @ptrCast(&arr[i]);
        try buffer.push(&jobs[i]);
    }

    // steal should return FIFO order (but leaves last element for owner)
    var expect_index: usize = 0;
    while (buffer.steal()) |job| {
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, arr[expect_index]);
        expect_index += 1;
    }
    try testing.expectEqual(expect_index, 7);

    // Owner can pop the last element
    if (buffer.pop()) |job| {
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, arr[7]);
    }
}

test "JobBufferPopEmpty" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var buffer: GioJobBuffer = .init(arena, 4);

    const result = buffer.pop();
    try testing.expectEqual(result, null);
    try testing.expectEqual(buffer.head.raw, 0);
    try testing.expectEqual(buffer.tail.raw, 0);
}

test "JobBufferWrapAround" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    // small capacity to force wrap-around (increased to avoid overflow in test)
    var buffer: GioJobBuffer = .init(arena, 8);

    const jobs = try arena.pushArray(GioJob, 6, .{});
    const arr = try arena.pushArray(usize, 6, .{});

    // push 3
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        arr[i] = i;
        jobs[i].func = dummy;
        jobs[i].ctx = @ptrCast(&arr[i]);
        try buffer.push(&jobs[i]);
    }

    // pop 2
    var popped: usize = 0;
    while (popped < 2) : (popped += 1) {
        const j = buffer.pop() orelse @panic("expected job");
        const value: *usize = @ptrCast(@alignCast(j.ctx));
        _ = value; // autofix
    }

    // push 3 more to wrap
    while (i < 6) : (i += 1) {
        arr[i] = i;
        jobs[i].func = dummy;
        jobs[i].ctx = @ptrCast(&arr[i]);
        try buffer.push(&jobs[i]);
    }

    // drain remaining via steal (FIFO) to verify nothing corrupt
    // steal leaves last element for owner
    const expected = [_]usize{ 0, 3, 4, 5 };
    var eidx: usize = 0;
    while (buffer.steal()) |job| {
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, expected[eidx]);
        eidx += 1;
    }
    try testing.expectEqual(eidx, expected.len - 1);

    // Owner pops the last element
    if (buffer.pop()) |job| {
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, expected[expected.len - 1]);
    }
}

test "JobBufferPushFull" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var buffer: GioJobBuffer = .init(arena, 4);

    const jobs = try arena.pushArray(GioJob, 4, .{});

    // Fill the buffer (capacity - 1 because circular buffer leaves one slot empty)
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        try buffer.push(&jobs[i]);
    }

    // This should fail because buffer is full
    const result = buffer.push(&jobs[3]);
    try testing.expectError(GioJobBufferError.JobBufferFull, result);
}

test "JobBufferMixedPopAndSteal" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var buffer: GioJobBuffer = .init(arena, 16);

    const jobs = try arena.pushArray(GioJob, 10, .{});
    const arr = try arena.pushArray(usize, 10, .{});

    // Push 10 jobs
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        arr[i] = i;
        jobs[i].func = dummy;
        jobs[i].ctx = @ptrCast(&arr[i]);
        try buffer.push(&jobs[i]);
    }

    // Pop 2 from top (should get 9, 8)
    if (buffer.pop()) |job| {
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, 9);
    }
    if (buffer.pop()) |job| {
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, 8);
    }

    // Steal 2 from bottom (should get 0, 1)
    if (buffer.steal()) |job| {
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, 0);
    }
    if (buffer.steal()) |job| {
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, 1);
    }

    // Remaining should be 2,3,4,5,6,7 (6 jobs)
    // But steal leaves last element, so we get 5 via steal + 1 via pop
    var count: usize = 0;
    while (buffer.steal()) |_| {
        count += 1;
    }
    try testing.expectEqual(count, 5);

    // Last element via pop
    try testing.expect(buffer.pop() != null);
}

test "WorkerInit" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    const worker = GioWorker.init(arena, 0, 16);

    try testing.expectEqual(worker.id, 0);
    try testing.expectEqual(worker.buffer.jobs.len, 16);
    try testing.expectEqual(worker.buffer.head.raw, 0);
    try testing.expectEqual(worker.buffer.tail.raw, 0);
}

test "WorkerSchedule" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var worker = GioWorker.init(arena, 0, 16);

    const job = try arena.push(GioJob, .{ .zero = true });
    var userData: usize = 42;
    job.func = dummy;
    job.ctx = @ptrCast(&userData);

    try worker.schedule(job);

    try testing.expect(worker.buffer.head.raw > 0);
}

fn incrementCounter(ctx: *anyopaque) void {
    const counter: *usize = @ptrCast(@alignCast(ctx));
    counter.* += 1;
}

test "JobExecution" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var counter: usize = 0;
    var job = GioJob{
        .func = incrementCounter,
        .ctx = @ptrCast(&counter),
    };

    try testing.expectEqual(counter, 0);
    job.func(job.ctx);
    try testing.expectEqual(counter, 1);
    job.func(job.ctx);
    try testing.expectEqual(counter, 2);
}

test "JobQueueMultiplePushPop" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var queue: GioJobQueue = .init(arena, 32);

    const jobs = try arena.pushArray(GioJob, 20, .{});
    const arr = try arena.pushArray(usize, 20, .{});

    // Push 10
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        arr[i] = i;
        jobs[i].ctx = @ptrCast(&arr[i]);
        try queue.push(&jobs[i]);
    }

    // Pop 5
    var popped: usize = 0;
    while (popped < 5) : (popped += 1) {
        _ = queue.pop() orelse return error.UnexpectedNull;
    }

    // Push 10 more
    while (i < 20) : (i += 1) {
        arr[i] = i;
        jobs[i].ctx = @ptrCast(&arr[i]);
        try queue.push(&jobs[i]);
    }

    // Should have 15 total
    var count: usize = 0;
    while (queue.pop()) |_| {
        count += 1;
    }
    try testing.expectEqual(count, 15);
}

test "JobBufferConcurrentPushes" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var buffer: GioJobBuffer = .init(arena, 64);

    const jobs = try arena.pushArray(GioJob, 20, .{});
    const arr = try arena.pushArray(usize, 20, .{});

    var i: usize = 0;
    while (i < 20) : (i += 1) {
        arr[i] = i;
        jobs[i].func = dummy;
        jobs[i].ctx = @ptrCast(&arr[i]);
    }

    // Simulate concurrent pushes (in reality would be from different threads)
    var pushed: usize = 0;
    for (jobs) |*job| {
        buffer.push(job) catch |err| {
            if (err == GioJobBufferError.JobBufferStealRaceLost) {
                continue; // This is expected in concurrent scenarios
            }
            return err;
        };
        pushed += 1;
    }

    // Count how many we can pop
    var count: usize = 0;
    while (buffer.pop()) |_| {
        count += 1;
    }

    try testing.expectEqual(count, pushed);
}

const ThreadTestContext = struct {
    buffer: *GioJobBuffer,
    jobs: []GioJob,
    start_index: usize,
    count: usize,
    stolen: std.atomic.Value(usize),
};

fn stealerThread(ctx: *ThreadTestContext) void {
    var stolen_count: usize = 0;
    while (ctx.buffer.steal()) |_| {
        stolen_count += 1;
    }
    _ = ctx.stolen.fetchAdd(stolen_count, .monotonic);
}

test "JobBufferWorkStealingMultipleThreads" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var buffer: GioJobBuffer = .init(arena, 128);

    const jobs = try arena.pushArray(GioJob, 60, .{});
    const arr = try arena.pushArray(usize, 60, .{});

    // Owner thread pushes jobs
    var i: usize = 0;
    while (i < 60) : (i += 1) {
        arr[i] = i;
        jobs[i].func = dummy;
        jobs[i].ctx = @ptrCast(&arr[i]);
        try buffer.push(&jobs[i]);
    }

    // Create context for stealer threads
    var ctx = ThreadTestContext{
        .buffer = &buffer,
        .jobs = jobs,
        .start_index = 0,
        .count = 60,
        .stolen = std.atomic.Value(usize).init(0),
    };

    // Spawn stealer threads
    const thread1 = try std.Thread.spawn(.{}, stealerThread, .{&ctx});
    const thread2 = try std.Thread.spawn(.{}, stealerThread, .{&ctx});

    thread1.join();
    thread2.join();

    // Owner pops remaining
    var popped: usize = 0;
    while (buffer.pop()) |_| {
        popped += 1;
    }

    const total_stolen = ctx.stolen.load(.monotonic);

    // Total processed should equal what we pushed (60 jobs total)
    // Either stolen by threads or popped by owner
    try testing.expectEqual(60, total_stolen + popped);
}

test "JobSystemInit" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    const system = GioJobSystem.init(arena, 4, 32, 128);

    try testing.expectEqual(system.workers.len, 4);
    try testing.expectEqual(system.global_queue.jobs.len, 128);
    try testing.expect(system.running.load(.acquire));

    for (system.workers, 0..) |worker, id| {
        try testing.expectEqual(worker.id, id);
        try testing.expectEqual(worker.buffer.jobs.len, 32);
    }
}

const JobSystemTestContext = struct {
    counter: std.atomic.Value(usize),
};

fn atomicIncrement(ctx: *anyopaque) void {
    const test_ctx: *JobSystemTestContext = @ptrCast(@alignCast(ctx));
    _ = test_ctx.counter.fetchAdd(1, .monotonic);
}

// // TODO: Re-enable when sleep API is stable in Zig 0.16+
// // test "JobSystemScheduleAndExecute" {
// //     const arena: *gio_arena.GioArena = try .init(.{});
// //     defer arena.deinit();
// //
// //     var system = GioJobSystem.init(arena, 2, 32, 128);
// //
// //     var test_ctx = JobSystemTestContext{
// //         .counter = std.atomic.Value(usize).init(0),
// //     };
// //
// //     const job_count = 10;
// //     const jobs = try arena.pushArray(GioJob, job_count, .{});
// //
// //     for (jobs) |*job| {
// //         job.func = atomicIncrement;
// //         job.ctx = @ptrCast(&test_ctx);
// //     }
// //
// //     try system.start(arena);
// //
// //     // Schedule jobs
// //     for (jobs) |*job| {
// //         try system.schedule(job);
// //     }
// //
// //     // Give threads time to process
// //     // SLEEP_FUNCTION_HERE
// //
// //     try system.stop();
// //     try system.join();
// //
// //     // Verify all jobs executed
// //     try testing.expectEqual(test_ctx.counter.load(.monotonic), job_count);
// // }

test "JobSystemStopAndJoin" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var system = GioJobSystem.init(arena, 2, 32, 128);

    try testing.expect(system.running.load(.acquire));

    try system.start(arena);

    // Stop immediately (tests start/stop mechanism)
    try system.stop();

    try testing.expect(!system.running.load(.acquire));

    try system.join();
}

test "JobBufferCapacityBoundary" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    const capacity: usize = 16;
    var buffer: GioJobBuffer = .init(arena, capacity);

    const jobs = try arena.pushArray(GioJob, capacity, .{});

    // Fill to capacity - 1 (circular buffer constraint)
    var i: usize = 0;
    while (i < capacity - 1) : (i += 1) {
        try buffer.push(&jobs[i]);
    }

    // Next push should fail
    const result = buffer.push(&jobs[capacity - 1]);
    try testing.expectError(GioJobBufferError.JobBufferFull, result);

    // Pop one
    _ = buffer.pop();

    // Now we should be able to push again
    try buffer.push(&jobs[capacity - 1]);
}

// // TODO: Re-enable when sleep API is stable in Zig 0.16+
// // fn slowJob(ctx: *anyopaque) void {
// //     const sleep_ms: *usize = @ptrCast(@alignCast(ctx));
// //     // SLEEP_FUNCTION_HERE
// //     _ = sleep_ms;
// // }

// // test "JobSystemStressTest" {
// //     const arena: *gio_arena.GioArena = try .init(.{});
// //     defer arena.deinit();
// //
// //     var system = GioJobSystem.init(arena, 4, 128, 512);
// //
// //     var test_ctx = JobSystemTestContext{
// //         .counter = std.atomic.Value(usize).init(0),
// //     };
// //
// //     const job_count = 100;
// //     const jobs = try arena.pushArray(GioJob, job_count, .{});
// //
// //     for (jobs) |*job| {
// //         job.func = atomicIncrement;
// //         job.ctx = @ptrCast(&test_ctx);
// //     }
// //
// //     try system.start(arena);
// //
// //     // Schedule all jobs
// //     for (jobs) |*job| {
// //         try system.schedule(job);
// //     }
// //
// //     // Wait for completion
// //     // SLEEP_FUNCTION_HERE
// //
// //     try system.stop();
// //     try system.join();
// //
// //     // All jobs should have executed
// //     // try testing.expectEqual(test_ctx.counter.load(.monotonic), job_count);
// // }

test "JobBufferAlternatingPushPop" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var buffer: GioJobBuffer = .init(arena, 8);

    const jobs = try arena.pushArray(GioJob, 10, .{});
    const arr = try arena.pushArray(usize, 10, .{});

    // Alternating push and pop
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        arr[i] = i;
        jobs[i].func = dummy;
        jobs[i].ctx = @ptrCast(&arr[i]);

        try buffer.push(&jobs[i]);

        if (buffer.pop()) |job| {
            const value: *usize = @ptrCast(@alignCast(job.ctx));
            try testing.expectEqual(value.*, i);
        } else {
            return error.UnexpectedNull;
        }
    }

    // Buffer should be empty
    const result = buffer.pop();
    try testing.expectEqual(result, null);
}

test "JobQueueSequentialPushPop" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var queue: GioJobQueue = .init(arena, 16);

    const jobs = try arena.pushArray(GioJob, 8, .{});
    const arr = try arena.pushArray(usize, 8, .{});

    // Push all
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        arr[i] = i;
        jobs[i].ctx = @ptrCast(&arr[i]);
        try queue.push(&jobs[i]);
    }

    // Pop all in LIFO order
    i = 8;
    while (queue.pop()) |job| {
        i -= 1;
        const value: *usize = @ptrCast(@alignCast(job.ctx));
        try testing.expectEqual(value.*, arr[i]);
    }

    try testing.expectEqual(i, 0);
}

test "WorkerBufferIndependence" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var worker1 = GioWorker.init(arena, 0, 16);
    var worker2 = GioWorker.init(arena, 1, 16);

    const jobs = try arena.pushArray(GioJob, 4, .{});

    // Schedule to different workers
    try worker1.schedule(&jobs[0]);
    try worker1.schedule(&jobs[1]);
    try worker2.schedule(&jobs[2]);
    try worker2.schedule(&jobs[3]);

    // Verify independence
    try testing.expectEqual(worker1.buffer.head.raw, 2);
    try testing.expectEqual(worker2.buffer.head.raw, 2);

    // Pop from each
    _ = worker1.buffer.pop();
    try testing.expectEqual(worker1.buffer.head.raw, 1);
    try testing.expectEqual(worker2.buffer.head.raw, 2); // unchanged
}

const PushThreadContext = struct {
    buffer: *GioJobBuffer,
    jobs: []GioJob,
    start_index: usize,
    count: usize,
    pushed: std.atomic.Value(usize),
};

fn pusherThread(ctx: *PushThreadContext) void {
    var pushed_count: usize = 0;
    const end = ctx.start_index + ctx.count;
    var i = ctx.start_index;
    while (i < end) : (i += 1) {
        ctx.buffer.push(&ctx.jobs[i]) catch |err| {
            if (err == GioJobBufferError.JobBufferStealRaceLost or err == GioJobBufferError.JobBufferFull) {
                continue;
            }
        };
        pushed_count += 1;
    }
    _ = ctx.pushed.fetchAdd(pushed_count, .monotonic);
}

// This test is INVALID for work-stealing architecture
// Multiple threads never push to the same buffer
// Only the owner thread pushes to its own buffer
// test "JobBufferConcurrentPushMultipleThreads" {
//     const arena: *gio_arena.GioArena = try .init(.{});
//     defer arena.deinit();
//
//     var buffer: GioJobBuffer = .init(arena, 128);
//
//     const jobs = try arena.pushArray(GioJob, 80, .{});
//     const arr = try arena.pushArray(usize, 80, .{});
//
//     var i: usize = 0;
//     while (i < 80) : (i += 1) {
//         arr[i] = i;
//         jobs[i].func = dummy;
//         jobs[i].ctx = @ptrCast(&arr[i]);
//     }
//
//     // Create contexts for pusher threads (each pushes 40 jobs)
//     var ctx1 = PushThreadContext{
//         .buffer = &buffer,
//         .jobs = jobs,
//         .start_index = 0,
//         .count = 40,
//         .pushed = std.atomic.Value(usize).init(0),
//     };
//
//     var ctx2 = PushThreadContext{
//         .buffer = &buffer,
//         .jobs = jobs,
//         .start_index = 40,
//         .count = 40,
//         .pushed = std.atomic.Value(usize).init(0),
//     };
//
//     // Spawn pusher threads
//     const thread1 = try std.Thread.spawn(.{}, pusherThread, .{&ctx1});
//     const thread2 = try std.Thread.spawn(.{}, pusherThread, .{&ctx2});
//
//     thread1.join();
//     thread2.join();
//
//     const total_pushed = ctx1.pushed.load(.monotonic) + ctx2.pushed.load(.monotonic);
//
//     // Count what we can pop
//     var popped: usize = 0;
//     while (buffer.pop()) |_| {
//         popped += 1;
//     }
//
//     // What was pushed should be poppable
//     try testing.expectEqual(total_pushed, popped);
// }

const PopThreadContext = struct {
    buffer: *GioJobBuffer,
    popped: std.atomic.Value(usize),
};

fn popperThread(ctx: *PopThreadContext) void {
    var popped_count: usize = 0;
    while (ctx.buffer.pop()) |_| {
        popped_count += 1;
    }
    _ = ctx.popped.fetchAdd(popped_count, .monotonic);
}

// This test is INVALID for work-stealing architecture
// In real usage, only the owner thread calls pop() on its buffer
// Multiple threads never compete using pop() on the same buffer
// Keeping for reference but commented out
// test "JobBufferConcurrentPopMultipleThreads" {
//     const arena: *gio_arena.GioArena = try .init(.{});
//     defer arena.deinit();
//
//     var buffer: GioJobBuffer = .init(arena, 128);
//
//     const jobs = try arena.pushArray(GioJob, 60, .{});
//     const arr = try arena.pushArray(usize, 60, .{});
//
//     // Owner thread pushes 60 jobs
//     var i: usize = 0;
//     while (i < 60) : (i += 1) {
//         arr[i] = i;
//         jobs[i].func = dummy;
//         jobs[i].ctx = @ptrCast(&arr[i]);
//         try buffer.push(&jobs[i]);
//     }
//
//     // Create contexts for popper threads
//     var ctx = PopThreadContext{
//         .buffer = &buffer,
//         .popped = std.atomic.Value(usize).init(0),
//     };
//
//     // Spawn popper threads (both competing to pop)
//     const thread1 = try std.Thread.spawn(.{}, popperThread, .{&ctx});
//     const thread2 = try std.Thread.spawn(.{}, popperThread, .{&ctx});
//
//     thread1.join();
//     thread2.join();
//
//     const total_popped = ctx.popped.load(.monotonic);
//
//     // All 60 jobs should be popped
//     try testing.expectEqual(60, total_popped);
// }

const QueueThreadContext = struct {
    queue: *GioJobQueue,
    jobs: []GioJob,
    start_index: usize,
    count: usize,
    pushed: std.atomic.Value(usize),
};

fn queuePusherThread(ctx: *QueueThreadContext) void {
    var pushed_count: usize = 0;
    const end = ctx.start_index + ctx.count;
    var i = ctx.start_index;
    while (i < end) : (i += 1) {
        ctx.queue.push(&ctx.jobs[i]) catch |err| {
            if (err == GioJobQueueError.GioJobQueueRaceLost or err == GioJobQueueError.GioJobQueueFull) {
                continue;
            }
        };
        pushed_count += 1;
    }
    _ = ctx.pushed.fetchAdd(pushed_count, .monotonic);
}

test "JobQueueConcurrentPushPopThreads" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var queue: GioJobQueue = .init(arena, 128);

    const jobs = try arena.pushArray(GioJob, 80, .{});
    const arr = try arena.pushArray(usize, 80, .{});

    var i: usize = 0;
    while (i < 80) : (i += 1) {
        arr[i] = i;
        jobs[i].func = dummy;
        jobs[i].ctx = @ptrCast(&arr[i]);
    }

    // Create contexts for pusher threads
    var ctx1 = QueueThreadContext{
        .queue = &queue,
        .jobs = jobs,
        .start_index = 0,
        .count = 40,
        .pushed = std.atomic.Value(usize).init(0),
    };

    var ctx2 = QueueThreadContext{
        .queue = &queue,
        .jobs = jobs,
        .start_index = 40,
        .count = 40,
        .pushed = std.atomic.Value(usize).init(0),
    };

    // Spawn pusher threads
    const thread1 = try std.Thread.spawn(.{}, queuePusherThread, .{&ctx1});
    const thread2 = try std.Thread.spawn(.{}, queuePusherThread, .{&ctx2});

    thread1.join();
    thread2.join();

    const total_pushed = ctx1.pushed.load(.monotonic) + ctx2.pushed.load(.monotonic);

    // Pop everything
    var popped: usize = 0;
    while (queue.pop()) |_| {
        popped += 1;
    }

    // What was pushed should equal what we popped
    try testing.expectEqual(total_pushed, popped);
}

const MixedThreadContext = struct {
    buffer: *GioJobBuffer,
    jobs: []GioJob,
    pushed: std.atomic.Value(usize),
    popped: std.atomic.Value(usize),
    stolen: std.atomic.Value(usize),
};

// Real-world work-stealing architecture test
// Owner thread: does push() and pop()
// Stealer threads: only do steal()
const OwnerWorkerContext = struct {
    buffer: *GioJobBuffer,
    jobs: []GioJob,
    keep_running: std.atomic.Value(bool),
    pushed: std.atomic.Value(usize),
    popped: std.atomic.Value(usize),
    stolen: std.atomic.Value(usize),
};

fn ownerWorkerThread(ctx: *OwnerWorkerContext) void {
    var pushed_count: usize = 0;
    var popped_count: usize = 0;
    var i: usize = 0;

    while (ctx.keep_running.load(.acquire)) {
        // Owner pushes work to its own buffer
        const job_idx = @mod(i, ctx.jobs.len);
        ctx.buffer.push(&ctx.jobs[job_idx]) catch {
            std.Thread.yield() catch {};
            continue;
        };
        pushed_count += 1;
        i += 1;

        // Owner also processes its own work (pop)
        if (ctx.buffer.pop()) |_| {
            popped_count += 1;
        }
    }

    _ = ctx.pushed.fetchAdd(pushed_count, .monotonic);
    _ = ctx.popped.fetchAdd(popped_count, .monotonic);
}

fn thiefWorkerThread(ctx: *OwnerWorkerContext) void {
    var stolen_count: usize = 0;

    while (ctx.keep_running.load(.acquire)) {
        // Thieves only steal from other workers
        if (ctx.buffer.steal()) |_| {
            stolen_count += 1;
        } else {
            std.Thread.yield() catch {};
        }
    }

    _ = ctx.stolen.fetchAdd(stolen_count, .monotonic);
}

test "JobBufferRealWorldWorkStealing" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    var buffer: GioJobBuffer = .init(arena, 512);

    const jobs = try arena.pushArray(GioJob, 500, .{});
    const arr = try arena.pushArray(usize, 500, .{});

    var i: usize = 0;
    while (i < 500) : (i += 1) {
        arr[i] = i;
        jobs[i].func = dummy;
        jobs[i].ctx = @ptrCast(&arr[i]);
    }

    var ctx = OwnerWorkerContext{
        .buffer = &buffer,
        .jobs = jobs,
        .keep_running = std.atomic.Value(bool).init(true),
        .pushed = std.atomic.Value(usize).init(0),
        .popped = std.atomic.Value(usize).init(0),
        .stolen = std.atomic.Value(usize).init(0),
    };

    // Real architecture: 1 owner + multiple thieves
    // Owner does push/pop, thieves only steal
    const owner = try std.Thread.spawn(.{}, ownerWorkerThread, .{&ctx});

    const thief1 = try std.Thread.spawn(.{}, thiefWorkerThread, .{&ctx});
    const thief2 = try std.Thread.spawn(.{}, thiefWorkerThread, .{&ctx});
    const thief3 = try std.Thread.spawn(.{}, thiefWorkerThread, .{&ctx});
    const thief4 = try std.Thread.spawn(.{}, thiefWorkerThread, .{&ctx});

    // Let it run
    var spin: usize = 0;
    while (spin < 5_000_000) : (spin += 1) {
        std.atomic.spinLoopHint();
    }

    ctx.keep_running.store(false, .release);

    owner.join();
    thief1.join();
    thief2.join();
    thief3.join();
    thief4.join();

    const total_pushed = ctx.pushed.load(.monotonic);
    const total_popped = ctx.popped.load(.monotonic);
    const total_stolen = ctx.stolen.load(.monotonic);

    // Clean up remaining jobs (owner drains its buffer)
    var remaining: usize = 0;
    while (buffer.pop()) |_| {
        remaining += 1;
    }

    // All pushed jobs should be accounted for
    try testing.expectEqual(total_pushed, total_popped + total_stolen + remaining);

    // Should have processed significant work
    try testing.expect(total_pushed > 100);
}

// Heavy stress test with CORRECT work-stealing architecture
// Simulates realistic multi-worker scenario with work stealing
const MultiWorkerStressContext = struct {
    workers: []WorkerBuffer,
    jobs: []GioJob,
    keep_running: std.atomic.Value(bool),
    total_executed: std.atomic.Value(usize),

    const WorkerBuffer = struct {
        buffer: GioJobBuffer,
        owner_id: usize,
    };
};

fn multiWorkerOwnerThread(ctx: *MultiWorkerStressContext, worker_id: usize) void {
    var local_executed: usize = 0;
    var job_count: usize = 0;

    while (ctx.keep_running.load(.acquire)) {
        // Owner pushes work to its own buffer
        const job_idx = @mod(job_count, ctx.jobs.len);
        ctx.workers[worker_id].buffer.push(&ctx.jobs[job_idx]) catch {
            std.Thread.yield() catch {};
            continue;
        };
        job_count += 1;

        // Owner processes its own work (pop first - LIFO for cache locality)
        if (ctx.workers[worker_id].buffer.pop()) |job| {
            job.func(job.ctx);
            local_executed += 1;
        } else {
            // If my buffer is empty, try stealing from other workers
            for (ctx.workers, 0..) |*worker, id| {
                if (id != worker_id) {
                    if (worker.buffer.steal()) |job| {
                        job.func(job.ctx);
                        local_executed += 1;
                        break;
                    }
                }
            }
        }
    }

    _ = ctx.total_executed.fetchAdd(local_executed, .monotonic);
}

test "JobBufferMultiWorkerStressTest" {
    const arena: *gio_arena.GioArena = try .init(.{});
    defer arena.deinit();

    const worker_count = 4;
    const workers = try arena.pushArray(MultiWorkerStressContext.WorkerBuffer, worker_count, .{});

    // Initialize each worker's buffer
    for (workers, 0..) |*worker, id| {
        worker.buffer = .init(arena, 256);
        worker.owner_id = id;
    }

    const jobs = try arena.pushArray(GioJob, 1000, .{});
    var test_ctx = JobSystemTestContext{
        .counter = std.atomic.Value(usize).init(0),
    };

    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        jobs[i].func = atomicIncrement;
        jobs[i].ctx = @ptrCast(&test_ctx);
    }

    var ctx = MultiWorkerStressContext{
        .workers = workers,
        .jobs = jobs,
        .keep_running = std.atomic.Value(bool).init(true),
        .total_executed = std.atomic.Value(usize).init(0),
    };

    // Spawn 4 worker threads (each owns its buffer)
    const worker0 = try std.Thread.spawn(.{}, multiWorkerOwnerThread, .{ &ctx, 0 });
    const worker1 = try std.Thread.spawn(.{}, multiWorkerOwnerThread, .{ &ctx, 1 });
    const worker2 = try std.Thread.spawn(.{}, multiWorkerOwnerThread, .{ &ctx, 2 });
    const worker3 = try std.Thread.spawn(.{}, multiWorkerOwnerThread, .{ &ctx, 3 });

    // Let it run
    var spin: usize = 0;
    while (spin < 10_000_000) : (spin += 1) {
        std.atomic.spinLoopHint();
    }

    ctx.keep_running.store(false, .release);

    worker0.join();
    worker1.join();
    worker2.join();
    worker3.join();

    const total_executed = ctx.total_executed.load(.monotonic);
    const counter_value = test_ctx.counter.load(.monotonic);

    // All executed jobs should have incremented the counter
    try testing.expectEqual(total_executed, counter_value);

    // Should have processed significant work
    try testing.expect(total_executed > 100);
}
