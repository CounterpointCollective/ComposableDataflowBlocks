using System.Collections.Concurrent;
using System.Threading.Tasks;
using CounterpointCollective.Threading;

namespace UnitTests.Threading
{
    public class NamedLockTests
    {
        public NamedLockHandler NamedLockHandler { get; } = new();

        [Fact]
        public async Task TestMultipleLocksConsecutively()
        {
            var l1 = await NamedLockHandler.LockAsync("test");
            Assert.True(NamedLockHandler.IsLocked("test"));
            l1.Dispose();
            Assert.False(NamedLockHandler.IsLocked("test"));

            var l2 = await NamedLockHandler.LockAsync("test");
            Assert.True(NamedLockHandler.IsLocked("test"));
            l2.Dispose();
            Assert.False(NamedLockHandler.IsLocked("test"));
        }

        [Fact]
        public async Task TestCancelFlow()
        {
            var l1 = await NamedLockHandler.LockAsync("test");

            var cts2 = new CancellationTokenSource();
            var taskL2 = NamedLockHandler.LockAsync("test", cancellationToken: cts2.Token);
            var taskL3 = NamedLockHandler.LockAsync("test");
            await Task.Delay(100);
            cts2.Cancel();
            var wasCanceled = false;
            try
            {
                await taskL2;
            }
            catch (TaskCanceledException)
            {
                wasCanceled = true;
            }
            Assert.True(wasCanceled);

            Assert.True(NamedLockHandler.IsLocked("test"));
            l1.Dispose();
            var l3 = await taskL3;
            Assert.True(NamedLockHandler.IsLocked("test"));
            l3.Dispose();
            Assert.False(NamedLockHandler.IsLocked("test"));
        }

        [Fact]
        public async Task TestAlreadyLocked()
        {
            var l1 = await NamedLockHandler.LockAsync("test");
            var taskL2 = NamedLockHandler.LockAsync("test");
            await Task.Delay(100);
            Assert.False(taskL2.IsCompleted);
            l1.Dispose();
            var l2 = await taskL2;
            l2.Dispose();
        }

        [Fact]
        public async Task TestAcquiringAlreadyCanceled()
        {
            var cts1 = new CancellationTokenSource();

            var tL1 = NamedLockHandler.LockAsync("test", cancellationToken: CancellationToken.None);
            var tL2 = NamedLockHandler.LockAsync("test", cancellationToken: cts1.Token);
            var tL3 = NamedLockHandler.LockAsync("test", cancellationToken: cts1.Token);
            (await tL1).Dispose();
            cts1.Cancel();
            (await tL2).Dispose();

            var cancelationExceptionThrown = false;
            try
            {
                await tL3;
            }
            catch (OperationCanceledException)
            {
                cancelationExceptionThrown = true;
            }

            Assert.True(cancelationExceptionThrown);
            Assert.True(tL3.IsCompleted);

            using var l4 = await NamedLockHandler.LockAsync("test");
        }

        [Fact]
        public async Task StressTest()
        {
            async IAsyncEnumerable<Task<NamedLock>> Acquire(int count)
            {
                for (var i = 0; i < count; i++)
                {
                    yield return NamedLockHandler.LockAsync("test", cancellationToken: CancellationToken.None);
                    await Task.Yield(); //to increase the chance of multiple LockAsyncs trying to acquire in parallel.
                }
            }

            //We launch many locking tasks in parallel.
            var parCount = 10;
            var secCount = 10;
            using var s = new SemaphoreSlim(0);
            List<Task<NamedLock>> tasks = [];
            for (var i = 0; i < parCount; i++)
            {
                _ = Task.Run(async () => {
                    var l = await Acquire(secCount).ToListAsync();
                    ;
                    lock(tasks)
                    {
                        tasks.AddRange(l);
                    }
                    s.Release();
                });
            }
            for (var i = 0; i < parCount; i++)
            {
                await s.WaitAsync();
            }
            Assert.Equal(tasks.Count, NamedLockHandler.DescribeLocks()[0].Descriptions.Length);

            var c = 0;
            //Hook up continuations on all locking tasks.
            for (var i = 0; i < tasks.Count; i++)
            {
                var t = tasks[i];
                tasks[i] = Task.Run(async () =>
                {
                    using var l = await t;
                    await Task.Delay(1);
                    Interlocked.Increment(ref c);
                    return l;
                });
            }

            await Task.WhenAll(tasks);
            Assert.Equal(tasks.Count, c);
        }

        [Fact]
        public async Task SynchronousAndAsynchronousLockTest()
        {
            //Test synchronous flow for open lock.
            var t = NamedLockHandler.LockAsync("test");
            Assert.True(t.IsCompletedSuccessfully);
            Assert.True(NamedLockHandler.IsLocked("test"));

            //Because the lock is now taken, we expect the asynchronous flow, let's see.
            var t2 = NamedLockHandler.LockAsync("test");
            Assert.False(t2.IsCompletedSuccessfully);

            //Disposing the lock from t should cause t2 to eventually be able to take it.
            (await t).Dispose();
            Assert.True(NamedLockHandler.IsLocked("test")); //because t2 is still waiting in line.
            (await t2).Dispose();
            Assert.False(NamedLockHandler.IsLocked("test"));

            //Because the lock is now free again, we expect the synchronous flow, let's see.
            t = NamedLockHandler.LockAsync("test");
            Assert.True(t.IsCompletedSuccessfully);
            (await t).Dispose();
        }
    }
}
