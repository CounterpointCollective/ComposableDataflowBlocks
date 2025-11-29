using CounterpointCollective.DataFlow;
using System.Threading.Tasks.Dataflow;
using static CounterpointCollective.DataFlow.ParallelDataflowBlockExtensions;

namespace UnitTests.DataFlow
{
    public class ParallelDataflowBlockExtensionsTests
    {
        [Fact]
        public async Task ParTest()
        {
            var r = new Random();

            async Task<int> F(int i)
            {
                var w = r.Next(100);
                await Task.Delay(w);
                return i;
            }

            var blocks = new TransformBlock<int, int>[10];
            for (var i = 0; i < blocks.Length; i++)
            {
                blocks[i] = new TransformBlock<int, int>(async j => await F(j));
            }

            var p = blocks.Par();

            var items = new List<int> { 1, 17 };

            _ = Task.Run(async () =>
            {
                foreach (var i in items)
                {
                    await p.SendAsync(i);
                }
                p.Complete();
            });

            var res = await p.AsAsyncEnumerable().ToListAsync();
            Assert.Equal(items, res);
        }

        [Fact]
        public async Task SupportsCancellation()
        {
            var cts = new CancellationTokenSource();
            var t1 = new TransformBlock<int, int>(e => e);
            var t2 = new TransformBlock<int, int>(e => e);
            var testSubject = new[] { t1, t2 }.Par(new() { CancellationToken = cts.Token });
            await testSubject.TestCancellationAsync(cts);
        }

        [Fact]
        public async Task Par2Test()
        {
            var t1 = new TransformBlock<int, int>(e => e + 1);
            var t2 = new TransformBlock<int, int>(e => e + 2);
            var testSubject = t1.Par(t2, (i1, i2) => i1 + i2, new());
            var items = Enumerable.Range(0, 10).ToList();
            var source = items.AsEnumerable().AsSourceBlock();
            source.LinkTo(testSubject, new() { PropagateCompletion = true });
            var ret = await testSubject.AsAsyncEnumerable().ToListAsync();
            var expected = items.Select(i => (2 * i) + 3).ToList();
            Assert.Equal(expected, ret);
        }

        [Fact]
        public async Task ParAsyncTest()
        {
            var worker0 = new TransformBlock<int, int>(e => e + 1);
            var worker1 = new TransformBlock<int, int>(e => e + 1);

            var tcsContinueTask0 = new TaskCompletionSource();

            var task0 = Task.Run(async () =>
            {
                await tcsContinueTask0.Task;
                return (IPropagatorBlock<int, int>)worker0;
            });
            var task1 = Task.Run(() => worker1);

            var b =
                ParallelDataflowBlockExtensions.CreateAsyncParBuilder<int>()
                .AddBlockFactory(_ => task0)
                .AddBlockFactory(_ => task1)
                .Build();

            await b.SendAsync(101);

            //worker1 should already be working while worker0 is still being set up
            await TestExtensions.Eventually(() =>
            {
                Assert.False(task0.IsCompleted);
                Assert.Equal(1, worker1.OutputCount);
            });

            tcsContinueTask0.SetResult();

            await TestExtensions.Eventually(() =>
            {
                Assert.Equal(0, worker0.OutputCount);
                Assert.Equal(0, worker1.OutputCount);
                Assert.Equal(1, b.OutputCount);
            });

            b.Complete();
            await b.ReceiveAsync();
            await b.Completion;
        }

        [Fact]
        public async Task ParAsyncWorkerBuildingPropagatesExceptionsTest()
        {
            var worker1 = new TransformBlock<int, int>(e => e + 1);

            var tcsContinueTask0 = new TaskCompletionSource();

            var task0 = Task.Run(async () =>
            {
                await tcsContinueTask0.Task;
                throw new Exception("Simulate worker0 setup failure");
#pragma warning disable CS0162 // Unreachable code detected
                return default(TransformBlock<int, int>)!;
#pragma warning restore CS0162 // Unreachable code detected

            });

            var task1 = Task.Run(async () =>
            {
                await Task.Delay(500); //Wait to simulate it costs time to set up this block
                return worker1;
            });


            var b =
                ParallelDataflowBlockExtensions.CreateAsyncParBuilder<int>()
                .AddBlockFactory(_ => task0)
                .AddBlockFactory(_ =>task1)
                .Build();

            await b.SendAsync(101);

            //worker1 should already be working while worker0 is still being set up
            await TestExtensions.Eventually(() =>
            {
                Assert.False(task0.IsCompleted);
                Assert.Equal(1, worker1.OutputCount);
            });

            tcsContinueTask0.SetResult();

            await Task.WhenAny(b.Completion);
            Assert.True(b.Completion.IsFaulted);
            Assert.True(worker1.Completion.IsFaulted);
        }

        [Fact]
        public async Task ParCancellingWhileBuildingUp()
        {

            async Task<IPropagatorBlock<int, int>> Fac1 (CancellationToken cancellationToken)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
                cancellationToken.ThrowIfCancellationRequested();
                return default(TransformBlock<int, int>)!; //unreachable code, but needed for the type
            }

            var worker1 = new TransformBlock<int, int>(i => i + 1);
            Task<TransformBlock<int, int>> Fac2(CancellationToken cancellationToken)
                => Task.FromResult(worker1);

            using var cts = new CancellationTokenSource();

            var b =
                ParallelDataflowBlockExtensions.CreateAsyncParBuilder<int>()
                .AddBlockFactory(Fac1)
                .AddBlockFactory(Fac2)
                .Build(cts.Token);

            await b.SendAsync(101);
            await TestExtensions.Eventually(() => Assert.Equal(1, worker1.OutputCount));

            cts.Cancel();
            await Task.WhenAny(b.Completion);
            Assert.True(b.Completion.IsFaulted);
            Assert.True(worker1.Completion.IsFaulted);
        }
    }
}
