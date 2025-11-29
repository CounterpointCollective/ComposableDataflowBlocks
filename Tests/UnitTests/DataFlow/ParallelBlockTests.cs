using CounterpointCollective.DataFlow;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow
{
    public class ParallelBlockTests
    {
        [Fact]
        public async Task TestMinCountMode()
        {
            var testSubject = new ParallelBlock<int, int, int> (2, e => e.Max(), new()
            {
                BoundedCapacityMode = GuaranteedBroadcastBlockBoundedCapacityMode.SmallestQueue,
                BoundedCapacity = 2
            });

            var b = new BufferBlock<int>();
            b.LinkTo(testSubject, new() { PropagateCompletion = true });

            b.PostAsserted(101);
            b.PostAsserted(102);
            b.PostAsserted(103);
            await TestExtensions.Eventually(() =>
            {
                Assert.Equal(2, testSubject.InputCount);
                Assert.Equal(2, testSubject.Count);
                Assert.Equal(1, b.Count);
            });

            var worker1 = new TransformBlock<int,int>(e => e + 1);

            testSubject.Hookup(worker1, new ());
            await TestExtensions.Eventually(() => Assert.Equal(0, testSubject.InputCount));

            testSubject.Hookup(new BufferBlock<int>(), new());
            await TestExtensions.Eventually(() => Assert.Equal(3, testSubject.OutputCount));
        }

        [Fact]
        public async Task TestMaxCountMode()
        {
            var testSubject = new ParallelBlock<int, int, int>(2, e => e.Max(), new()
            {
                BoundedCapacityMode = GuaranteedBroadcastBlockBoundedCapacityMode.LargestQueue,
                BoundedCapacity = 2
            });

            var source = new BufferBlock<int>();
            source.LinkTo(testSubject, new() { PropagateCompletion = true });

            Assert.True(source.Post(101));
            Assert.True(source.Post(102));
            Assert.True(source.Post(103));

            await TestExtensions.Eventually(() =>
            {
                Assert.Equal(2, testSubject.InputCount);
                Assert.Equal(1, source.Count);
            });

            var worker1 = new TransformBlock<int, int>(e => e + 1);

            testSubject.Hookup(worker1, new());

            await UnitTests.TestExtensions.Eventually(() => Assert.Equal(2, worker1.OutputCount)); //it should already start delivering messages

            Assert.Equal(2, testSubject.InputCount); //because we count the largest queue
            Assert.Equal(1, source.Count);

            var tcs = new TaskCompletionSource();
            var worker2 = new TransformBlock<int,int>(async i =>
            {
                await tcs.Task;
                return i;
            }, new() { BoundedCapacity = 1 });
            testSubject.Hookup(worker2, new()); //worker2 will absorb the first message from queue[1].
            await TestExtensions.Eventually(() =>
            {
                Assert.Equal(0, source.Count); //since both queues have now delivered items, a second item should fit into the broadcast.
                Assert.Equal(3, worker1.OutputCount);
                Assert.Equal(2, testSubject.InputCount);
                Assert.Equal(0, testSubject.OutputCount);
                Assert.Equal(2, testSubject.Count);
            });

            tcs.SetResult(); //alow worker2 to process messages.
            await TestExtensions.Eventually(() =>
            {
                Assert.Equal(0, testSubject.InputCount);
                Assert.Equal(3, testSubject.OutputCount);
                Assert.Equal(3, testSubject.Count);
            });
        }
    }
}
