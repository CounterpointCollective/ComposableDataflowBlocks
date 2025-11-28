using CounterpointCollective.DataFlow;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow
{
    public class PriorityBufferBlockTest
    {
        [Fact]
        public async Task TestHigherPrioWillOvertake()
        {
            var testSubject = new PriorityBufferBlock<int, int>();
            testSubject.PostAsserted((2, 2));
            await testSubject.AwaitElectionsHeldAsync();
            testSubject.PostAsserted((1, 1));
            await testSubject.AwaitElectionsHeldAsync();
            var msg = await testSubject.ReceiveAsync();
            Assert.Equal((1, 1), msg);
        }

        [Fact]
        public async Task TestCompletion()
        {
            var testSubject = new PriorityBufferBlock<int, int>();
            testSubject.PostAsserted((2, 2));
            await testSubject.AwaitElectionsHeldAsync();
            testSubject.PostAsserted((1, 1));
            testSubject.Complete();
            await testSubject.AwaitElectionsHeldAsync();
            Assert.Equal((1, 1), await testSubject.ReceiveAsync());
            Assert.Equal((2, 2), await testSubject.ReceiveAsync());
            await testSubject.Completion;
        }

        [Fact]
        public async Task TestOnceReservedOrderWontChange()
        {
            var dummyTarget = new BufferBlock<(int, int)>();
            var testSubject = new PriorityBufferBlock<int, int>();
            testSubject.PostAsserted((2, 2));
            await testSubject.AwaitElectionsHeldAsync();
            Assert.True(testSubject.ReserveMessage(new DataflowMessageHeader(1), dummyTarget));
            testSubject.PostAsserted((1, 1));
            var msg = testSubject.ConsumeMessage(
                new DataflowMessageHeader(1),
                dummyTarget,
                out var messageConsumed
            );
            Assert.True(messageConsumed);
            Assert.Equal((2, 2), msg);
        }

        [Fact]
        public async Task TestReleasingReservationReordersMessages()
        {
            var dummyTarget = new BufferBlock<(int, int)>();
            var testSubject = new PriorityBufferBlock<int, int>();
            testSubject.PostAsserted((2, 2));
            await testSubject.AwaitElectionsHeldAsync();
            Assert.True(testSubject.ReserveMessage(new DataflowMessageHeader(1), dummyTarget));
            testSubject.PostAsserted((1, 1));
            await testSubject.AwaitElectionsHeldAsync();
            testSubject.ReleaseReservation(new DataflowMessageHeader(1), dummyTarget);
            await testSubject.AwaitElectionsHeldAsync();
            var msg = await testSubject.ReceiveAsync();
            Assert.Equal((1, 1), msg);
        }

        [Fact]
        public async Task TestReceiveAll()
        {
            var testSubject = new PriorityBufferBlock<int, int>(new(), electedBufferSize : 2);
            testSubject.PostAsserted((4, 4));
            testSubject.PostAsserted((1, 1));
            await testSubject.AwaitElectionsHeldAsync();
            await testSubject.OutputAvailableAsync();
            Assert.True(testSubject.TryReceiveAll(out var items));
            Assert.Equal((1, 1), items![0]);
            Assert.Equal((4, 4), items![1]);

            testSubject.PostAsserted((3, 3));
            testSubject.PostAsserted((2, 2));

            await testSubject.AwaitElectionsHeldAsync();
            await testSubject.OutputAvailableAsync();
            Assert.True(testSubject.TryReceiveAll(out items));
            Assert.Equal((2, 2), items![0]);
            Assert.Equal((3, 3), items[1]);

            Assert.Equal(0, testSubject.Count);
        }

        [Fact]
        public async Task PicksUpPostponedMessages()
        {
            var source = Enumerable.Range(0, 10).AsSourceBlock();
            var testSubject = new PriorityBufferBlock<int, int>(new() { BoundedCapacity = 2 });
            source.LinkToTransformed(testSubject, new() { PropagateCompletion = true }, i => (i, i));
            for(var i = 0; i < 10; i++)
            {
                while(testSubject.Count != Math.Min(10 - i, 2))
                {
                    await Task.Delay(1); //fill up the block.
                }
                Assert.Equal((i,i), await testSubject.ReceiveAsync());
            }
            await testSubject.Completion;
        }

        [Fact]
        public async Task OutOfOrderWithLargerBuffer()
        {
            var mockTarget = new BufferBlock<(int, int)>();
            var testSubject = new PriorityBufferBlock<int, int>(new(), electedBufferSize: 2);
            testSubject.PostAsserted((10, 10));
            await testSubject.OutputAvailableAsync();
            Assert.True(testSubject.ReserveMessage(new DataflowMessageHeader(1), mockTarget));

            testSubject.PostAsserted((2, 2));
            testSubject.PostAsserted((1, 1));
            await testSubject.AwaitElectionsHeldAsync();
            Assert.True(testSubject.ElectionBufferContainsOutOfOrderItem);

            testSubject.ReleaseReservation(new DataflowMessageHeader(1), mockTarget);
            await testSubject.AwaitElectionsHeldAsync();
            Assert.Equal((1, 1), await testSubject.ReceiveAsync());
            Assert.True(testSubject.TryReceive(out var m) && m == (2, 2));
            Assert.Equal((10, 10), await testSubject.ReceiveAsync());
        }
    }
}
