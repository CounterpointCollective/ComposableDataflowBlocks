using CounterpointCollective.DataFlow;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow
{
    public class GuaranteedBroadcastBlockTests
    {
        [Fact]
        public async Task WillAcceptMessagesWhileALinkedTargetDoes()
        {
            var o1 = new BufferBlock<int>(
                new() { BoundedCapacity = DataflowBlockOptions.Unbounded }
            );
            var o2 = new BufferBlock<int>(new() { BoundedCapacity = 1 });

            var testSubject = new GuaranteedBroadcastBlock<int>(2, new());
            testSubject[0].LinkTo(o1);
            testSubject[1].LinkTo(o2);
            for (var i = 0; i < 100; i++)
            {
                await testSubject.SendAsync(1);
            }
        }

        [Fact]
        public async Task ProperlyCompletes()
        {
            var o1 = new BufferBlock<int>(
                new() { BoundedCapacity = DataflowBlockOptions.Unbounded }
            );
            var o2 = new BufferBlock<int>(
                new() { BoundedCapacity = DataflowBlockOptions.Unbounded }
            );

            var testSubject = new GuaranteedBroadcastBlock<int>(2, new());
            testSubject[0].LinkTo(o1);
            testSubject[1].LinkTo(o2);

            for (var i = 0; i < 100; i++)
            {
                await testSubject.SendAsync(i);
            }
            testSubject.Complete();
            await testSubject.Completion;
        }

        [Fact]
        public async Task WillPauseProcessingAfterAllLinkedTargetsDo()
        {
            var o1 = new BufferBlock<int>(new() { BoundedCapacity = 1 });
            var o2 = new BufferBlock<int>(new() { BoundedCapacity = 2 });

            var testSubject = new GuaranteedBroadcastBlock<int>(
                2,
                new() { BoundedCapacity = 1 }
            );

            testSubject[0].LinkTo(o1);
            testSubject[1].LinkTo(o2);

            for (var i = 0; i < 3; i++)
            {
                await testSubject.SendAsync(1);
            }

            await Task.Delay(100); // give it some time to swallow all messages.
            Assert.False(testSubject.Post(1));

            o2.Receive();

            await testSubject.SendAsync(1);
        }

        [Fact]
        public async Task QueueStartsCountingWhenFastestNoLongerCanConsume()
        {
            var o1 = new BufferBlock<int>(new() { BoundedCapacity = 1 });
            var o2 = new BufferBlock<int>(new() { BoundedCapacity = 2 });

            var testSubject = new GuaranteedBroadcastBlock<int>(
                2,
                new() { BoundedCapacity = 2 }
            );

            testSubject[0].LinkTo(o1);
            testSubject[1].LinkTo(o2);

            for (var i = 0; i < 4; i++)
            {
                await testSubject.SendAsync(1);
            }
            Assert.Equal(2, testSubject.Count);
            Assert.False(testSubject.Post(1));


            //read from slowest target
            o1.Receive();
            await TestToolExtensions.Eventually(() => Assert.Equal(1, o1.Count));
            Assert.Equal(2, testSubject.Count);

            Assert.False(testSubject.Post(1));

            //read from fastest target
            o2.Receive();
            await TestToolExtensions.Eventually(() => Assert.Equal(2, o2.Count));
            Assert.Equal(1, testSubject.Count);


            Assert.True(testSubject.Post(1));
        }

        [Fact]
        public async Task SupportsCancellation()
        {
            var cts = new CancellationTokenSource();
            var testSubject = new GuaranteedBroadcastBlock<int>(
                1,
                new() { CancellationToken = cts.Token }
            );
            testSubject[0].LinkTo(new BufferBlock<int>());
            await testSubject.TestCancellationAsync(cts);
        }

        [Fact]
        public async Task TestWithBoundedLargestQueue()
        {
            var testSubject = new GuaranteedBroadcastBlock<int>(
                2,
                new() {
                    BoundedCapacity = 2,
                    BoundedCapacityMode = GuaranteedBroadcastBlockBoundedCapacityMode.LargestQueue
                }
            );
            var t0 = new BufferBlock<int>(new() { BoundedCapacity = 2 });
            testSubject[0].LinkTo(t0);
            await testSubject.SendAsync(101);
            await testSubject.SendAsync(102);

            await TestToolExtensions.Eventually(() => Assert.Equal(2, t0.Count));
            Assert.Equal(2, testSubject.Count);

            Assert.False(testSubject.Post(103)); //because max queue is full.
            Assert.Equal(101, await testSubject[1].ReceiveAsync());

            Assert.Equal(1, testSubject.Count);

            await testSubject.SendAsync(103);
            Assert.Equal(2, testSubject.Count);
        }

        [Fact]
        public async Task CheckCounting()
        {
            var testSubject = new GuaranteedBroadcastBlock<int>(
                2,
                new()
                {
                    BoundedCapacityMode = GuaranteedBroadcastBlockBoundedCapacityMode.SmallestQueue
                }
            );
            Assert.Equal(0, testSubject.Count);  //[0,0]
            testSubject.PostAsserted(1);         //[1,1]
            Assert.Equal(1, testSubject.Count);
            await testSubject[0].ReceiveAsync(); //[0,1]
            Assert.Equal(0, testSubject.Count);
            testSubject.PostAsserted(1);         //[1,2]
            Assert.Equal(1, testSubject.Count);
            await testSubject[1].ReceiveAsync(); //[1,1]
            Assert.Equal(1, testSubject.Count);
            await testSubject[0].ReceiveAsync(); //[0,1]
            Assert.Equal(0, testSubject.Count);

        }
    }
}
