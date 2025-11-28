using CounterpointCollective.DataFlow;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow
{
    public class Examples
    {

        [Fact]
        public async Task Example1BoundingYourOwnComposedBlock()
        {
            //Example of using a BoundedPropagatorBlock to add bounded capacity and item counting
            //First we create a DataflowBlock ourselves, composed of multiple subblocks.
            var b = new BufferBlock<int>(new() { BoundedCapacity = DataflowBlockOptions.Unbounded });
            var t = new TransformBlock<int, int>(async i =>
            {
                await Task.Yield(); //simulate some work.
                return i + 1;
            }, new() { BoundedCapacity = DataflowBlockOptions.Unbounded });
            b.LinkTo(t, new DataflowLinkOptions() { PropagateCompletion = true });
            var ourOwnDataflowBlock = DataflowBlock.Encapsulate(b, t);


            //Now we want to
            // - put a boundedCapacity on our new block and
            // - be able to count how many items are contained with it, at any given time
            //This is what we will use a BoundedPropagatorBlock for.

            var testSubject = new BoundedPropagatorBlock<int, int>(ourOwnDataflowBlock, boundedCapacity: 2000);

            //Thus we enabled a bounded capacity of 2000 messages, and real-time counting on our own custom DataflowBlock!

            Assert.Equal(0, testSubject.Count);

            //we should be able to push synchronously up to the bounded capacity.
            for (var i = 0; i < 2000; i++)
            {
                Assert.True(testSubject.Post(i));
                Assert.Equal(i + 1, testSubject.Count); //count is administered properly
            }
        }

        [Fact]
        public async Task Example2DiYResizableBufferBlockTest()
        {
            //Example showing that you can dynamically resize the bounded capacity of any block by wrapping it into a BoundedPropagatorBlock
            var bufferBlock = new BufferBlock<int>(new() { BoundedCapacity = DataflowBlockOptions.Unbounded });
            var dynamicBufferBlock = new BoundedPropagatorBlock<int, int>(bufferBlock);

            //We did not specify a bounded capacity, so it defaults to DataflowBlockOptions.Unbounded

            Assert.True(dynamicBufferBlock.Post(1));

            //But we can dynamically set the bounded capacity at any point.
            dynamicBufferBlock.BoundedCapacity = 2;
            Assert.True(dynamicBufferBlock.Post(2));
            Assert.False(dynamicBufferBlock.Post(3));

            dynamicBufferBlock.BoundedCapacity = 3;
            Assert.True(dynamicBufferBlock.Post(3));
        }

        [Fact]
        public async Task AutoScalingBlockTest()
        {
            // Example: Using AutoScaling on a ResizableBatchTransformBlock
            // For demonstration: assume our workload performs best when batch size is ~100 items.
            async Task<IEnumerable<int>> ProcessBatch(int[] batch)
            {
                var distanceFromOptimal = Math.Abs(batch.Length - 100);
                await Task.Delay(distanceFromOptimal * batch.Length / 10); // Simulate slower processing when batch isn't optimal
                return batch;
            }

            // Create a ResizableBatchTransformBlock using our ProcessBatch function.
            var testSubject = new ResizableBatchTransformBlock<int, int>(
                ProcessBatch,
                initialBatchSize: 1,
                new ExecutionDataflowBlockOptions { BoundedCapacity = 10000 }
            );

            // Batch size can be manually adjusted:
            testSubject.BatchSize = 5;

            // Or automatically optimized using AutoScaling:
            testSubject.EnableAutoScaling(
                new DefaultBatchSizeStrategy(minBatchSize: 1, maxBatchSize: 200, initialBatchSize: 5)
            );

            // Send some work:
            for (var i = 0; i < 10000; i++)
            {
                await testSubject.SendAsync(i);
            }
            testSubject.Complete();

            // Process outputs while AutoScaling gradually converges toward the optimal batch size (~100).
            for (var i = 0; i < 10000; i++)
            {
                var result = await testSubject.ReceiveAsync();
            }

            Assert.True(testSubject.BatchSize > 50 && testSubject.BatchSize < 150, $"Final batch size {testSubject.BatchSize} should be near optimal of 100.");
        }
    }
}
