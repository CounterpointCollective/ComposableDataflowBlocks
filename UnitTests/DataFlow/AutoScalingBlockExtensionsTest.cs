using System.Threading.Tasks.Dataflow;
using CounterpointCollective.DataFlow;

namespace UnitTests.DataFlow
{
    public class AutoScalingBlockExtensionsTest
    {
        [Fact]
        public async Task CheckWillMax()
        {

            var constantDelayBlock = new ResizableBatchTransformBlock<int,int>(
                async b =>
                {
                    await Task.Delay(10);
                    return b;
                },
                1,
                new() { MaxDegreeOfParallelism = 4}
            );


            var exponentialSlowdownBlock = new ResizableBatchTransformBlock<int, int>(
                async b =>
                {
                    await Task.Delay(10 * (int)Math.Pow(b.Length, 2));
                    return b;
                },
                1,
                new() { MaxDegreeOfParallelism = 4 }
            );


            constantDelayBlock.EnableAutoScaling(
                new DefaultBatchSizeStrategy(1, 10, 1)
            );
            exponentialSlowdownBlock.EnableAutoScaling(
                new DefaultBatchSizeStrategy(1, 10, 1)
            );


            constantDelayBlock.LinkTo(DataflowBlock.NullTarget<int>(), new());
            exponentialSlowdownBlock.LinkTo(DataflowBlock.NullTarget<int>(), new());

            var s1 = Enumerable.Range(1, 1000).AsSourceBlock();
            var s2 = Enumerable.Range(1, 1000).AsSourceBlock();
            s1.LinkTo(constantDelayBlock, new DataflowLinkOptions { PropagateCompletion = true });
            s2.LinkTo(exponentialSlowdownBlock, new DataflowLinkOptions { PropagateCompletion = true });

            await constantDelayBlock.Completion;
            await exponentialSlowdownBlock.Completion;

            Assert.True(constantDelayBlock.BatchSize > exponentialSlowdownBlock.BatchSize);
            Assert.True(constantDelayBlock.BatchSize >= 7);
            Assert.True(exponentialSlowdownBlock.BatchSize <= 3);
        }
    }
}
