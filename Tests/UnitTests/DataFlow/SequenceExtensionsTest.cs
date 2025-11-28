using CounterpointCollective.DataFlow;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow
{
    public class SequenceExtensionsTest
    {
        [Fact]
        public void Test()
        {
            var first = Task.FromResult<IPropagatorBlock<int, int>>(new TransformBlock<int, int>(i => i + 1));
            var second = Task.FromResult<IPropagatorBlock<int, int>>(new TransformBlock<int, int>(i => i + 1));
            var res = first.AndThenAsync(second);
        }

        [Fact]
        public async Task TestPropagatesErrorsDuringConstruction()
        {
            var t1 = Task.Run(async () =>
            {
                await Task.Delay(10);
                throw new InvalidOperationException("Test exception");
#pragma warning disable CS0162 // Unreachable code detected
                return default(IPropagatorBlock<int, int>)!;
#pragma warning restore CS0162 // Unreachable code detected
            });

            var b2 = new TransformBlock<int, int>(i => i + 1);

            var res = t1.AndThenAsync(b2);
            await Assert.ThrowsAsync<InvalidOperationException>(() => b2.Completion);
        }
    }
}
