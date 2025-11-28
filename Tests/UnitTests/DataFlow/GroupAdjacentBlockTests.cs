using CounterpointCollective.DataFlow;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow
{
    public class GroupAdjacentBlockTests
    {
        [Fact]
        public async Task TestNormalFlow()
        {
            int[] inputs = [1, 1, 2, 2, 1, 2];
            var input = inputs.AsSourceBlock();

            var t = input.GroupAdjacent(e => e, e => e, new());

            var g = await t.ReceiveAsync();
            Assert.Equal(1, g.Key);
            Assert.Equal([1, 1], g.AsEnumerable());
            g = await t.ReceiveAsync();
            Assert.Equal(2, g.Key);
            Assert.Equal([2, 2], g.AsEnumerable());
            g = await t.ReceiveAsync();
            Assert.Equal(1, g.Key);
            Assert.Equal([1], g.AsEnumerable());
            g = await t.ReceiveAsync();
            Assert.Equal(2, g.Key);
            Assert.Equal([2], g.AsEnumerable());
            await t.Completion;
        }

        [Fact]
        public async Task TestCancellation()
        {
            using var cts = new CancellationTokenSource();

            var input = new BufferBlock<int>(new());
            var testSubject =
                (GroupAdjacentBlock<int, int, int>)
                    input.GroupAdjacent(e => e, e => e, new() { CancellationToken = cts.Token });

            input.PostAsserted(1);
            input.PostAsserted(1);
            input.LinkTo(testSubject);
            await TestToolExtensions.Eventually(() => Assert.Equal(0, input.Count));

            cts.Cancel();
            await Task.WhenAny(testSubject.Completion);

            input.PostAsserted(1);


            Assert.True(testSubject.Completion.IsCanceled);
            Assert.Equal(1, input.Count);
            Assert.Equal(0, testSubject.Count);
        }
    }
}
