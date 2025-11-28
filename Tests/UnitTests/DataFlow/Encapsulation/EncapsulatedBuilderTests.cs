using CounterpointCollective.DataFlow;
using CounterpointCollective.DataFlow.Encapsulation;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow.Encapsulation
{
    public class EncapsulatedBuilderTests
    {
        [Fact]
        public async Task TestPropagationToActionBlock()
        {
            var actionBlock = new ActionBlock<int>(i => { });
            var a = Enumerable.Range(1, 100).ToAsyncEnumerable().AsSourceBlock();
            var p = a
                .BeginEncapsulation()
                .LinkTo(actionBlock, new DataflowLinkOptions { PropagateCompletion = true })
                .BuildTargetBlock();

            p.Complete();
            await actionBlock.Completion;
        }

        [Fact]
        public async Task TestEnumerationIsFaulted()
        {
            var b = Enumerable.Range(1, 100)
                .ToAsyncEnumerable()
                .Select(async (e,ct) =>
                {
                    await Task.Delay(1000, ct);
                    return e;
                })
                .AsSourceBlock();

            var t = b.BeginEncapsulation()
                .LinkTo(new TransformBlock<int, int>(i => i + 1), new() { PropagateCompletion = true })
                .Build();

            var a = t.AsAsyncEnumerable();

            var f = await a.FirstAsync();

            await Task.WhenAny(b.Completion);
            Assert.True(b.Completion.IsFaulted);

        }

        [Fact]
        public async Task TestTee()
        {
            var testSubject =
                Enumerable.Range(1, 100).ToAsyncEnumerable().AsSourceBlock()
                .BeginEncapsulation()
                .Tee(tb =>
                    tb
                    .ConfigureBranch(e => e.Transform(i => i * 2))
                    .CombineWith((i, t) => i * t)
                )
                .BuildSourceBlock();

            var res = await testSubject.AsAsyncEnumerable().ToListAsync();
            await testSubject.Completion;

            var expected = Enumerable.Range(1, 100).Select(e => e * e * 2);
            Assert.False(
                res.Zip(expected).Where(p => p.First != p.Second).Any()
            );
        }

        [Fact]
        public async Task TestTeeWithExoticExhaustions()
        {
            var testSubject =
                Enumerable.Range(1, 100).ToAsyncEnumerable().AsSourceBlock()
                .BeginEncapsulation()
                .Transform(e => e.ToString())
                .Tee(tb =>
                    tb
                    //this will TryReceiveAll.
                    .ConfigureBranch(e => e.BuildSourceBlock().AsAsyncEnumerable().AsSourceBlock().BeginEncapsulation())
                    .CombineWith((i, t) =>
                    {
                        Assert.Equal(i, t);
                        return i;
                    })
                )
                .BuildSourceBlock();

            var res = await testSubject.AsAsyncEnumerable().ToListAsync();
            await testSubject.Completion;
        }

        [Fact]
        public async Task TestTeeUnit()
        {
            var r = Enumerable.Range(1, 100);
            var testSubject =
                r.ToAsyncEnumerable().AsSourceBlock()
                .BeginEncapsulation()
                .Tee(tb => tb)
                .BuildSourceBlock();

            var res = await testSubject.AsAsyncEnumerable().ToListAsync();
            await testSubject.Completion;
            Assert.Equal(r.Zip(r), res);
        }
    }
}
