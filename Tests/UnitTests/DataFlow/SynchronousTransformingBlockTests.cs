using CounterpointCollective.DataFlow;

namespace UnitTests.DataFlow
{
    public class SynchronousTransformingBlockTests
    {
        [Fact]
        public async Task Test()
        {
            var a = Enumerable.Range(1, 10).ToAsyncEnumerable()
                .AsSourceBlock();

            var testSubject = new SynchronousTransformingBlock<int, int>(a, i => i * 2);

            var l = await testSubject.AsAsyncEnumerable().ToListAsync();
        }
    }
}
