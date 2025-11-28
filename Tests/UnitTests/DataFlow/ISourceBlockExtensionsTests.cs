using CounterpointCollective.DataFlow;

namespace UnitTests.DataFlow
{
    public class ISourceBlockExtensionsTests
    {
        [Fact]
        public async Task TransformManyPreservesOrder()
        {
            var range = Enumerable.Range(0, 65536).ToList();

            var transformed = await range
                .ToAsyncEnumerable()
                .AsSourceBlock()
                .Batch(32)
                .Transform(
                    x => x.ToAsyncEnumerable().AsSourceBlock(),
                    new()
                )
                .Flatten()
                .AsAsyncEnumerable()
                .ToListAsync();
            Assert.Equal(range, transformed);
        }
    }
}
