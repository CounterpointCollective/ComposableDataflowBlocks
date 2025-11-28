using System;
using System.Threading.Tasks.Dataflow;
using CounterpointCollective.DataFlow;
using Xunit;

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
                .TransformMany(
                    x => x.ToAsyncEnumerable().AsSourceBlock(),
                    new() { MaxDegreeOfParallelism = 10, EnsureOrdered = true },
                    new()
                )
                .AsAsyncEnumerable()
                .ToListAsync();
            Assert.Equal(range, transformed);
        }
    }
}
