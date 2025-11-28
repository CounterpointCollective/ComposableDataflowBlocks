using System.Threading.Tasks.Dataflow;
using Xunit;

namespace UnitTests.DataFlow
{
    public static class TestExtensions
    {
        public static async Task TestCancellationAsync(
            this IDataflowBlock testSubject,
            CancellationTokenSource cts
        )
        {
            cts.Cancel();
            try
            {
                await testSubject.Completion;
            }
            catch (OperationCanceledException) { }
            Assert.True(testSubject.Completion.IsCanceled);
        }
    }
}
