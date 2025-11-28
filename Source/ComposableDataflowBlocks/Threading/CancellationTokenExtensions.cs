using System;
using System.Threading;
using System.Threading.Tasks;

namespace CounterpointCollective.Threading
{
    public static class CancellationTokenExtensions
    {
        public static Task WaitAsync(this CancellationToken token, CancellationToken cancellationToken = default)
        {
            if (token.IsCancellationRequested)
            {
                return Task.CompletedTask;
            }
            else if (cancellationToken.IsCancellationRequested)
            {
                return Task.FromCanceled(cancellationToken);
            }
            else
            {
                return DoAwait();
            }

            async Task DoAwait()
            {
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                using var r1 = token.Register(() => tcs.TrySetResult());
                using var r2 = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));
                await tcs.Task;            
            }
        }
    }
}
