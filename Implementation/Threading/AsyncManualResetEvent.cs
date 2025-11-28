using System.Threading.Tasks;

namespace CounterpointCollective.Threading
{
    public class AsyncManualResetEvent
    {
        private volatile TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task WaitAsync() => tcs.Task;

        public void Set() => tcs.TrySetResult(true);

        public void Reset()
        {
            if (tcs.Task.IsCompleted)
            {
                tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }
    }
}
