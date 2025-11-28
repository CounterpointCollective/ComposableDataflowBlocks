using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Internal
{
#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class TaskCompletionSourceWithCancellation : IDataflowBlock
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        private readonly TaskCompletionSource _tcsStartCompletion = new();
        private readonly CancellationTokenRegistration _ctr;

        public Task Completion => _tcsStartCompletion.Task;

        public TaskCompletionSourceWithCancellation(CancellationToken cancellationToken)
        {
            _ctr = cancellationToken.Register(Cancel);

            _tcsStartCompletion.Task.ContinueWith(_ =>
            {
                _ctr.Unregister();

            }, CancellationToken.None);
        }

        public void Complete() => _tcsStartCompletion.TrySetResult();

        public void Fault(Exception exception) => _tcsStartCompletion.TrySetException(exception);

        private void Cancel() => _tcsStartCompletion.TrySetCanceled();
    }
}
