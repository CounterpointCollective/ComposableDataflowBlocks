using CounterpointCollective.DataFlow.Encapsulation;
using System;
using System.Diagnostics.Contracts;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    /// <summary>
    /// Will always accept messages until it is explicitly told to complete or fault.
    /// </summary>
    public sealed class UnboundedTargetBlock<T> : ITargetBlock<T>
    {
        private object Lock { get; } = new object();

        private readonly ITargetBlock<T> _inner;
        private BufferBlock<T>? buffer;

        private ITargetBlock<T> target;
        private readonly TaskCompletionSource _tcsCompletionRequest = new();
        private readonly TaskCompletionSource _tcsInputCompletion = new();

        public bool IsCompletionRequested => _tcsCompletionRequest.Task.IsCompleted;
        public Task InputCompletion => _tcsInputCompletion.Task;

        public Task Completion => target.Completion;

        public void Complete()
        {
            lock(Lock)
            {
                _tcsCompletionRequest.TrySetResult();
            }
        }

        public void Fault(Exception exception)
        {
            lock (Lock)
            {
                _tcsCompletionRequest.TrySetException(exception);
            }
        }

        public UnboundedTargetBlock(ITargetBlock<T> inner)
        {
            _inner = inner;
            target = inner;

            Task.Run(async () =>
            {
                await Task.WhenAny(_tcsCompletionRequest.Task);
                await _tcsCompletionRequest.Task.PropagateCompletion(target);
                var inputComplete = (buffer == null) ? _tcsCompletionRequest.Task : buffer.Completion;
                await inputComplete.PropagateCompletionAsync(_tcsInputCompletion);
            });
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
        {
            //Optimistic path: is not completing, first try without locks to feed it to the target.
            if (IsCompletionRequested)
            {
                return DataflowMessageStatus.DecliningPermanently;
            }
            var ret = target.OfferMessage(messageHeader, messageValue, null, consumeToAccept);
            if (ret == DataflowMessageStatus.Accepted)
            {
                return ret;
            }

            //Optimistic path failed. Acquire the lock and find out what happened.
            lock (Lock)
            {
                //Another thread may have completed concurrently.
                if (IsCompletionRequested)
                {
                    return DataflowMessageStatus.DecliningPermanently;
                }
                //Or we may need to install a buffer, in case another thread didn't do it concurrently.
                if (buffer == null)
                {
                    buffer = new BufferBlock<T>();
                    target =
                        buffer
                        .BeginEncapsulation()
                        .LinkTo(_inner, new DataflowLinkOptions() { PropagateCompletion = true })
                        .BuildTargetBlock();
                }
                ret = target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
                if (ret != DataflowMessageStatus.Accepted)
                {
                    //This cannot happen in practice.
                    throw new InvalidOperationException("Protocol breakdown. Target must always accept unless we are completed");
                }
                return ret;
            }
        }
    }
}
