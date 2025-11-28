using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CounterpointCollective.DataFlow.Encapsulation;
using CounterpointCollective.DataFlow.Internal;
namespace CounterpointCollective.DataFlow
{
    public record AdmissionGateHooks
    {
        /// <summary>
        /// When this returns true, always either Entering or FailingToEnter will be called afterwards.
        /// Will not be called multiple times concurrently.
        /// </summary>
        public Func<bool>? MayTryToEnter { get; set; }

        /// <summary>
        /// A message is successfully entering the block.
        /// Allows you to commit state changes accordingly.
        /// Will not be called multiple times concurrently.
        /// </summary>
        public Action? Entering { get; set; }

        /// <summary>
        /// Called when a message has successfully entered the block, and after all state changes.
        /// </summary>
        public Action? HasEntered { get; set; }

        /// <summary>
        /// Although a message was allowed to enter, it failed to do so (e.g. because it could not be consumed from the source block).
        /// Allows you to roll back any state changes made in MayTryToEnter.
        /// Will not be called multiple times concurrently.
        /// </summary>
        public Action? FailingToEnter { get; set; } = () => { };
    }

    public class AdmissionGateBlock<T> : AbstractEncapsulatedTargetBlock<T> 
    {
        protected override ITargetBlock<T> TargetSide { get; }
        protected override IDataflowBlock CompletionSide => this;

        public override Task Completion { get; }
        private readonly UnboundedTargetBlock<T> _inner;

        private AdmissionGateHooks hooks;
        private readonly PostponedMessages<AdmissionGateBlock<T>, T> _postponedMessages;
        private readonly PostponedMessagesManager<AdmissionGateBlock<T>, T> _postponedMessagesManager;

        private readonly TaskCompletionSource _tcsCompletionRequest = new();
        public bool IsCompletionRequested => _tcsCompletionRequest.Task.IsCompleted;
        public Task InputCompletion => _inner.InputCompletion;

        public CancellationToken CancellationToken { get; }

        public void ConfigureHooks(AdmissionGateHooks h)
        {
            lock(_postponedMessagesManager.IncomingLock)
            {
                hooks = Clone(h);
                _postponedMessagesManager.ProcessPostponedMessages();
            }
        }

        public AdmissionGateBlock(ITargetBlock<T> buffer, AdmissionGateHooks h)
        {
            TargetSide = new Target(this);
            hooks = Clone(h);

            _inner = new UnboundedTargetBlock<T>(buffer);

            _postponedMessages = new(this);
            _postponedMessagesManager = new(
                (out PostponedMessage<AdmissionGateBlock<T>, T> p) =>
                    _postponedMessages.TryPeekPostponed(out p)
                    && hooks.MayTryToEnter !=  null
                    && hooks.MayTryToEnter()
                    && _postponedMessages.TryDequeuePostponed(out p));


            _postponedMessagesManager.Consume =
                (in PostponedMessage<AdmissionGateBlock<T>, T> postponedMessage) =>
                {
                    var succ = postponedMessage.Consume(out var msg);
                    lock (_postponedMessagesManager.IncomingLock)
                    {
                        if (succ)
                        {
                            hooks.Entering?.Invoke();
                            _inner.PostAsserted(msg!);
                            hooks.HasEntered?.Invoke();
                        }
                        else
                        {
                            hooks.FailingToEnter?.Invoke();
                        }
                    }
                };

            var tcs = new TaskCompletionSource();
            Completion = tcs.Task;
            Task.Run(async () =>
            {
                var t1 = await Task.WhenAny(_tcsCompletionRequest.Task);
                var t2 = await Task.WhenAny(_postponedMessagesManager.ShutdownAsync());
                _ = t1.PropagateCompletion(_inner);
                var t3 = await Task.WhenAny(_inner.Completion);
                await Task.WhenAll(t3).PropagateCompletionAsync(tcs);
            }, CancellationToken.None);

            _inner.Completion.ContinueWith(_ =>
            {
                if (!IsCompletionRequested)
                {
                    Fault(new InvalidOperationException("Source side completed unexpectedly."));
                }
            });
        }

        public void ProcessPostponedMessages()
        {
            if (hooks.MayTryToEnter != null)
            {
                _postponedMessagesManager.ProcessPostponedMessages();
            } else
            {
                //nothing to do because we always allow every message anyway.
            }
        }

        public override void Fault(Exception exception) => _tcsCompletionRequest.TrySetException(exception);
        public override void Complete() => _tcsCompletionRequest.TrySetResult();

        private class Target(AdmissionGateBlock<T> outer) : ITargetBlock<T>
        {
            public Task Completion => outer.Completion;
            public void Complete() => outer.Complete();
            public void Fault(Exception exception) => outer.Fault(exception);
            public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
            {
                if (outer.IsCompletionRequested)
                {
                    return DataflowMessageStatus.DecliningPermanently;
                }
                if (consumeToAccept && source == null)
                {
                    return DataflowMessageStatus.Declined; //not allowed in protocol
                }

                lock (outer._postponedMessagesManager.IncomingLock)
                {
                    var allow =
                        outer.hooks.MayTryToEnter == null
                        || (!outer._postponedMessagesManager.IsRunningPostponedMessages && outer.hooks.MayTryToEnter());

                    if (!allow)
                    {
                        if (source != null)
                        {
                            outer._postponedMessages.Postpone(source, messageHeader);
                            return DataflowMessageStatus.Postponed;
                        }
                        else
                        {
                            return DataflowMessageStatus.Declined;
                        }
                    }
                    else if (consumeToAccept)
                    {
                        var newValue = source!.ConsumeMessage(messageHeader, this, out var messageConsumed);
                        if (!messageConsumed)
                        {
                            outer.hooks.FailingToEnter?.Invoke();
                            return DataflowMessageStatus.NotAvailable;
                        }
                        else
                        {
                            messageValue = newValue!;
                        }
                    }
                    outer.hooks.Entering?.Invoke();
                    outer._inner.PostAsserted(messageValue);
                    outer.hooks.HasEntered?.Invoke();
                    return DataflowMessageStatus.Accepted;
                }
            }
        }

        private static AdmissionGateHooks Clone(AdmissionGateHooks hooks) => new()
        {
            Entering = hooks.Entering,
            FailingToEnter = hooks.FailingToEnter,
            MayTryToEnter = hooks.MayTryToEnter,
            HasEntered = hooks.HasEntered,
        };
    }
}
