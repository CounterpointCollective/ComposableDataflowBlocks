using CounterpointCollective.DataFlow.Encapsulation;
using CounterpointCollective.DataFlow.Internal;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Notifying
{

    public delegate void ConfigureHooks<T>(SourceBlockNotificationHooks<T> hooks);
    /// <summary>
    /// Notifies via Hooks when a message is sent to a target block and when a reservation is released.
    /// </summary>
    public class SourceBlockWithNotification<T> : AbstractEncapsulatedSourceBlock<T>
    {
        private readonly Implementation _implementation;
        protected override IDataflowBlock CompleteSide => _implementation;

        protected override ISourceBlock<T> SourceSide => _implementation;

        /// <summary>
        /// Wait until all prior events have been dispatched.
        /// </summary>
        public void DispatchPendingEvents() => _implementation.DispatchPendingEvents();

        public SourceBlockWithNotification(ISourceBlock<T> innerBlock)
        {
            if (innerBlock is SourceBlockWithNotification<T> s)
            {
                //If the innerBlock is already a SourceBlockWithNotification, reuse its implementation.
                //This avoids wrapping multiple times; avoiding all the overhead of multiple event dispatching queues.
                _implementation = s._implementation;
            } else
            {
                _implementation = new(innerBlock);
            }
        }

        public SourceBlockWithNotification(ISourceBlock<T> innerBlock, ConfigureHooks<T> h) : this(innerBlock) => AddHooks(h);

        public SourceBlockWithNotification<T> AddHooks(ConfigureHooks<T> c)
        {
            _implementation.AddHooks(c);
            return this;
        }

        private class Implementation: IReceivableSourceBlock<T>
        {
            private object SynchronousOfferLock { get; } = new();

            private ISourceBlock<T> InnerBlock { get; }

            private readonly TaskCompletionSource _tcs = new();

            protected SourceBlockNotificationHooks<T> Hooks { get; }

            public void AddHooks(ConfigureHooks<T> c) => Hooks.Add(c);


            public Implementation(ISourceBlock<T> innerBlock)
            {
                InnerBlock = innerBlock;
                Hooks = new(this);
                Task.Run(async () =>
                {
                    await Task.WhenAny(InnerBlock.Completion);
                    Hooks.OnCompleting?.Invoke();
                    var t = await Task.WhenAny(Task.WhenAll(InnerBlock.Completion, Hooks.Completion));
                    await t.PropagateCompletionAsync(_tcs);
                });
            }


            public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions)
                => new InspectableLink(this, target, linkOptions);

            public Task Completion => _tcs.Task;

            public void Complete() => InnerBlock.Complete();

            public T? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target, out bool messageConsumed)
            {
                lock (SynchronousOfferLock)
                {
                    var ret = InnerBlock.ConsumeMessage(messageHeader, target, out messageConsumed);
                    if (messageConsumed)
                    {
                        Hooks.OnDeliveringMessages?.Invoke(new DeliveringMessagesEvent(1));
                    }
                    return ret;
                }
            }

            public void Fault(Exception exception) => InnerBlock.Fault(exception);

            public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
            {
                lock (SynchronousOfferLock)
                {
                    InnerBlock.ReleaseReservation(messageHeader, target);
                    Hooks.OnReservationReleased?.Invoke(new ReservationReleasedEvent());
                }
            }

            public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
                => InnerBlock.ReserveMessage(messageHeader, target);


            public bool TryReceive(Predicate<T>? filter, [MaybeNullWhen(false)] out T item)
            {
                if (InnerBlock is IReceivableSourceBlock<T> r)
                {
                    lock (SynchronousOfferLock)
                    {
                        var res = r.TryReceive(filter, out item);
                        if (res)
                        {
                            Hooks.OnDeliveringMessages?.Invoke(new DeliveringMessagesEvent(1));
                        }
                        return res;
                    }
                }
                else
                {
                    item = default;
                    return false;
                }
            }

            public bool TryReceiveAll([NotNullWhen(true)] out IList<T>? items)
            {
                if (InnerBlock is IReceivableSourceBlock<T> r)
                {
                    lock (SynchronousOfferLock)
                    {
                        var res = r.TryReceiveAll(out items);
                        if (res)
                        {
                            Hooks.OnDeliveringMessages?.Invoke(new DeliveringMessagesEvent(items!.Count));
                        }
                        return res;
                    }
                }
                else
                {
                    items = null;
                    return false;
                }
            }

            public void DispatchPendingEvents()
            {
                lock (SynchronousOfferLock)
                {
                }
            }

            private sealed class InspectableLink : ITargetBlock<T>, IDisposable
            {
                private readonly Implementation _outer;
                private readonly ITargetBlock<T> _target;
                private readonly IDisposable _link;
                private readonly TaskCompletionSource _tcsDisposed = new();

                private record Postponed(
                    ISourceBlock<T> Source,
                    DataflowMessageHeader MessageHeader,
                    T MessageValue
                );

                private Postponed? lastPostponed;
                private readonly PostponedMessagesManager<InspectableLink, T> _postponedMessagesManager;

                public InspectableLink(
                    Implementation outer,
                    ITargetBlock<T> target,
                    DataflowLinkOptions opts,
                    Predicate<T>? pred = null
                )
                {
                    _postponedMessagesManager = new(
                        (out PostponedMessage<InspectableLink, T> p) => {
                            p = default;

                            if (lastPostponed != null)
                            {
                                return true;
                            } else
                            {
                                return false;
                            }
                        }
                    );

                    _postponedMessagesManager.Consume = (in p) =>
                    {
                        if (lastPostponed != null)
                        {
                            lock (outer.SynchronousOfferLock)
                            {
                                OfferMessage(lastPostponed.MessageHeader, lastPostponed.MessageValue, lastPostponed.Source, true);
                            }
                            lastPostponed = null;
                        }
                    };

                    _outer = outer;
                    _target = target;

                    _link = pred == null ? _outer.InnerBlock.LinkTo(this, opts) : _outer.InnerBlock.LinkTo(this, opts, pred);
                    if (opts.PropagateCompletion)
                    {
                        Task.Run(async () =>
                        {
                            var ranToCompletion = _outer.Completion;

                            var t = await Task.WhenAny(_tcsDisposed.Task, ranToCompletion);
                            if (ranToCompletion.IsCompleted)
                            {
                                _ = ranToCompletion.PropagateCompletion(_target);
                            }
                        });
                    }
                }

                public DataflowMessageStatus OfferMessage(
                    DataflowMessageHeader messageHeader,
                    T messageValue,
                    ISourceBlock<T>? source,
                    bool consumeToAccept
                )
                {
                    if (Monitor.TryEnter(_outer.SynchronousOfferLock))
                    {
                        try
                        {
                            lock (_postponedMessagesManager.IncomingLock)
                            {
                                lastPostponed = null;
                            }
                            var res = _target.OfferMessage(messageHeader, messageValue, _outer, consumeToAccept);
                            if (!consumeToAccept && _outer.Hooks.OnDeliveringMessages != null && res == DataflowMessageStatus.Accepted)
                            {
                                _outer.Hooks.OnDeliveringMessages(new DeliveringMessagesEvent(1));
                            }
                            return res;
                        } finally
                        {
                            Monitor.Exit(_outer.SynchronousOfferLock);
                        }
                    } else
                    {
                        //We are already making calls into Source... We have to postpone to prevent deadlocks.
                        if (source != null) 
                        {
                            lock (_postponedMessagesManager.IncomingLock)
                            {
                                lastPostponed = new(source, messageHeader, messageValue);
                            }
                            _postponedMessagesManager.ProcessPostponedMessages();
                            return DataflowMessageStatus.Postponed;
                        } else
                        {
                            return DataflowMessageStatus.Declined;
                        }
                    }
                }

                public void Dispose()
                {
                    if (_tcsDisposed.TrySetResult())
                    {
                        _link.Dispose();
                    }
                }

                public Task Completion => _outer.Completion;

                public void Complete() => _outer.Complete();
                public void Fault(Exception exception) => _outer.Fault(exception);

            }
        }
    }
}
