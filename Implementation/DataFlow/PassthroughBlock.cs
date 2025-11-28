using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CounterpointCollective.DataFlow.Internal;
using CounterpointCollective.Threading;

namespace CounterpointCollective.DataFlow
{
    /// <summary>
    /// A zero-capacity BufferBlock that acts as the "identity" or neutral element 
    /// in composite dataflow blocks. 
    /// 
    /// Messages offered to this block are immediately forwarded to all linked targets. 
    /// A message is considered accepted as soon as the first target accepts it. 
    /// If a new target is linked, the oldest postponed message (if any) is offered to it automatically.
    /// 
    /// This block is typically used as a building block for more complex sources or targets, 
    /// providing a simple pass-through mechanism without storing messages.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class PassthroughBlock<T> : IPropagatorBlock<T, T>
    {
        private object Lock { get; } = new();
        private object OfferLock { get; } = new();

        private readonly PostponedMessages<PassthroughBlock<T>, T> _postponedMessages;
        private readonly Dictionary<ISourceBlock<T>, Message> _postponedValues = [];

        private bool TryGetCurrentMessage([MaybeNullWhen(false)] out Message msg)
        {
            Debug.Assert(Monitor.IsEntered(Lock));
            if (_postponedMessages.TryPeekPostponed(out var pm))
            {
                msg = _postponedValues[pm.SourceBlock];
                if (msg.SourceHeader != pm.MessageHeader)
                {
                    throw new InvalidOperationException("administration error");
                }
                return true;
            }
            else
            {
                msg = null;
                return false;
            }
        }

        private record Message(ISourceBlock<T> Source, DataflowMessageHeader SourceHeader, DataflowMessageHeader MyHeader, T Value)
        {
            public bool HasBeenOffered { get; set; }
            public ITargetBlock<T>? ReservedBy { get; set; }
        }

        private long i;
        private readonly BinarySemaphoreSlim _semPendingWork = new(false);

        private bool CanDoPendingWorkNow()
        {
            Debug.Assert(Monitor.IsEntered(Lock));
            return
                !Monitor.IsEntered(OfferLock)
                && _links.Count > 0
                && TryGetCurrentMessage(out var c)
                && c.HasBeenOffered == false;
        }

        /// <summary>
        /// Call this whenever you release the OfferLock
        /// </summary>
        private void SchedulePendingWork()
        {
            if (CanDoPendingWorkNow())
            {
                _semPendingWork.Release();
            }
        }

        private record Link(ITargetBlock<T> Target, DataflowLinkOptions Options);

        private readonly List<Link> _links = [];
        private readonly TaskCompletionSourceWithCancellation _completionRequest;
        private readonly TaskCompletionSource _tcs = new();
        private readonly CountdownEventSlim _completionCountDown = new(1);

        public PassthroughBlock(CancellationToken cancellationToken = default)
        {
            _postponedMessages = new(this);
            _completionRequest = new(cancellationToken);

            var pendingWorkLoop = Task.Run(() => ProcessPendingWork(), CancellationToken.None);

            _ = Task.Run(async () =>
                {
                    await Task.WhenAny(_completionRequest.Completion);
                    lock(Lock)
                    {
                        if (TryGetCurrentMessage(out var c) && c.ReservedBy != null)
                        {
                            ReleaseReservation(c.SourceHeader, c.ReservedBy);
                        }
                        _postponedMessages.Clear();
                    }
                    _semPendingWork.Terminate();
                    await Task.WhenAny(pendingWorkLoop);
                    _completionCountDown.Decrement();
                    await _completionCountDown.WaitAsync();
                    var t = Task.WhenAll(pendingWorkLoop, _completionRequest.Completion);
                    await t.PropagateCompletionAsync(_tcs);
                }, CancellationToken.None);

            Completion.ContinueWith(t =>
            {
                lock (Lock)
                {
                    foreach (var l in _links)
                    {
                        if (l.Options.PropagateCompletion)
                        {
                            t.PropagateCompletion(l.Target);
                        }
                    }
                }
                lock (OfferLock)
                {
                    lock(Lock)
                    {
                        _links.Clear();
                    }
                }
            }, CancellationToken.None);
        }

        private async Task ProcessPendingWork()
        {
            while (true)
            {
                try
                {
                    await _semPendingWork.WaitAsync();
                } catch (OperationCanceledException)
                {
                    break;
                }

                while (true)
                {
                    lock (Lock)
                    {
                        if (!CanDoPendingWorkNow())
                        {
                            _semPendingWork.Reset(); //drain the semaphore in case it is set. We just checked there is nothing to do.
                            break;
                        }
                    }

                    lock (OfferLock)
                    {
                        Message? m = null;
                        List<Link> links = default!;

                        lock (Lock)
                        {
                            if (TryGetCurrentMessage(out m) && m.HasBeenOffered == false)
                            {
                                m.HasBeenOffered = true;
                                if (_links.Count > 0)
                                {
                                    links = [.._links];
                                } else
                                {
                                    m = null;
                                }
                            }
                        }

                        if (m != null)
                        {
                            OfferSynchronously(m.Source, m.Value, true, links, m.MyHeader, m.SourceHeader);
                        }
                    }
                }
            }
        }

        #region completion
        public Task Completion => _tcs.Task;

        public void Complete() => _completionRequest.Complete();
        public void Fault(Exception exception) => _completionRequest.Fault(exception);
        #endregion

        public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions)
            => _completionCountDown.SupressingCompletion(() =>
            {

                var link = new Link(target, linkOptions);
                bool serviceImmediately;
                Message? m;
                lock (OfferLock)
                {
                    lock (Lock)
                    {
                        if (linkOptions.Append)
                        {
                            _links.Add(link);
                        }
                        else
                        {
                            _links.Insert(0, link);
                        }

                        m = null;
                        var h = TryGetCurrentMessage(out m);
                        serviceImmediately = h && m!.HasBeenOffered;
                    }

                    if (serviceImmediately)
                    {
                        target.OfferMessage(m!.MyHeader, m.Value, this, true);
                    }
                }

                lock (Lock)
                {
                    SchedulePendingWork();
                }

                return new ActionDisposable(() =>
                {
                    lock (OfferLock)
                    {
                        lock (Lock)
                        {
                            _links.Remove(link);
                        }
                    }
                });
            },
            () =>
            {
                if (linkOptions.PropagateCompletion)
                {
                    Completion.PropagateCompletion(target);
                }
                return new ActionDisposable(() => { });
            }
        );

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
            =>
            _completionCountDown.SupressingCompletion(() =>
            {
                List<Link> links = null!;
                DataflowMessageHeader myHeader = default;

                try
                {
                    return
                    OfferLock.TryLock(haveOfferLock =>
                    {
                        bool offerSynchronously;
                        lock (Lock)
                        {
                            myHeader = new DataflowMessageHeader(++i);

                            Message m = null!;
                            if (source != null)
                            {
                                m = new Message(source, messageHeader, myHeader, messageValue);
                                _postponedMessages.Postpone(source, messageHeader);
                                _postponedValues[source] = m;
                            }
                            var onlyMessage = source == null ? _postponedMessages.Count == 0 : _postponedMessages.Count == 1;
                            offerSynchronously = haveOfferLock && onlyMessage;
                            if (offerSynchronously)
                            {
                                if (m != null)
                                {
                                    m.HasBeenOffered = true;
                                }
                                links = [.. _links];
                            }
                        }

                        var ret = source == null ? DataflowMessageStatus.Declined : DataflowMessageStatus.Postponed;
                        if (offerSynchronously && links.Count > 0)
                        {
                            ret = OfferSynchronously(source, messageValue, consumeToAccept, links, myHeader, messageHeader);
                        }

                        return ret;
                    });
                } finally
                {
                    lock (Lock)
                    {
                        SchedulePendingWork();
                    }
                }
            },
            () => DataflowMessageStatus.DecliningPermanently
        );

        private DataflowMessageStatus OfferSynchronously(ISourceBlock<T>? sourceBlock, T messageValue, bool consumeToAccept, List<Link> links, DataflowMessageHeader myHeader, DataflowMessageHeader sourceHeader)
        {
            Debug.Assert(Monitor.IsEntered(OfferLock));
            var ret = DataflowMessageStatus.Postponed;
            var accepted = false;
            foreach (var l in links)
            {
                var r = l.Target.OfferMessage(myHeader, messageValue, this, consumeToAccept);
                if (r == DataflowMessageStatus.Accepted)
                {
                    accepted = true;
                    if (sourceBlock != null)
                    {
                        lock (Lock)
                        {
                            if (_postponedMessages.Unregister(sourceBlock, sourceHeader))
                            {
                                _postponedValues.Remove(sourceBlock);
                            }
                        }
                    }
                    break;
                }
                else if (r == DataflowMessageStatus.DecliningPermanently)
                {
                    lock (Lock)
                    {
                        _links.Remove(l);
                    }
                }
            }

            ret = accepted ? DataflowMessageStatus.Accepted : sourceBlock != null ? DataflowMessageStatus.Postponed : DataflowMessageStatus.Declined;
            return ret;
        }

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
            =>
            _completionCountDown.SupressingCompletion(
                () =>
                {
                    lock (Lock)
                    {
                        if (TryGetCurrentMessage(out var c) && c.ReservedBy == target)
                        {
                            c.Source.ReleaseReservation(c.SourceHeader, target);
                            c.ReservedBy = null;
                        }
                    }
                }
                , () => { }
            );

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
            => _completionCountDown.SupressingCompletion(
                () =>
                {
                    lock (Lock)
                    {
                        if (TryGetCurrentMessage(out var c)
                            && c.MyHeader == messageHeader
                            && (c.ReservedBy == null || c.ReservedBy == target))
                        {
                            var ret = c.Source.ReserveMessage(c.SourceHeader, target);
                            if (ret)
                            {
                                c.ReservedBy = target;
                            }
                            return ret;
                        }
                        else
                        {
                            return false;
                        }
                    }
                },
                () => false
            );

        public T? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target, out bool messageConsumed)
        {
            var consumed = false;
            var ret = _completionCountDown.SupressingCompletion(() =>
            {
                Message? c;
                bool consume;
                lock (Lock)
                {
                    consume = TryGetCurrentMessage(out c)
                        && c.MyHeader == messageHeader
                        && (c.ReservedBy == null || c.ReservedBy == target);
                }
                if (consume)
                {
                    var r = c!.Source.ConsumeMessage(c.SourceHeader, target, out consumed);
                    lock (Lock)
                    {
                        if (TryGetCurrentMessage(out c) && c.MyHeader == messageHeader)
                        {
                            if (_postponedMessages.Unregister(c.Source, c.SourceHeader))
                            {
                                _postponedValues.Remove(c.Source);
                            }
                        }
                        SchedulePendingWork();
                    }
                    return r;
                }
                return default;
            },
                () => default
            );
            messageConsumed = consumed;
            return ret;
        }
    }
}
