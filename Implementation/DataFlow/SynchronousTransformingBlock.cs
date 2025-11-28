using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    /// <summary>
    /// Important: transform must be idempotent and cheap! It will be run multiple times for the same input in some scenarios.
    /// </summary>
    public sealed class SynchronousTransformingBlock<I, O>: IReceivableSourceBlock<O>
    {
        private readonly ISourceBlock<I> _sourceBlock;
        private Func<I, O> Transform { get; }
        private readonly DummyTarget _target;
        private record LastMessage(I Message, O TransformedMessage);
        private object OutgoingLock { get; } = new();
        private ITargetBlock<O>? reservationTarget;

        private static readonly IEqualityComparer<I> Comparer = typeof(I).IsValueType ? EqualityComparer<I>.Default : new ReferenceComparer();

        private class ReferenceComparer : IEqualityComparer<I>
        {
            public bool Equals(I? x, I? y) => ReferenceEquals(x, y);
            public int GetHashCode([DisallowNull] I obj) => RuntimeHelpers.GetHashCode(obj);
        }

        private object Lock { get; } = new();
        private LastMessage? lastMessage;

        public SynchronousTransformingBlock(ISourceBlock<I> sourceBlock, Func<I, O> transform)
        {
            _sourceBlock = sourceBlock;
            _target = new(this);
            Transform = message =>
            {
                lock (Lock)
                {
                    if (lastMessage == null || !Comparer.Equals(message, lastMessage.Message))
                    {
                        lastMessage = new LastMessage(message, transform(message));
                    }
                    else
                    {
                        //Serve the cached transformed value.
                    }
                    return lastMessage.TransformedMessage;
                }
            };
        }

        public IDisposable LinkTo(ITargetBlock<O> target, DataflowLinkOptions linkOptions)
        {
            var t = new LinkTarget(this, target);
            return _sourceBlock.LinkTo(t, linkOptions);
        }

        public Task Completion => _sourceBlock.Completion;

        public void Complete() => _sourceBlock.Complete();

        public void Fault(Exception exception) => _sourceBlock.Fault(exception);

        public O? ConsumeMessage(
            DataflowMessageHeader messageHeader,
            ITargetBlock<O> target,
            out bool messageConsumed
        )
        {
            messageConsumed = false;
            I? ret = default;
            lock (OutgoingLock)
            {
                if (reservationTarget == null || reservationTarget == target)
                {
                    ret = _sourceBlock.ConsumeMessage(messageHeader, _target, out messageConsumed);
                    reservationTarget = null;
                }
            }
            return messageConsumed ? Transform(ret!) : default;
        }

        public void ReleaseReservation(
            DataflowMessageHeader messageHeader,
            ITargetBlock<O> target
        )
        {
            lock(OutgoingLock)
            {
                if (reservationTarget == target)
                {
                    _sourceBlock.ReleaseReservation(messageHeader, _target);
                    reservationTarget = null;
                }
            }
        }

        public bool ReserveMessage(
            DataflowMessageHeader messageHeader,
            ITargetBlock<O> target
        )
        {
            lock (OutgoingLock)
            {
                if (reservationTarget == null || reservationTarget == target)
                {
                    var res = _sourceBlock.ReserveMessage(messageHeader, _target);
                    if (res)
                    {
                        reservationTarget = target;
                    }
                    return res;
                } else
                {
                    return false; //other target already holds a reservation.
                }
            }
        }

        public bool TryReceive(Predicate<O>? filter, [MaybeNullWhen(false)] out O item)
        {
            var filter2 = filter == null ? null : new Predicate<I>(i => filter(Transform(i)));

            if (_sourceBlock is IReceivableSourceBlock<I> receivableSource &&
                receivableSource.TryReceive(filter2, out var v)
            )
            {
                item = Transform(v!);
                return true;
            }
            else
            {
                item = default;
                return false;
            }
        }

        public bool TryReceiveAll([NotNullWhen(true)] out IList<O>? items)
        {
            if (_sourceBlock is IReceivableSourceBlock<I> receivableSource &&
                receivableSource.TryReceiveAll(out var vs)
            )
            {
                var ret = new List<O>(vs.Count);
                foreach (var v in vs)
                {
                    ret.Add(Transform(v!));
                }
                items = ret;
                return true;
            }
            else
            {
                items = default;
                return false;
            }
        }

        private sealed class LinkTarget(SynchronousTransformingBlock<I,O> outer, ITargetBlock<O> target) : ITargetBlock<I>
        {
            public Task Completion => target.Completion;

            public void Complete() => target.Complete();
            public void Fault(Exception exception) => target.Fault(exception);
            public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, I messageValue, ISourceBlock<I>? source, bool consumeToAccept)
                => target.OfferMessage(messageHeader, outer.Transform(messageValue), outer, consumeToAccept);
        }

        private sealed class DummyTarget(SynchronousTransformingBlock<I, O> outer) : ITargetBlock<I>
        {
            public Task Completion => outer.Completion;
            public void Complete() => outer.Complete();
            public void Fault(Exception exception) => outer.Fault(exception);
            public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, I messageValue, ISourceBlock<I>? source, bool consumeToAccept)
                => DataflowMessageStatus.Postponed;
        }
    }
}
