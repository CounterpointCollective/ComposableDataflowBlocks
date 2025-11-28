using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    /// <summary>
    /// Block means not to accept messages if the predicate does not hold.
    /// Drop means the messages are accepted (and disappear) if the predicate does not hold.
    /// </summary>
    public enum FilterMode
    {
        Block,
        Drop
    }

    public sealed class SynchronousFilterBlock<T> : IReceivableSourceBlock<T>
    {

        private readonly ISourceBlock<T> _sourceBlock;
        private Predicate<T> Predicate { get; }
        private readonly DummyTarget _target;
        private object OutgoingLock { get; } = new();
        private ITargetBlock<T>? reservationTarget;
        public FilterMode Mode { get; set; }

        public SynchronousFilterBlock(ISourceBlock<T> sourceBlock, Predicate<T> predicate, FilterMode mode = FilterMode.Block)
        {
            Mode = mode;
            _sourceBlock = sourceBlock;
            _target = new(this);
            Predicate = predicate;
        }

        public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions)
        {
            var t = new LinkTarget(this, target);
            return _sourceBlock.LinkTo(t, linkOptions);
        }

        public Task Completion => _sourceBlock.Completion;

        public void Complete() => _sourceBlock.Complete();

        public void Fault(Exception exception) => _sourceBlock.Fault(exception);

        public T? ConsumeMessage(
            DataflowMessageHeader messageHeader,
            ITargetBlock<T> target,
            out bool messageConsumed
        )
        {
            messageConsumed = false;
            T? ret = default;
            lock (OutgoingLock)
            {
                if (reservationTarget == null || reservationTarget == target)
                {
                    ret = _sourceBlock.ConsumeMessage(messageHeader, _target, out messageConsumed);
                    reservationTarget = null;
                }
            }
            return ret;
        }

        public void ReleaseReservation(
            DataflowMessageHeader messageHeader,
            ITargetBlock<T> target
        )
        {
            lock (OutgoingLock)
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
            ITargetBlock<T> target
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
                }
                else
                {
                    return false; //other target already holds a reservation.
                }
            }
        }

        public bool TryReceive(Predicate<T>? filter, [MaybeNullWhen(false)] out T item)
        {
            if (_sourceBlock is IReceivableSourceBlock<T> receivableSource &&
                receivableSource.TryReceive((filter == null) ? Predicate : i => Predicate(i) && filter(i), out item)
            )
            {
                return true;
            }
            else
            {
                item = default;
                return false;
            }
        }

        public bool TryReceiveAll([NotNullWhen(true)] out IList<T>? items)
        {
            List<T> l = [];
            while(TryReceive(null, out var item))
            {
                l.Add(item);
            }

            if (l.Count > 0)
            {
                items = l;
                return true;
            }
            else
            {
                items = null;
                return false;
            }
        }

        private sealed class LinkTarget(SynchronousFilterBlock<T> outer, ITargetBlock<T> target) : ITargetBlock<T>
        {
            public Task Completion => target.Completion;

            public void Complete() => target.Complete();
            public void Fault(Exception exception) => target.Fault(exception);
            public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
            {
                if (!outer.Predicate(messageValue))
                {
                    return outer.Mode == FilterMode.Drop ? DataflowMessageStatus.Accepted : DataflowMessageStatus.Declined;
                }
                else
                {
                    return target.OfferMessage(messageHeader, messageValue, outer, consumeToAccept);
                }
            }
        }

        private sealed class DummyTarget(SynchronousFilterBlock<T> outer) : ITargetBlock<T>
        {
            public Task Completion => outer.Completion;
            public void Complete() => outer.Complete();
            public void Fault(Exception exception) => outer.Fault(exception);
            public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
                => DataflowMessageStatus.Postponed;
        }
    }
}
