using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public class BoundedPropagatorBlock<I,O> : BoundedTargetBlock<I>, IReceivableSourceBlock<O>, IPropagatorBlock<I,O>
    {
        private ISourceBlock<O> SourceSide { get; }
        public BoundedPropagatorBlock(
            IPropagatorBlock<I, O> inner,
            int boundedCapacity = DataflowBlockOptions.Unbounded,
            Action? onEntered = null
        ) : base(inner, boundedCapacity, onEntered) => SourceSide = CreateExit(inner);

        public BoundedPropagatorBlock(
            ITargetBlock<I> entrance,
            ISourceBlock<O> exit,
            int boundedCapacity,
            Action? onEntered = null
        ) : this(DataflowBlock.Encapsulate(entrance, exit), boundedCapacity, onEntered)
        {
        }

        public O? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<O> target, out bool messageConsumed)
            => SourceSide.ConsumeMessage(messageHeader, target, out messageConsumed);
        public IDisposable LinkTo(ITargetBlock<O> target, DataflowLinkOptions linkOptions)
            => SourceSide.LinkTo(target, linkOptions);
        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<O> target)
            => SourceSide.ReleaseReservation(messageHeader, target);
        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<O> target)
            => SourceSide.ReserveMessage(messageHeader, target);
        public bool TryReceive(Predicate<O>? filter, [MaybeNullWhen(false)] out O item)
        {
            if (SourceSide is IReceivableSourceBlock<O> r && r.TryReceive(filter, out item))
            {
                return true;
            }
            else
            {
                item = default;
                return false;
            }
        }
        public bool TryReceiveAll([NotNullWhen(true)] out IList<O>? items) {
            if (SourceSide is IReceivableSourceBlock<O> r && r.TryReceiveAll(out items))
            {
                return true;
            }
            else
            {
                items = null;
                return false;
            }
        }
    }
}
