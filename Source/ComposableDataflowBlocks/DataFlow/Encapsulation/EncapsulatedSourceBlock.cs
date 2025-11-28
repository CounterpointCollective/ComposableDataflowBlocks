using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Encapsulation
{
    public sealed class EncapsulatedSourceBlock<T>(IDataflowBlock completeSide, ISourceBlock<T> sourceSide) : AbstractEncapsulatedSourceBlock<T>
    {
        protected override IDataflowBlock CompleteSide { get; } = completeSide;
        protected override ISourceBlock<T> SourceSide { get; } = sourceSide;
    }

    public abstract class AbstractEncapsulatedSourceBlock<T>: IReceivableSourceBlock<T>
    {
        protected abstract IDataflowBlock CompleteSide { get; }
        protected abstract ISourceBlock<T> SourceSide { get; }


        public virtual void Complete()
            => CompleteSide.Complete();

        public virtual void Fault(Exception exception)
            => CompleteSide.Fault(exception);

        public virtual Task Completion
            => SourceSide.Completion;


        public T? ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target, out bool messageConsumed) =>
            SourceSide.ConsumeMessage(messageHeader, target, out messageConsumed);

        public IDisposable LinkTo(ITargetBlock<T> target, DataflowLinkOptions linkOptions)
            => SourceSide.LinkToWithCustomCompletion(Completion, target, linkOptions);

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
            => SourceSide.ReleaseReservation(messageHeader, target);

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T> target)
            => SourceSide.ReserveMessage(messageHeader, target);

        public virtual bool TryReceive(Predicate<T>? filter, [MaybeNullWhen(false)] out T item)
        {
            if (SourceSide is IReceivableSourceBlock<T> receivableSource)
            {
                return receivableSource.TryReceive(filter, out item);
            }

            item = default;
            return false;
        }

        public virtual bool TryReceiveAll([NotNullWhen(true)] out IList<T>? items)
        {
            if (SourceSide is IReceivableSourceBlock<T> receivableSource)
            {
                return receivableSource.TryReceiveAll(out items);
            }

            items = default;
            return false;
        }
    }
}
