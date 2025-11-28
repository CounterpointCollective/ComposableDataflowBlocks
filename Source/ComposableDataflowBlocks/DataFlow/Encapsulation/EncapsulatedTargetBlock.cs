using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Encapsulation
{
    public sealed class EncapsulatedTargetBlock<T>(ITargetBlock<T> targetSide, IDataflowBlock completionSide)
        : AbstractEncapsulatedTargetBlock<T>, ITargetBlock<T>
    {
        protected override IDataflowBlock CompletionSide => completionSide;

        protected override ITargetBlock<T> TargetSide => targetSide;
    }

    public abstract class AbstractEncapsulatedTargetBlock<T> : AbstractEncapsulatedDataflowBlock, ITargetBlock<T>
    {
        protected abstract ITargetBlock<T> TargetSide { get; }
        protected override IDataflowBlock CompleteSide => TargetSide;


        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept)
            => TargetSide.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
    }
}
