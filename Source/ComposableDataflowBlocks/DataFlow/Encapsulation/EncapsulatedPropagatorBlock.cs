using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Encapsulation
{
    public abstract class AbstractEncapsulatedPropagatorBlock<I, O>: AbstractEncapsulatedSourceBlock<O>, IPropagatorBlock<I, O>
    {
        protected abstract ITargetBlock<I> TargetSide { get; }

        protected override IDataflowBlock CompleteSide => TargetSide;

        public DataflowMessageStatus OfferMessage(
            DataflowMessageHeader messageHeader,
            I messageValue,
            ISourceBlock<I>? source,
            bool consumeToAccept
        )
        {
            if (Completion.IsCompleted)
            {
                return DataflowMessageStatus.DecliningPermanently;
            }
            else
            {
                return TargetSide.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
            }
        }
    }
}
