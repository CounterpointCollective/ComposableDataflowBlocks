using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Notifying
{
    public class PropagatingBlockWithNotification<I, O> : SourceBlockWithNotification<O>, IPropagatorBlock<I,O>
    {
        private readonly ITargetBlock<I> _targetBlock;

        public PropagatingBlockWithNotification(IPropagatorBlock<I, O> inner, ConfigureHooks<O> hooks) : base(inner, hooks)
            => _targetBlock = inner;

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, I messageValue, ISourceBlock<I>? source, bool consumeToAccept)
            => _targetBlock.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
    }
}
