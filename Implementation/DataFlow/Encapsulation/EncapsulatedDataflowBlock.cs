using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Encapsulation
{
    public sealed class EncapsulatedDataflowBlock(IDataflowBlock completeSide, IDataflowBlock completionSide) : AbstractEncapsulatedDataflowBlock
    {
        protected override IDataflowBlock CompleteSide => completeSide;

        protected override IDataflowBlock CompletionSide => completionSide;
    }

    public abstract class AbstractEncapsulatedDataflowBlock : IDataflowBlock
    {
        protected abstract IDataflowBlock CompleteSide { get; }
        protected abstract IDataflowBlock CompletionSide { get; }
        public virtual Task Completion => CompletionSide.Completion;
        public virtual void Complete() => CompleteSide.Complete();
        public virtual void Fault(Exception exception) => CompleteSide.Fault(exception);
    }
}
