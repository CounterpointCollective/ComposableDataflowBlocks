using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Encapsulation
{
    public static class EncapsulationExtensions
    {
        public static IReceivableSourceBlock<T> EncapsulateAsSourceBlock<T>(this IDataflowBlock b1, ISourceBlock<T> b2)
            => new EncapsulatedSourceBlock<T>(b1, b2);

        public static IPropagatorBlock<TInput, TOutput> Encapsulate<TInput, TOutput>(
            this ITargetBlock<TInput> b1,
            ISourceBlock<TOutput> b2
        )
            => DataflowBlock.Encapsulate(b1, b2);

        public static ITargetBlock<T> EncapsulateAsTargetBlock<T>(this ITargetBlock<T> targetSide, IDataflowBlock b2)
            => new EncapsulatedTargetBlock<T>(targetSide, b2);

        public static IDataflowBlock EncapsulateAsDataflowBlock(this IDataflowBlock b1, IDataflowBlock b2)
            => new EncapsulatedDataflowBlock(b1, b2);
    }
}
