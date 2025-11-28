using System;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public static class LinkTransformedExtensions
    {
        /// <param name="transformer">
        /// Called synchronously, and potentially multiple times for the same message.
        /// Must be cheap, otherwise use a real TransformBlock.
        /// </param>
        public static IDisposable LinkToTransformed<I, O>(
            this ISourceBlock<I> source,
            ITargetBlock<O> target,
            DataflowLinkOptions options,
            Func<I, O> transformer
        )
        {
            var b = new SynchronousTransformingBlock<I, O>(source, transformer);
            return b.LinkTo(target, options);
        }
    }
}
