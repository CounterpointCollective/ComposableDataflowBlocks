using CounterpointCollective.Utilities;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public static class IAsyncEnumerableExtensions
    {
        public static BufferBlock<I> AsSourceBlock<I>(this IAsyncEnumerable<I> source) =>
            AsSourceBlock(source, true);

        public static BufferBlock<I> AsSourceBlock<I>(
            this IAsyncEnumerable<I> source,
            bool complete
        ) => AsSourceBlock(source, new DataflowBlockOptions(), complete);

        public static BufferBlock<I> AsSourceBlock<I>(
            this IAsyncEnumerable<I> source,
            DataflowBlockOptions options
        ) => AsSourceBlock(source, options, true);

        public static BufferBlock<I> AsSourceBlock<I>(
            this IAsyncEnumerable<I> source,
            DataflowBlockOptions options,
            bool complete
        )
        {
            var bufferBlock = new BufferBlock<I>(options);
            _ = source.FeedTo(bufferBlock, complete, options.CancellationToken);
            return bufferBlock;
        }

        public static Task FeedTo<T>(
            this IAsyncEnumerable<T> source,
            ITargetBlock<T> target,
            CancellationToken cancel = default
        ) => FeedTo(source, target, true, cancel);

        public static async Task FeedTo<T>(
            this IAsyncEnumerable<T> source,
            ITargetBlock<T> target,
            bool complete,
            CancellationToken cancel = default
        )
        {
            try
            {
                await foreach (var e in source.WithCancellation(cancel))
                {
                    var consumed = await target.SendAsync(e, cancel);
                    if (cancel.IsCancellationRequested)
                    {
                        break;
                    }

                    if (!consumed)
                    {
                        throw new ArgumentException("Target does not accept message");
                    }
                }
                if (complete)
                {
                    target.Complete();
                }
            }
            catch (Exception e)
            {
                target.Fault(e);
            }
        }
    }
}
