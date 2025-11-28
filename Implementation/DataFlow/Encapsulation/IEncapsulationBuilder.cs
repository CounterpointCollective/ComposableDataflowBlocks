using CounterpointCollective.DataFlow.Notifying;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Encapsulation
{
    public interface IEncapsulationBuilder<out TFirst, out TLast>
        where TFirst : IDataflowBlock
        where TLast : IDataflowBlock
    {
        public TFirst First { get; }
        public TLast Last { get; }
    }

    public static class EncapsulationBuilderExtensions
    {
        /// <summary>
        /// Starts an encapsulation pipeline from the given dataflow block.
        /// </summary>
        /// <typeparam name="B">The type of the starting dataflow block.</typeparam>
        /// <param name="block">The dataflow block to start the encapsulation from.</param>
        /// <returns>
        /// A new <see cref="IEncapsulationBuilder{B, B}"/> representing the encapsulation pipeline 
        /// starting and ending with <paramref name="block"/>. This allows additional blocks to be appended
        /// fluently to build a larger pipeline, e.g. using methods like <c>Transform</c> or <c>Tee</c>.
        /// </returns>
        public static
            IEncapsulationBuilder<B, B>
            BeginEncapsulation<B>(this B block) where B : IDataflowBlock
            => new EncapsulationBuilder<B, B>(block, block);

        #region builders
        /// <summary>
        /// Builds the encapsulated pipeline as a single <see cref="IPropagatorBlock{TInput,TOutput}"/> 
        /// </summary>
        /// <returns>A block that encapsulates the entire pipeline.</returns>
        public static IPropagatorBlock<TInput, TOutput> Build<TInput, TOutput>(
            this IEncapsulationBuilder<ITargetBlock<TInput>, ISourceBlock<TOutput>> b
        )
        {
            if (b.Last == b.First && b.First is IPropagatorBlock<TInput, TOutput> p)
            {
                return p;
            }
            else
            {
                p = DataflowBlock.Encapsulate(b.First, b.Last);
                return p;
            }
        }

        /// <summary>
        /// Builds the encapsulated pipeline as a single <see cref="ISourceBlock{TOutput}"/> 
        /// </summary>
        /// <returns>A block that encapsulates the entire pipeline.</returns>
        public static ISourceBlock<TOutput> BuildSourceBlock<TOutput>(
            this IEncapsulationBuilder<IDataflowBlock, ISourceBlock<TOutput>> b
        )
        {
            if (b.Last == b.First)
            {
                return b.Last;
            }
            else
            {
                var p = b.First.EncapsulateAsSourceBlock(b.Last);
                return p;
            }
        }

        /// <summary>
        /// Builds the encapsulated pipeline as a single <see cref="ITargetBlock{TInput}"/> 
        /// </summary>
        /// <returns>A block that encapsulates the entire pipeline.</returns>
        public static ITargetBlock<TInput> BuildTargetBlock<TInput>(
            this IEncapsulationBuilder<ITargetBlock<TInput>, IDataflowBlock> b
        )
        {
            if (b.Last == b.First)
            {
                return b.First;
            }
            else
            {
                var p = b.First.EncapsulateAsTargetBlock(b.Last);
                return p;
            }
        }

        /// <summary>
        /// Builds the encapsulated pipeline as a single <see cref="IDataflowBlock"/> 
        /// </summary>
        /// <returns>A block that encapsulates the entire pipeline.</returns>
        public static IDataflowBlock BuildDataflowBlock<TInput>(
            this IEncapsulationBuilder<IDataflowBlock, IDataflowBlock> b
        )
        {
            if (b.Last == b.First)
            {
                return b.First;
            }
            else
            {
                var p = b.First.EncapsulateAsDataflowBlock(b.Last);
                return p;
            }
        }
        #endregion builders

        #region selectors
        public static IEncapsulationBuilder<TNewFirst, TNewLast> Select<TFirst, TLast, TNewFirst, TNewLast>
            (
            this IEncapsulationBuilder<TFirst, TLast> eb,
            Func<
                IEncapsulationBuilder<TFirst, TLast>,
                IEncapsulationBuilder<TNewFirst, TNewLast>
                > f)
        where TFirst : IDataflowBlock
        where TLast : IDataflowBlock
        where TNewFirst : IDataflowBlock
        where TNewLast : IDataflowBlock
            => f(eb);

        public static IEncapsulationBuilder<TFirst, TNew> SelectLast<TFirst, TLast, TNew>(
            this IEncapsulationBuilder<TFirst, TLast> eb, Func<TLast, TNew> f)
        where TFirst : IDataflowBlock
        where TLast : IDataflowBlock
        where TNew : IDataflowBlock
            => new EncapsulationBuilder<TFirst, TNew>(eb.First, f(eb.Last));

        public static IEncapsulationBuilder<TNew, TLast> SelectFirst<TFirst, TLast, TNew>(
            this IEncapsulationBuilder<TFirst, TLast> eb, Func<TFirst, TNew> f)
        where TFirst : IDataflowBlock
        where TLast : IDataflowBlock
        where TNew : IDataflowBlock
            => new EncapsulationBuilder<TNew, TLast>(f(eb.First), eb.Last);
        #endregion selectors

        public static IEncapsulationBuilder<TFirst, TNewLast> LinkTo<TFirst, TNewLast, T>
            (this IEncapsulationBuilder<TFirst, ISourceBlock<T>> eb, TNewLast nl, DataflowLinkOptions options)
            where TNewLast : ITargetBlock<T>
            where TFirst: IDataflowBlock
        =>
            eb.SelectLast(e =>
            {
                e.LinkTo(nl, options);
                return nl;
            });

        #region convenience methods
        /// <summary>
        /// Links a BufferBlock to the end of the current encapsulation builder.
        /// </summary>
        public static IEncapsulationBuilder<TFirst, BufferBlock<T>> Buffer<TFirst, T>
        (
            this IEncapsulationBuilder<TFirst, ISourceBlock<T>> eb,
            DataflowBlockOptions? options = null
        ) where TFirst : IDataflowBlock
        => eb.LinkTo(
            new BufferBlock<T>(options ?? new()), new DataflowLinkOptions() { PropagateCompletion = true }
        );

        /// <summary>
        /// Links a TransformBlock to the end of the current encapsulation builder.
        /// </summary>
        public static IEncapsulationBuilder<TFirst, TransformBlock<I, O>> Transform<TFirst, I, O>
        (
            this IEncapsulationBuilder<TFirst, ISourceBlock<I>> eb,
            Func<I, O> f,
            ExecutionDataflowBlockOptions? options = null
        ) where TFirst : IDataflowBlock
        => eb.LinkTo(
            new TransformBlock<I,O>(f, options ?? new()), new DataflowLinkOptions() { PropagateCompletion = true }
        );

        /// <summary>
        /// Links a TransformManyBlock to the end of the current encapsulation builder.
        /// </summary>
        public static IEncapsulationBuilder<TFirst, TransformManyBlock<I, O>> TransformMany<TFirst, I, O>
        (
            this IEncapsulationBuilder<TFirst, ISourceBlock<I>> eb,
            Func<I, IEnumerable<O>> f,
            ExecutionDataflowBlockOptions? options = null
        ) where TFirst : IDataflowBlock
        => eb.LinkTo(
            new TransformManyBlock<I, O>(f, options ?? new()), new DataflowLinkOptions() { PropagateCompletion = true }
        );

        /// <summary>
        /// Links a TransformManyBlock to the end of the current encapsulation builder.
        /// </summary>
        public static IEncapsulationBuilder<TFirst, TransformManyBlock<I, O>> TransformMany<TFirst, I, O>
        (
            this IEncapsulationBuilder<TFirst, ISourceBlock<I>> eb,
            Func<I, Task<IEnumerable<O>>> f,
            ExecutionDataflowBlockOptions? options = null
        ) where TFirst : IDataflowBlock
        => eb.LinkTo(
            new TransformManyBlock<I, O>(f, options ?? new()), new DataflowLinkOptions() { PropagateCompletion = true }
        );

        /// <summary>
        /// Links a TransformManyBlock to the end of the current encapsulation builder.
        /// </summary>
        public static IEncapsulationBuilder<TFirst, TransformManyBlock<I, O>> TransformMany<TFirst, I, O>
        (
            this IEncapsulationBuilder<TFirst, ISourceBlock<I>> eb,
            Func<I, IAsyncEnumerable<O>> f,
            ExecutionDataflowBlockOptions? options = null
        ) where TFirst : IDataflowBlock
        => eb.LinkTo(
            new TransformManyBlock<I, O>(f, options ?? new()), new DataflowLinkOptions() { PropagateCompletion = true }
        );

        /// <summary>
        /// Links a BatchBlock to the end of the current encapsulation builder.
        /// </summary>
        public static IEncapsulationBuilder<TFirst, BatchBlock<T>> Batch<TFirst, T>
        (
            this IEncapsulationBuilder<TFirst, ISourceBlock<T>> eb,
            int batchSize,
            GroupingDataflowBlockOptions? options = null
        ) where TFirst : IDataflowBlock
        => eb.LinkTo(
            new BatchBlock<T>(batchSize, options ?? new()), new DataflowLinkOptions() { PropagateCompletion = true }
        );


        /// <summary>
        /// Links an ActionBlock to the end of the current encapsulation builder.
        /// </summary>
        public static IEncapsulationBuilder<TFirst, ActionBlock<T>> Action<TFirst, T>
        (
            this IEncapsulationBuilder<TFirst, ISourceBlock<T>> eb,
            Action<T> a,
            ExecutionDataflowBlockOptions? options = null
        ) where TFirst : IDataflowBlock
        => eb.LinkTo(
            new ActionBlock<T>(a, options ?? new()), new DataflowLinkOptions() { PropagateCompletion = true }
        );


        public static IEncapsulationBuilder<TFirst, StreamingGroupByBlock<T, K, V>>
        StreamingGroupBy<TFirst, T, K, V>
            (this IEncapsulationBuilder<TFirst, ISourceBlock<T>> eb,
            Func<T, K> keySelector,
            Func<T, V> valueSelector,
            DataflowBlockOptions options,
            bool flushOnIdle = false
        )
            where TFirst : IDataflowBlock
        => eb.LinkTo(new StreamingGroupByBlock<T, K, V>(keySelector, valueSelector, options, flushOnIdle), new DataflowLinkOptions() { PropagateCompletion = true });

        /// <summary>
        /// Wraps the end of the current encapsulation builder in a SynchronousFilterBlock.
        /// </summary>
        public static IEncapsulationBuilder<TFirst, SynchronousFilterBlock<T>> SynchronousFilter<TFirst, T>
            (
                this IEncapsulationBuilder<TFirst, ISourceBlock<T>> eb,
                Predicate<T> predicate,
                FilterMode mode = FilterMode.Block
            )
            where TFirst : IDataflowBlock
        =>
            eb.SelectLast(e => new SynchronousFilterBlock<T>(e, predicate, mode));


        /// <summary>
        /// Wraps the end of the current encapsulation builder in a SynchronousTransformingBlock.
        /// </summary>
        public static IEncapsulationBuilder<TFirst, SynchronousTransformingBlock<T, O>> SynchronousTransform<TFirst, T, O>
            (
                this IEncapsulationBuilder<TFirst, ISourceBlock<T>> eb,
                Func<T, O> transform
            )
            where TFirst : IDataflowBlock
        =>
            eb.SelectLast(e => new SynchronousTransformingBlock<T, O>(e, transform));


        /// <summary>
        /// Wraps the end of the current encapsulation builder in a SourceBlockWithDeliveryNotification.
        /// </summary>
        public static IEncapsulationBuilder<TFirst, SourceBlockWithNotification<T>>
            WithNotification<TFirst, T>
            (this IEncapsulationBuilder<TFirst, ISourceBlock<T>> eb,
            ConfigureHooks<T> hooks)
            where TFirst : IDataflowBlock
        => eb.SelectLast(l => l.WithNotification(hooks));

        #endregion convenience methods

        /// <summary>
        /// Creates a “tee” branch of a dataflow pipeline that allows you to recombine the original input
        /// with transformed output using a custom combinator.
        /// <code>
        /// var pipeline = Enumerable.Range(1, 100).ToAsyncEnumerable().AsSourceBlock()
        ///     .BeginEncapsulation()
        ///     .Tee(tb =>
        ///         tb
        ///         .ConfigureBranch(e => e.Transform(i => i * 2))
        ///         .CombineWith((original, transformed) => new { Original = original, Doubled = transformed })
        ///     )
        ///     .BuildSourceBlock();
        ///
        /// var results = await pipeline.AsAsyncEnumerable().ToListAsync();
        /// </code>
        /// In this example, each input `i` is sent through a branch that multiplies it by 2, and the main flow
        /// recombines it with the original value using the combinator.
        /// </summary>
        /// <returns>
        /// A new IEncapsulationBuilder representing the pipeline with the tee applied.
        /// </returns>
        public static IEncapsulationBuilder<TFirst, ISourceBlock<O>>
            Tee<TFirst, I, T, O>
            (this IEncapsulationBuilder<TFirst, ISourceBlock<I>> eb,
            Func<ITeeBuilder<TFirst, ISourceBlock<I>, I, I, (I, I)>, ITeeBuilder<TFirst, ISourceBlock<I>, I, T, O>> f,
            DataflowBlockOptions? options = null
        )
            where TFirst : IDataflowBlock
        {
            var teeBuilder = eb.BeginTee();
            var teeBuilder2 = f(teeBuilder);
            var ret = teeBuilder2.BuildTee(options);
            return ret;
        }

        private sealed class EncapsulationBuilder<TFirst, TLast>(TFirst first, TLast last) : IEncapsulationBuilder<TFirst, TLast>
        where TFirst : IDataflowBlock
        where TLast : IDataflowBlock
        {
            public TFirst First { get; } = first;
            public TLast Last { get; } = last;
        }
    }
}
