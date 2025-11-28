using System;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Encapsulation
{
    public interface ITeeBuilder<out TFirst, out TLast, in I, T, out O>
        where TFirst : IDataflowBlock
        where TLast : ISourceBlock<I>
    {
        public IEncapsulationBuilder<TFirst, TLast> EncapsulationBuilder { get; }

        public Func<I, T, O> Combinator { get; }

        public Func<
            IEncapsulationBuilder<IDataflowBlock,ISourceBlock<I>>,
            IEncapsulationBuilder<IDataflowBlock, ISourceBlock<T>>>
            FConfigureBranch { get; }
    }

    public static class TeeBuilderExtensions
    {
        public static ITeeBuilder<TFirst, ISourceBlock<I>, I, I, (I, I)>
            BeginTee<TFirst, I>
            (this IEncapsulationBuilder<TFirst, ISourceBlock<I>> eb)
            where TFirst: IDataflowBlock
            => new TeeBuilder<TFirst, ISourceBlock<I>, I, I, (I, I)>(
                eb,
                inner => inner,
                (i, t) => (i, i)
            );

        /// <summary>
        /// Configures the branch in the tee pipeline
        /// </summary>
        public static ITeeBuilder<TFirst, TLast, I, T2, (I,T2)>
            ConfigureBranch<TFirst, TLast, I, T, T2, O>(
                this ITeeBuilder<TFirst, TLast, I, T, O> tb,
                Func<
                    IEncapsulationBuilder<IDataflowBlock, ISourceBlock<I>>,
                    IEncapsulationBuilder<IDataflowBlock, ISourceBlock<T2>>>
                fConfigureBranch
            )
                where TFirst : IDataflowBlock
                where TLast : ISourceBlock<I> =>
                new TeeBuilder<TFirst, TLast, I, T2, (I,T2)>(
                    tb.EncapsulationBuilder, fConfigureBranch, (i,o) => (i,o));

        /// <summary>
        /// Configures the combinator in the tee pipeline
        /// </summary>
        public static ITeeBuilder<TFirst, TLast, I, T, TO2>
            CombineWith<TFirst, TLast, I, T, TO1, TO2>(
                this ITeeBuilder<TFirst, TLast, I, T, TO1> tb,
                Func<I, T, TO2> combinator
            )
                where TFirst : IDataflowBlock
                where TLast : ISourceBlock<I> =>
                new TeeBuilder<TFirst, TLast, I, T, TO2>(
                    tb.EncapsulationBuilder, tb.FConfigureBranch, combinator);

        public static IEncapsulationBuilder<TFirst, ISourceBlock<O>> BuildTee<TFirst, I, T, O>(
             this ITeeBuilder<TFirst, ISourceBlock<I>, I, T, O> tb,
             DataflowBlockOptions? options = null
         ) where TFirst : IDataflowBlock
        {
            options ??= new DataflowBlockOptions();
            return tb.EncapsulationBuilder.SelectLast(s =>
            {
                var q = new BufferBlock<I>();

                var inner = 
                    new TransformBlock<I,I>(h => {
                        q.PostAsserted(h);
                        return h;
                    }, new() { CancellationToken = options.CancellationToken, SingleProducerConstrained = true })
                    .BeginEncapsulation()
                    .SelectLast(t => tb.FConfigureBranch(t.BeginEncapsulation()).BuildSourceBlock())
                    .Transform(
                        t =>
                        {
                            var i = q.Receive();
                            return tb.Combinator(i, t);
                        },
                        new ExecutionDataflowBlockOptions()
                        {
                            CancellationToken = options.CancellationToken,
                            SingleProducerConstrained = true
                        }
                    )
                    .Build();

                var ret = s.BeginEncapsulation().LinkTo(inner, new() { PropagateCompletion = true }).BuildSourceBlock();
                ret.PropagateCompletion(q);

                return ret;
            });
        }

        private class TeeBuilder<TFirst, TLast, I, T, O>
        (IEncapsulationBuilder<TFirst, TLast> eb,
            Func<
                IEncapsulationBuilder<IDataflowBlock, ISourceBlock<I>>,
                IEncapsulationBuilder<IDataflowBlock, ISourceBlock<T>>>
            fConfigureBranch,
            Func<I, T, O> combinator
        ) : ITeeBuilder<TFirst, TLast, I, T, O>
        where TFirst : IDataflowBlock
        where TLast : ISourceBlock<I>
        {
            public IEncapsulationBuilder<TFirst, TLast> EncapsulationBuilder => eb;

            public Func<I, T, O> Combinator => combinator;


            public Func<
                IEncapsulationBuilder<IDataflowBlock, ISourceBlock<I>>,
                IEncapsulationBuilder<IDataflowBlock, ISourceBlock<T>>>
                FConfigureBranch => fConfigureBranch;
        }
    }
}
