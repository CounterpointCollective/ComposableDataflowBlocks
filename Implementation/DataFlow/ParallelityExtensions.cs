using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CounterpointCollective.DataFlow.Encapsulation;
using CounterpointCollective.Utilities;

namespace CounterpointCollective.DataFlow
{

    public static class ParallelityExtensions
    {
        public static IPropagatorBlock<I, O> Par<I, T1, T2, O>(
            this IPropagatorBlock<I, T1> p1,
            IPropagatorBlock<I, T2> p2,
            Func<T1, T2, O> f,
            GuaranteedBroadcastBlockOptions options
        )
        {
            var ret = new ParBlock<I, Either<T1, T2>, O>(2, Recombine, options);

            O Recombine(Either<T1, T2>[] inputs)
            {
                var idx = inputs[0].IsLeft ? 0 : 1;
                var l = inputs[idx].FromLeft;
                var r = inputs[(idx + 1) % 2].FromRight;
                return f(l, r);
            }

            var pt1 =
                p1.BeginEncapsulation()
                .SynchronousTransform(e => Either<T1, T2>.Left(e))
                .Build();

            var pt2 =
                p2.BeginEncapsulation()
                .SynchronousTransform(e => Either<T1, T2>.Right(e))
                .Build();

            ret.Hookup(pt1, new() { PropagateCompletion = true });
            ret.Hookup(pt2, new() { PropagateCompletion = true });

            return ret;
        }

        public static IPropagatorBlock<I, I> Par<I>(this IPropagatorBlock<I, I>[] blocks) =>
            blocks.Par(new());

        public static IPropagatorBlock<I, I> Par<I>(
            this IEnumerable<IPropagatorBlock<I, I>> blocks,
            GuaranteedBroadcastBlockOptions options
        ) => CreateParBuilder<I>()
            .WithOptions(options)
            .AddBlocks(blocks)
            .Par();

        public static ParBlock<I, T, O> Par<I, T, O>(
            this IPropagatorBlock<I, T>[] blocks,
            Func<T[], O> recombine,
            GuaranteedBroadcastBlockOptions options
        )
        {
            var res = new ParBlock<I, T, O>(
                blocks.Length,
                recombine,
                options
            );
            res.LinkWorkers(blocks, new() { PropagateCompletion = true });
            return res;
        }

        public static Task<IPropagatorBlock<I, I>> ParAsync<I>(
            this IEnumerable<Func<CancellationToken, Task<IPropagatorBlock<I, I>>>> blockFactories,
            GuaranteedBroadcastBlockOptions options,
            CancellationToken cancellationToken
        ) => CreateAsyncParBuilder<I>()
            .WithOptions(options)
            .AddBlockFactories(blockFactories)
            .ParAsync(cancellationToken);

        public static ParBlock<I, T, O> ParAsync<I, T, O>(
            this Func<CancellationToken, Task<IPropagatorBlock<I, T>>>[] blockFactories,
            Func<T[], O> recombine,
            GuaranteedBroadcastBlockOptions options,
            CancellationToken cancellationToken
        )
        {
            var res = new ParBlock<I, T, O>(
                blockFactories.Length,
                recombine,
                options
            );

            var blocks = blockFactories
                .Select(f => f(cancellationToken))
                .ToArray();

            _ = Task.Run(async () =>
            {
                try
                {
                    await res.LinkWorkersAsync(blocks, new() { PropagateCompletion = true });
                }
                catch (Exception e)
                {
                    res.Fault(e);
                }
            }, cancellationToken);
            return res;
        }


        private static I RecombineByVerifyingLockstepOutput<I>(I[] outputs)
        {
            var first = outputs[0];
            for (var i = 1; i < outputs.Length; i++)
            {
                if (!EqualityComparer<I>.Default.Equals(first, outputs[i]))
                {
                    throw new ArgumentException(
                        "Not all outputs were the same. The order of the message probably changed."
                    );
                }
            }
            return first;
        }

        public static AsyncParBuilder<I, I, I> CreateAsyncParBuilder<I>() => new(RecombineByVerifyingLockstepOutput);

        public class AsyncParBuilder<I,T,O>(Func<T[], O> recombine)
        {
            private readonly List<Func<CancellationToken, Task<IPropagatorBlock<I, T>>>> _blockFactories = [];

            private GuaranteedBroadcastBlockOptions Options { get; set;  } = new();
            public IReadOnlyList<Func<CancellationToken, Task<IPropagatorBlock<I, T>>>> BlockFactories => _blockFactories.AsReadOnly();

            public AsyncParBuilder<I, T, O> WithOptions(GuaranteedBroadcastBlockOptions options)
            {
                Options = options;
                return this;
            }

            public AsyncParBuilder<I,T,O> AddBlockFactory<TB>(Func<CancellationToken, Task<TB>> blockFactory) where TB : IPropagatorBlock<I, T>
            {
                _blockFactories.Add(cancel => blockFactory(cancel).ContinueWith(t => (IPropagatorBlock<I, T>)t.Result));
                return this;
            }

            public AsyncParBuilder<I, T, O> AddBlockFactories<TB>(IEnumerable<Func<CancellationToken, Task<TB>>> e) where TB : IPropagatorBlock<I, T>
            {
                foreach(var b in e)
                {
                    AddBlockFactory(b);
                }
                return this;
            }

            public AsyncParBuilder<I, T, O> AddBlock<TB>(TB block) where TB : IPropagatorBlock<I, T>
            {
                _blockFactories.Add(cancel => Task.FromResult((IPropagatorBlock<I, T>)block));
                return this;
            }

            public AsyncParBuilder<I, T, O> AddBlocks<TB>(IEnumerable<TB> e) where TB : IPropagatorBlock<I, T>
            {
                foreach (var b in e)
                {
                    AddBlock(b);
                }
                return this;
            }

            public ParBlock<I, T, O> Build(CancellationToken cancellationToken = default)
                => _blockFactories.ToArray().ParAsync<I,T,O>(recombine, Options, cancellationToken);
        }

        public static async Task<IPropagatorBlock<I,I>> ParAsync<I>(this AsyncParBuilder<I,I,I> b, CancellationToken cancellationToken)
        {
            if (b.BlockFactories.Count == 0)
            {
                throw new ArgumentException("Pass at least one block.");
            } else if (b.BlockFactories.Count == 1)
            {
                return await b.BlockFactories[0](cancellationToken);
            }
            else
            {
                return b.Build(cancellationToken);
            }
        }

        public static IPropagatorBlock<I, I> Par<I>(this ParBuilder<I, I, I> b)
        {
            if (b.Blocks.Count == 0)
            {
                throw new ArgumentException("Pass at least one block.");
            }
            else if (b.Blocks.Count == 1)
            {
                return b.Blocks[0];
            }
            else
            {
                return b.Build();
            }
        }

        public static ParBuilder<I, I, I> CreateParBuilder<I>() => new(RecombineByVerifyingLockstepOutput);

        public class ParBuilder<I, T, O>(Func<T[], O> recombine)
        {
            private readonly List<IPropagatorBlock<I, T>> _blocks = [];

            private GuaranteedBroadcastBlockOptions Options { get; set; } = new();
            public IReadOnlyList<IPropagatorBlock<I, T>> Blocks => _blocks.AsReadOnly();

            public ParBuilder<I, T, O> WithOptions(GuaranteedBroadcastBlockOptions options)
            {
                Options = options;
                return this;
            }

            public ParBuilder<I, T, O> AddBlock<TB>(TB block) where TB : IPropagatorBlock<I, T>
            {
                _blocks.Add((IPropagatorBlock<I, T>)block);
                return this;
            }

            public ParBuilder<I, T, O> AddBlocks<TB>(IEnumerable<TB> e) where TB : IPropagatorBlock<I, T>
            {
                foreach (var b in e)
                {
                    AddBlock(b);
                }
                return this;
            }

            public ParBlock<I, T, O> Build()
                => _blocks.ToArray().Par<I, T, O>(recombine, Options);
        }
    }
}
