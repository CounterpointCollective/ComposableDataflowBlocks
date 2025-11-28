using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public static class ISourceBlockExtensions
    {
        public static SynchronousFilterBlock<T> Filter<T>(
            this ISourceBlock<T> source,
            Predicate<T> pred,
            FilterMode mode = FilterMode.Block
        )
        => new(source, pred, mode);

        public static SynchronousFilterBlock<T> Distinct<T>(this ISourceBlock<T> source)
        {
            HashSet<T> seen = [];
            return source.Filter(i => seen.Add(i));
        }

        public static ISourceBlock<O> Transform<I, O>(this ISourceBlock<I> source, Func<I, O> f) =>
            Transform(source, f, new());

        public static ISourceBlock<O> Transform<I, O>(
            this ISourceBlock<I> source,
            Func<I, Task<O>> f
        ) => Transform(source, f, new());

        public static TransformBlock<I,O> Transform<I, O>(
            this ISourceBlock<I> source,
            Func<I, O> f,
            ExecutionDataflowBlockOptions opts
        )
        {
            var t = new TransformBlock<I, O>(f, opts);
            _ = source.LinkTo(t, new DataflowLinkOptions() { PropagateCompletion = true });
            return t;
        }

        public static ISourceBlock<O> Transform<I, O>(
            this ISourceBlock<I> source,
            Func<I, Task<O>> f,
            ExecutionDataflowBlockOptions opts
        )
        {
            var t = new TransformBlock<I, O>(f, opts);
            _ = source.LinkTo(t, new DataflowLinkOptions() { PropagateCompletion = true });
            return t;
        }

        public static  ISourceBlock<T> Flatten<T> (
            this ISourceBlock<ISourceBlock<T>> sources,
            ExecutionDataflowBlockOptions? options = null
        )
        {
            options ??= new();
            if (options.EnsureOrdered)
            {
                options.MaxDegreeOfParallelism = 1;
            }

            BufferBlock<T> b = new(options);
            var a = sources.Action(async s => {
                using var _ = s.LinkTo(b);
                await s.Completion;
            }, new ExecutionDataflowBlockOptions
            {
                EnsureOrdered = options.EnsureOrdered,
                CancellationToken = options.CancellationToken,
                SingleProducerConstrained = true,
                BoundedCapacity = options.BoundedCapacity,
                MaxDegreeOfParallelism = options.EnsureOrdered ? 1 : options.MaxDegreeOfParallelism
            });

            _ = a.PropagateCompletion(b);

            return b;
        }

        [Obsolete("Use Transform and Flatten instead")]
        public static ISourceBlock<O> TransformMany<I, O>(
            this ISourceBlock<I> source,
            Func<I, Task<ISourceBlock<O>>> f,
            ExecutionDataflowBlockOptions optsForProcessing,
            DataflowBlockOptions optsForBuffer
        )
        {
            if (optsForProcessing.EnsureOrdered)
            {
                optsForProcessing.MaxDegreeOfParallelism = 1;
            }
            BufferBlock<O> b = new(optsForBuffer);
            var a = source
                .Action(async msg =>
                {
                var q = await f(msg);
                _ = q.LinkTo(b);
                await q.Completion;
                },
                optsForProcessing
            );

            _ = a.PropagateCompletion(b);

            return b;
        }

        public static TransformManyBlock<I,O> TransformMany<I, O>(
            this ISourceBlock<I> source,
            Func<I, Task<IEnumerable<O>>> f,
            ExecutionDataflowBlockOptions dataflowBlockOptions
        )
        {
            TransformManyBlock<I, O> res = new(f, dataflowBlockOptions);
            _ = source.LinkTo(res, new DataflowLinkOptions { PropagateCompletion = true });
            return res;
        }

        public static ISourceBlock<O> TransformMany<I, O>(
            this ISourceBlock<I> source,
            Func<I, IEnumerable<O>> f,
            ExecutionDataflowBlockOptions dataflowBlockOptions
        )
        {
            TransformManyBlock<I, O> res = new(f, dataflowBlockOptions);
            _ = source.LinkTo(res, new DataflowLinkOptions { PropagateCompletion = true });
            return res;
        }

        public static TransformManyBlock<I, O> TransformMany<I, O>(
            this ISourceBlock<I> source,
            Func<I, IAsyncEnumerable<O>> f,
            ExecutionDataflowBlockOptions dataflowBlockOptions
        )
        {
            TransformManyBlock<I, O> res = new(f, dataflowBlockOptions);
            _ = source.LinkTo(res, new DataflowLinkOptions { PropagateCompletion = true });
            return res;
        }

        public static ISourceBlock<IGrouping<K, V>> GroupAdjacent<T, K, V>(
            this ISourceBlock<T> source,
            Func<T, K> keySelector,
            Func<T, V> valueSelector,
            DataflowBlockOptions options
        )
        {
            var res = new GroupAdjacentBlock<T, K, V>(keySelector, valueSelector, options);
            _ = source.LinkTo(res, new DataflowLinkOptions { PropagateCompletion = true });
            return res;
        }

        public static BatchBlock<T> Batch<T>(this ISourceBlock<T> source, int batchSize) =>
            Batch(source, batchSize, new());

        public static BatchBlock<T> Batch<T>(
            this ISourceBlock<T> source,
            int batchSize,
            GroupingDataflowBlockOptions opts
        )
        {
            var res = new BatchBlock<T>(batchSize, opts);
            _ = source.LinkTo(res, new DataflowLinkOptions { PropagateCompletion = true });
            return res;
        }

        public static BufferBlock<T> Buffer<T>(this ISourceBlock<T> source) =>
            Buffer(source, new());

        public static BufferBlock<T> Buffer<T>(
            this ISourceBlock<T> source,
            DataflowBlockOptions opts
        )
        {
            var res = new BufferBlock<T>(opts);
            _ = source.LinkTo(res, new DataflowLinkOptions { PropagateCompletion = true });
            return res;
        }

        public static ActionBlock<T> Action<T>(this ISourceBlock<T> source, Action<T> action) =>
            Action(source, action, new());

        public static ActionBlock<T> Action<T>(
            this ISourceBlock<T> source,
            Action<T> action,
            ExecutionDataflowBlockOptions opts
        )
        {
            var res = new ActionBlock<T>(action, opts);
            _ = source.LinkTo(res, new DataflowLinkOptions { PropagateCompletion = true });
            return res;
        }

        public static ActionBlock<T> Action<T>(this ISourceBlock<T> source, Func<T, Task> action) =>
            Action(source, action, new());

        public static ActionBlock<T> Action<T>(
            this ISourceBlock<T> source,
            Func<T, Task> action,
            ExecutionDataflowBlockOptions opts
        )
        {
            var res = new ActionBlock<T>(action, opts);
            _ = source.LinkTo(res, new DataflowLinkOptions { PropagateCompletion = true });
            return res;
        }

        public static BroadcastBlock<T> BroadcastTo<T>(
            this ISourceBlock<T> source,
            Func<T, T>? clone,
            params ITargetBlock<T>[] targets
        ) => BroadcastTo(source, clone, new(), targets);

        public static BroadcastBlock<T> BroadcastTo<T>(
            this ISourceBlock<T> source,
            Func<T, T>? clone,
            DataflowBlockOptions opts,
            params ITargetBlock<T>[] targets
        )
        {
            var res = new BroadcastBlock<T>(clone, opts);
            foreach (var t in targets)
            {
                _ = res.LinkTo(t, new DataflowLinkOptions { PropagateCompletion = true });
            }
            _ = source.LinkTo(res, new DataflowLinkOptions { PropagateCompletion = true });
            return res;
        }

        public static GuaranteedBroadcastBlock<T> GuaranteedBroadcastTo<T>(
            this ISourceBlock<T> source,
            params ITargetBlock<T>[] targets)
        => GuaranteedBroadcastTo(source, new(), targets);

        public static GuaranteedBroadcastBlock<T> GuaranteedBroadcastTo<T>(
            this ISourceBlock<T> source,
            GuaranteedBroadcastBlockOptions opts,
            params ITargetBlock<T>[] targets)
        {
            var res = new GuaranteedBroadcastBlock<T>(targets.Length, opts);
            for (var i = 0; i < targets.Length; i++)
            {
                _ = res[i].LinkTo(targets[i], new DataflowLinkOptions { PropagateCompletion = true });
            }
            _ = source.LinkTo(res, new DataflowLinkOptions { PropagateCompletion = true });
            return res;
        }

        /// <summary>
        /// Will eagerly buffer <paramref name="source"/> items from the async enumerable.
        /// </summary>
        public static IAsyncEnumerable<T> WithBuffer<T>(this IAsyncEnumerable<T> source, int bufferSize = DataflowBlockOptions.Unbounded, CancellationToken cancellationToken = default)
        => source
                .AsSourceBlock(options: new() { BoundedCapacity = bufferSize, CancellationToken = cancellationToken })
                .AsAsyncEnumerable(cancellationToken: cancellationToken);

        public static async IAsyncEnumerable<T> Finally<T>(
            this IAsyncEnumerable<T> source,
            Action finallyAction)
        {
            try
            {
                await foreach (var item in source)
                {
                    yield return item;
                }
            }
            finally
            {
                finallyAction();
            }
        }

        public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(
            this ISourceBlock<T> source,
            bool throwFirstExceptionOnly = true,
            bool completeSource = true,
            CancellationToken cancellationToken = default
        )
        {
            var ret = source is IReceivableSourceBlock<T> r ? Fast(r) : Slow();

            if (completeSource)
            {
                ret = ret.Finally(() =>
                {
                    if (!source.Completion.IsCompleted)
                    {
                        source.Fault(new OperationCanceledException("Evaluation stopped prematurely"));
                    }
                });
            }

            return ret;

            async Task<TResult> UnwrapAggregateAsync<TResult>(Func<Task<TResult>> fn)
            {
                try
                {
                    return await fn();
                }
                catch (AggregateException ex) when (throwFirstExceptionOnly)
                {
                    var inner = ex.Flatten().InnerExceptions.First();
                    ExceptionDispatchInfo.Capture(inner).Throw();
                    throw; // unreachable
                }
            }

            Task<bool> OutputAvailableAsync() => UnwrapAggregateAsync(() => source.OutputAvailableAsync(cancellationToken));
            Task<T> ReceiveAsync() => UnwrapAggregateAsync(() => source.ReceiveAsync(cancellationToken));
            Task<int> CompletionAsync() => UnwrapAggregateAsync(async () => {
                await source.Completion;
                return 0;
            });

            async IAsyncEnumerable<T> Fast(IReceivableSourceBlock<T> source)
            {
                while (await OutputAvailableAsync())
                {
                    var fastPathFails = true;
                    while (source.TryReceiveAll(out var l))
                    {
                        fastPathFails = false;
                        foreach (var t in l)
                        {
                            yield return t;
                        }
                    }

                    if (fastPathFails)
                    {
                        //Fall back to slow path
                        await foreach (var i in Slow())
                        {
                            yield return i;
                        }
                        yield break;
                    }
                }
                await CompletionAsync();
            }

            async IAsyncEnumerable<T> Slow()
            {
                
                while (true)
                {
                    T t;
                    try
                    {
                        t = await ReceiveAsync();
                    } catch (Exception) when (source.Completion.IsCompleted)
                    {
                        break;
                    }
                    yield return t;
                }
                await CompletionAsync();
            }
        }
    }
}
