using CounterpointCollective.DataFlow.Encapsulation;
using CounterpointCollective.DataFlow.Notifying;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    /// <summary>
    /// To prevent deadlocks, Source will always accept messages.
    /// However, the block itself will only accept new messages if the combined count of
    /// Source and the joining side (using either the smallest or largest queue count,
    /// depending on the configured mode) is below the capacity limit.
    ///
    /// Be careful when you pass GuaranteedBroadcastBlockBoundedCapacityMode.LargestQueue in the options.
    /// This may trigger a deadlock in some cases, namely:
    ///   - Queue[1] is empty.
    ///   - Queue[2] has items
    ///   - The ParBlock is full; it will not enqueue more messages before some are consumed.
    ///   - worker 1 requires to consume messages before it will produce a next message, e.g. because it is batching.
    ///   - worker 2 cannot consume more messages at the moment because it's output side is full
    /// The JoiningTarget will not consume messages until all workers have output available, causing a deadlock.
    /// </summary>
    public class ParallelBlock<I, T, O> : AbstractEncapsulatedPropagatorBlock<I, O>
    {
        protected override ITargetBlock<I> TargetSide => _boundedPropagatorBlock;
        protected override ISourceBlock<O> SourceSide => _boundedPropagatorBlock;

        private readonly GuaranteedBroadcastBlock<I> _fanOutBlock;
        private readonly ITargetBlock<T> _fanInBlock;

        private int DegreeOfParallelism { get; }

        public int InputCount => _fanOutBlock.Count;

        public int OutputCount => _boundedPropagatorBlock.Count - InputCount;

        /// <summary>
        /// Items currently in the block, being either in the input side or output side.
        /// When they leave for the workers, they are no longer counted here.
        /// When they return from the workers, they are counted here again.
        /// This can cause the Count to be temporarily higher than the BoundedCapacity, just like
        /// in the TransformManyBlock.
        /// </summary>
        public int Count => _boundedPropagatorBlock.Count;

        private readonly BoundedPropagatorBlock<I,O> _boundedPropagatorBlock;

        private readonly List<IPropagatorBlock<I, T>> _workers = [];
        private int i; //Index of next worker to hookup

        public ParallelBlock(
            int degreeOfParallelism,
            Func<T[], O> recombine,
            GuaranteedBroadcastBlockOptions options
        )
        {
            if (degreeOfParallelism < 2)
            {
                throw new ArgumentException("degreeOfParallelism must be at least 2.", nameof(degreeOfParallelism));
            }
            DegreeOfParallelism = degreeOfParallelism;

            _fanOutBlock = new GuaranteedBroadcastBlock<I>(
                degreeOfParallelism,
                new()
                {
                    CancellationToken = options.CancellationToken,
                    BoundedCapacityMode = options.BoundedCapacityMode,
                },
                onQueuesShrunk : diff => MessagesFannedOut(-diff)
            );

            var j = new BatchBlock<T>(degreeOfParallelism, new()
            {
                CancellationToken = options.CancellationToken,
                Greedy = false
            })
            .BeginEncapsulation()
            .Transform(
                b => recombine([.. b]),
                new ExecutionDataflowBlockOptions
                {
                    SingleProducerConstrained = true,
                    CancellationToken = options.CancellationToken
                }
                )
            .Build();
            _fanInBlock = j;

            _boundedPropagatorBlock = new BoundedPropagatorBlock<I, O>(_fanOutBlock, j, options.BoundedCapacity);
        }

        private void MessagesFannedOut(int diff) => _boundedPropagatorBlock.AdjustCount(-diff);

        private void MessagesFannedIn(int diff) => _boundedPropagatorBlock.AdjustCount(diff);

        public void Hookup(IPropagatorBlock<I,T> worker, DataflowLinkOptions options)
        {
            if (i == DegreeOfParallelism)
            {
                throw new InvalidOperationException($"All {DegreeOfParallelism} workers have already been hooked up.");
            }
            _fanOutBlock[i].LinkTo(worker, options);

            worker
                = i == 0
                ? worker
                .BeginEncapsulation()
                .WithNotification(h =>
                {
                    h.OnDeliveringMessages = msgs => MessagesFannedIn(msgs.Count);
                    h.NotifySynchronously = true;
                })
                .Build()
                : worker;

            _workers.Add(worker);

            worker.LinkTo(_fanInBlock);
            worker.PropagateCompletion(TaskContinuationOptions.OnlyOnFaulted, _fanInBlock);
            i++;
            if (i == DegreeOfParallelism)
            {
                Task.WhenAll(_workers.Select(e => e.Completion))
                    .PropagateCompletion(_fanInBlock);
            }
        }

        /// <summary>
        /// Convenience method to link all the workers in one go.
        /// </summary>
        public void LinkWorkers(IPropagatorBlock<I, T>[] workers, DataflowLinkOptions options)
        {
            if (workers.Length != DegreeOfParallelism)
            {
                throw new ArgumentException($"Exactly {DegreeOfParallelism} workers are required.", nameof(workers));
            }

            for (var i = 0; i < workers.Length; i++)
            {
                Hookup(workers[i], options);
            }
        }

        /// <summary>
        /// Convenience method to link all the workers in one go.
        /// It will hookup the workers from the finished task eagerly, so that they can already start working while the rest is initializing.
        /// </summary>
        public async Task LinkWorkersAsync(Task<IPropagatorBlock<I, T>>[] tWorkers, DataflowLinkOptions options)
        {
            if (tWorkers.Length != DegreeOfParallelism)
            {
                throw new ArgumentException($"Exactly {DegreeOfParallelism} workers are required.", nameof(tWorkers));
            }

            var workers = new IPropagatorBlock<I, T>[tWorkers.Length];
            var pending = tWorkers;
            while (pending.Length > 0)
            {
                var t = await Task.WhenAny(pending);
                List<Task<IPropagatorBlock<I, T>>> stillPending = [];
                foreach (var task in pending)
                {
                    if (task.IsCompleted)
                    {
                        var w = await task;
                        Hookup(w, options);
                    }
                    else
                    {
                        stillPending.Add(task);
                    }
                }
                pending = [.. stillPending];
            }
        }
    }
}
