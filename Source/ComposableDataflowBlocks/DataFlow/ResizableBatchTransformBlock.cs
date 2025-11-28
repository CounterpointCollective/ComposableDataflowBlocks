using CounterpointCollective.DataFlow.Encapsulation;
using CounterpointCollective.Threading;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public sealed class ResizableBatchTransformBlock<I, O>
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
        : AbstractEncapsulatedPropagatorBlock<I, O>
    {
        protected override ITargetBlock<I> TargetSide => _boundedPropagatorBlock;

        protected override ISourceBlock<O> SourceSide => _boundedPropagatorBlock;

        private readonly SemaphoreSlim _semRunNextBatch;

        private readonly BoundedPropagatorBlock<I,O> _boundedPropagatorBlock;
        private readonly DynamicBufferBlock<I> _batchGatherBlock;
        private readonly TransformManyBlock<I[], O> _transformManyBlock;

        private DateTime earliestEntrance;

        public bool IsBottleneckCurrently => OutputCount == 0 && IsFull;

        public bool IsFull => _batchGatherBlock.Count >= BatchSize;

        public int BoundedCapacity
        {
            get => _boundedPropagatorBlock.BoundedCapacity;
            set => _boundedPropagatorBlock.BoundedCapacity = value;
        }


        public int BatchSize
        {
            get => _batchGatherBlock.BoundedCapacity;
            set
            {
                _batchGatherBlock.BoundedCapacity = value;
                OnBatchSizeChanged?.Invoke(value);
                RunBatchIfNeeded();
            }
        }

        public Func<ResizableBatchTransformBlock<I, O>, IList<I>, IDisposable>? OnBatch { get; set; }

        public int Count => _boundedPropagatorBlock.Count;

        private int inputCount;
        public int InputCount => Volatile.Read(ref inputCount);

        public int InProgressCount => Count - InputCount - OutputCount;

        public int OutputCount => _transformManyBlock.OutputCount;

        public TimeSpan LongestWait => InputCount == 0 ? TimeSpan.Zero : DateTime.UtcNow - earliestEntrance;

        private readonly BinarySemaphoreSlim _sem = new(false);

        public Action<int>? OnBatchSizeChanged
        {
            get;
            set
            {
                field = value;
                field?.Invoke(BatchSize);
            }
        }

        public ResizableBatchTransformBlock(
            Func<I[], Task<IEnumerable<O>>> transform,
            int initialBatchSize,
            ExecutionDataflowBlockOptions? options = null
        )
        {
            options ??= new();
            _batchGatherBlock = new(new() { CancellationToken = options.CancellationToken }, RunBatchIfNeeded);

            _semRunNextBatch = new SemaphoreSlim(options.MaxDegreeOfParallelism - 1);
            _transformManyBlock = new TransformManyBlock<I[], O>(
                async batch =>
                {
                    var d = OnBatch?.Invoke(this, batch);
                    IEnumerable<O> ret;
                    try
                    {
                        ret = await transform(batch);
                    }
                    finally
                    {
                        d?.Dispose();
                    }
                    _semRunNextBatch.Release();
                    return ret;
                },
                new() { CancellationToken = options.CancellationToken, MaxDegreeOfParallelism = options.MaxDegreeOfParallelism }
            );

            BatchSize = initialBatchSize;

            _boundedPropagatorBlock = new BoundedPropagatorBlock<I, O>(
                _batchGatherBlock,
                _transformManyBlock,
                options.BoundedCapacity,
                onEntered: () =>
                {
                    if (Interlocked.Increment(ref inputCount) == 1)
                    {
                        earliestEntrance = DateTime.UtcNow;
                    }
                }
            );

            Task.Run(async () =>
            {
                var runBatchesTask = Task.Run(() => RunBatchesAsync());
                var t = await Task.WhenAny(_boundedPropagatorBlock.InputCompletion);
                RunBatchIfNeeded();
                _sem.Terminate();
                await runBatchesTask;

                _ = t.PropagateCompletion(_transformManyBlock);
            });
        }


        private void RunBatchIfNeeded() => _sem.Release();

        private async Task RunBatchesAsync()
        {
            while (true)
            {
                var t = await Task.WhenAny(_sem.WaitAsync());
                if (!t.IsCompletedSuccessfully)
                {
                    break;
                }

                while (TryGetBatch(out var batch))
                {
                    Interlocked.Add(ref inputCount, -batch.Count);
                    _transformManyBlock.PostAsserted([.. batch]);
                    await _semRunNextBatch.WaitAsync();
                }
            }

            bool TryGetBatch([MaybeNullWhen(false)] out IList<I> batch)
            {
                if (CanRunBatch())
                {
                    //First try to get maximally BatchSize items synchronously.
                    //The buffer may be overfull though, in case we resized to a smaller BatchSize, so we need to check the actual count.
                    if (!(_batchGatherBlock.Count <= BatchSize && _batchGatherBlock.TryReceiveAll(out batch)))
                    {
                        //Fall back to slow mode.
                        batch = [];
                        while (batch.Count < BatchSize && _batchGatherBlock.TryReceive(out var item))
                        {
                            batch.Add(item);
                        }
                    }
                    return true;
                }
                batch = null;
                return false;
            }

            // Normal             -> buffer.Count >= BatchSize
            // Faulting/canceling -> false
            // Complete           -> buffer.Count > 0
            bool CanRunBatch()
            {
                var finishedUnsuccessful = Completion.IsFaulted || Completion.IsCanceled;
                return
                    !finishedUnsuccessful
                    &&
                    (
                        _batchGatherBlock.Count >= BatchSize
                        || (_boundedPropagatorBlock.InputCompletion.IsCompletedSuccessfully && _batchGatherBlock.Count > 0)
                    );
            }
        }
    }
}
