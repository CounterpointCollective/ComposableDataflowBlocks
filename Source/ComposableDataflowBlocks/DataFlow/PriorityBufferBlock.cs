using CounterpointCollective.DataFlow.Encapsulation;
using CounterpointCollective.DataFlow.Notifying;
using CounterpointCollective.Threading;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public class PriorityBufferBlock<T, TPriority>
        : AbstractEncapsulatedPropagatorBlock<(T Message, TPriority Priority), (T Message, TPriority Priority)>
    {
        private readonly BoundedPropagatorBlock<(T Message, TPriority Priority),(T Message, TPriority Priority)> _boundedState;
        private readonly BinarySemaphoreSlim _holdElectionsSemaphore = new(false);
        private readonly AsyncManualResetEvent _electionsHeld = new();

        private TPriority worstPrioInElected = default!;
        internal bool ElectionBufferContainsOutOfOrderItem { get; private set; }
        private int electedBufferCount;
        internal int ElectedBufferCount => Volatile.Read(ref electedBufferCount);

        protected override ITargetBlock<(T Message, TPriority Priority)> TargetSide => _boundedState;

        protected override ISourceBlock<(T Message, TPriority Priority)> SourceSide => _boundedState;

        public Task AwaitElectionsHeldAsync() => _electionsHeld.WaitAsync();

        public int Count => _boundedState.Count;

        public PriorityBufferBlock() : this(new())
        { }

        /// <param name="electedBufferSize">
        /// The number of items to eagerly buffer for immediate consumption.
        /// 
        /// A larger buffer can improve throughput when consuming messages, because more items are ready to be delivered without waiting for additional elections.
        /// However, a larger buffer also increases the likelihood of re-elections if new messages with higher priority arrive out-of-order.
        /// Choose a value that balances consumption throughput against priority accuracy for your scenario.
        /// </param>
        public PriorityBufferBlock(DataflowBlockOptions options, int electedBufferSize = 1)
            : this(Comparer<TPriority>.Default, options, electedBufferSize)
        { }

        public PriorityBufferBlock(IComparer<TPriority> comparer, DataflowBlockOptions options, int electedBufferSize = 1)
        {
            void TriggerElection()
            {
                _electionsHeld.Reset();
                _holdElectionsSemaphore.Release();
            }

            var inputQueue = new BufferBlock<(T Message, TPriority Priority)>();

            var queuedItems = new PriorityQueue<T, TPriority>(comparer);
            var electionsCanTerminateGracefullyEvent = new AsyncManualResetEvent();
            electionsCanTerminateGracefullyEvent.Set();

            electedBufferCount = 0;
            var electedBuffer =
                new BufferBlock<(T Message, TPriority Priority)>(
                    new()
                    {
                        BoundedCapacity = electedBufferSize,
                        CancellationToken = options.CancellationToken
                    });


            var electionTask =
                Task.Run(async () =>
                {
                    while (true)
                    {
                        var t = await Task.WhenAny(_holdElectionsSemaphore.WaitAsync());

                        if (!t.IsCompletedSuccessfully)
                        {
                            break;
                        }

                        //Add new items to the queue
                        if (inputQueue.TryReceiveAll(out var ii))
                        {
                            queuedItems.EnqueueRange(ii);
                        }

                        //Check if the queue has an item that should go before the electedBuffer's last item.
                        ElectionBufferContainsOutOfOrderItem = electedBufferCount == 0
                            ? false
                            : ElectionBufferContainsOutOfOrderItem ||
                                (
                                    queuedItems.TryPeek(out var _, out var bestPriorityInQueue)
                                    && comparer.Compare(bestPriorityInQueue, worstPrioInElected) < 0
                                );

                        //If so, try to reclaim electedBuffer's items and add them to the queue too.
                        if (ElectionBufferContainsOutOfOrderItem && electedBuffer.TryReceiveAll(out var oldItems))
                        {
                            ElectionBufferContainsOutOfOrderItem = false;
                            Interlocked.Add(ref electedBufferCount, -oldItems.Count);
                            queuedItems.EnqueueRange(oldItems); //O(m log(n+m)), m is expected to be small.
                        }

                        if (queuedItems.Count == 0 && !ElectionBufferContainsOutOfOrderItem)
                        {
                            electionsCanTerminateGracefullyEvent.Set();
                        }
                        else
                        {
                            electionsCanTerminateGracefullyEvent.Reset();
                            if (!ElectionBufferContainsOutOfOrderItem)
                            {
                                while (electedBufferCount < electedBufferSize && queuedItems.TryDequeue(out var m, out var p))
                                {
                                    worstPrioInElected = p;
                                    await electedBuffer.SendAsync((m, p));
                                    Interlocked.Increment(ref electedBufferCount);
                                }
                            }
                        }
                        _electionsHeld.Set();
                    }
                });

            var outputBlock = electedBuffer.WithNotification(h =>
                {
                    h.OnDeliveringMessages = msgs =>
                    {
                        Interlocked.Add(ref electedBufferCount, -msgs.Count);
                        TriggerElection();
                    };
                    h.OnReservationReleased = _ => TriggerElection();
                    h.NotifySynchronously = true;
                }
            );

            inputQueue.Completion.ContinueWith(async t =>
            {
                if (t.IsCompletedSuccessfully)
                {
                    await electionsCanTerminateGracefullyEvent.WaitAsync();
                }
                _holdElectionsSemaphore.Terminate();
                queuedItems.Clear();
            });

            electionTask.ContinueWith(_ =>
            {
                _ = inputQueue.PropagateCompletion(electedBuffer);
            });

            _boundedState = new BoundedPropagatorBlock<
                (T Message, TPriority Priority),
                (T Message, TPriority Priority)>
            (
                inputQueue,
                outputBlock,
                options.BoundedCapacity,
                onEntered: TriggerElection
            );
        }
    }
}
