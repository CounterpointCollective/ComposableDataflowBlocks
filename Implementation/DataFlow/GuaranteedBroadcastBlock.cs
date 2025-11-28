using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CounterpointCollective.DataFlow.Encapsulation;
using CounterpointCollective.DataFlow.Notifying;
using CounterpointCollective.Utilities;

namespace CounterpointCollective.DataFlow
{
    public enum GuaranteedBroadcastBlockBoundedCapacityMode
    {
        SmallestQueue,
        LargestQueue
    }

    public class GuaranteedBroadcastBlockOptions : DataflowBlockOptions
    {
        /// <summary>
        /// Whether to use the smallest or largest queue size for bounded capacity.
        /// Defaults to <see cref="GuaranteedBroadcastBlockBoundedCapacityMode.SmallestQueue"/>,
        /// meaning the fastest consuming block will determine the Count.
        ///
        /// If you set it to <see cref="GuaranteedBroadcastBlockBoundedCapacityMode.LargestQueue"/>,
        /// the slowest consuming block will determine when the Count.
        /// </summary>
        public GuaranteedBroadcastBlockBoundedCapacityMode BoundedCapacityMode { get; set; } = GuaranteedBroadcastBlockBoundedCapacityMode.SmallestQueue;
    }

    /// <summary>
    /// Guaranteed delivery of messages to NrOfSources sources.
    /// Queueing up messages in the sources until the smallest queue reached BoundedCapacity.
    /// </summary>

    public sealed class GuaranteedBroadcastBlock<T> : AbstractEncapsulatedTargetBlock<T>
    {
        private object Lock { get; } = new();
        private readonly UniquePriorityQueue<int, long, long> _totalMessagesDeliveredPerQueue;
        private readonly BufferBlockWithNotification<T>[] _queues;
        public IReceivableSourceBlock<T> this[int index] => _queues[index];

        /// <summary>
        /// Size of the smallest/largest queue, depending on BoundedCapacityMode.
        /// </summary>
        public int Count => _bounded.Count;
        private readonly Action<int>? _onQueuesShrunk;
        private readonly BoundedTargetBlock<T> _bounded;

        public override Task Completion { get; }

        protected override ITargetBlock<T> TargetSide => _bounded;

        protected override IDataflowBlock CompletionSide => this;

        public GuaranteedBroadcastBlock(
            int nrOfSources,
            GuaranteedBroadcastBlockOptions options,
            Action<int>? onQueuesShrunk = null
        )
        {
            _onQueuesShrunk = onQueuesShrunk;

            _queues = new BufferBlockWithNotification<T>[nrOfSources];
            if (options.BoundedCapacityMode == GuaranteedBroadcastBlockBoundedCapacityMode.LargestQueue)
            {
                //In largest queue mode, the queue with the most total messages deliveries determines the Count.
                _totalMessagesDeliveredPerQueue = new();
            }
            else
            {
                //In smallest queue mode, the queue with the least total messages deliveries determines the Count,
                _totalMessagesDeliveredPerQueue = new(Comparer<long>.Create((x, y) => y.CompareTo(x)));
            }

            for (var i = 0; i < nrOfSources; i++)
            {
                var j = i;
                _queues[i] =
                    new BufferBlock<T>(new() { CancellationToken = options.CancellationToken })
                    .WithNotification(h =>
                    {
                        h.OnDeliveringMessages = m => HandleMessageDelivery(m, j);
                        //i also checked asynchronously, but synchronously is at least just as fast.
                        //apparently the costs for batching and delivery and context switching does
                        //not pay off against having to do less administration work.
                        h.NotifySynchronously = true; 
                    });


                _totalMessagesDeliveredPerQueue.Enqueue(i, 0, 0);
            }

            var a = new ActionBlock<T>(m =>
            {
                for (var i = 0; i < nrOfSources; i++)
                {
                    _queues[i].PostAsserted(m);
                }
            }, new() { CancellationToken = options.CancellationToken, SingleProducerConstrained = true });
            _bounded = new(a, options.BoundedCapacity);

            var tcs = new TaskCompletionSource();
            Completion = tcs.Task;
            Task.Run(async () =>
            {
                var t = await Task.WhenAny(a.Completion);
                _ = t.PropagateCompletion(_queues);
                await Task.WhenAny(Task.WhenAll(_queues.Select(b => b.Completion)));
                if (options.CancellationToken.IsCancellationRequested)
                {
                    tcs.SetCanceled();
                }
                else
                {
                    await t.PropagateCompletionAsync(tcs);
                }
            });
        }

        private void HandleMessageDelivery(DeliveringMessagesEvent m, int queueIndex)
        {
            lock (Lock)
            {
                //Remember the previous smallest/ largest queue total delivery
                var (_, totalDelivered, _) = _totalMessagesDeliveredPerQueue.Peek();

                //Update my total delivered count
                var (myTotalDelivered, _) = _totalMessagesDeliveredPerQueue.Get(queueIndex);
                myTotalDelivered += m.Count;
                _totalMessagesDeliveredPerQueue.Enqueue(queueIndex, myTotalDelivered, myTotalDelivered);

                //If the smallest/ largest queue total delivery changed, update blocks Count property.
                (_, var newTotalDelivered, _) = _totalMessagesDeliveredPerQueue.Peek();
                var diff = (int)(totalDelivered - newTotalDelivered);
                if (diff < 0)
                {
                    _bounded.AdjustCount(diff);
                    _onQueuesShrunk?.Invoke(diff);
                }
            }
        }
    }
}
