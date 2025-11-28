using CounterpointCollective.DataFlow.Encapsulation;
using CounterpointCollective.DataFlow.Notifying;
using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    /// <summary>
    /// A wrapper that enforces a bounded capacity on an arbitrary ITargetBlock.
    /// This enables building dataflow pipelines with precise backpressure and
    /// bounded-buffer behaviour, even for blocks that do not natively support
    /// bounded capacity, like composed blocks.
    /// 
    /// Incoming messages are added to the Count.
    ///
    /// Exits can be created with <see cref="CreateExit{O}"/>. Messages that leave
    /// an exit are deduced from the Count.
    ///
    /// The Count can also be adjusted manually with <see cref="AdjustCount(int)"/>,
    /// e.g. if you make messages re-enter the block.
    ///
    /// If Count hits BoundedCapacity, no new messages will be allowed to enter,
    /// until Count drops below BoundedCapacity again. Then, postponed messages
    /// will be processed in FIFO order.
    ///
    /// If you set BoundedCapacity to DataflowBlockOptions.Unbounded, the block
    /// will only track Count, and all messages will be allowed to enter.
    /// </summary>
    public class BoundedTargetBlock<T> : AbstractEncapsulatedTargetBlock<T>
    {
        protected override ITargetBlock<T> TargetSide => AdmissionGateBlock;
        protected override IDataflowBlock CompletionSide => AdmissionGateBlock;


        private object ValueLock { get; } = new();
        private int reserved;
        private int count;
        private int boundedCapacity;

        private Action dispatchPendingEventsFromAllExits = () => { };


        public int Count
        {
            get
            {
                if (Completion.IsCompleted)
                {
                    return 0;
                }
                else
                {
                    dispatchPendingEventsFromAllExits();
                    return Volatile.Read(ref count);
                }
            }
        }

        private bool HaveSpace => boundedCapacity == DataflowBlockOptions.Unbounded || reserved + count < boundedCapacity;
        private readonly Action? _onEntered;

        private AdmissionGateBlock<T> AdmissionGateBlock { get; }
        public bool IsCompletionRequested => AdmissionGateBlock.IsCompletionRequested;
        public Task InputCompletion => AdmissionGateBlock.InputCompletion;

        public BoundedTargetBlock(
            ITargetBlock<T> inner,
            int boundedCapacity,
            Action? onEntered = null
        )
        {
            _onEntered = onEntered;
            this.boundedCapacity = boundedCapacity;
            var h = CreateHooks();
            AdmissionGateBlock = new AdmissionGateBlock<T>(inner, h);
        }

        private AdmissionGateHooks CreateHooks()
        {
            var res = new AdmissionGateHooks()
            {
                HasEntered = _onEntered
            };

            if (BoundedCapacity == DataflowBlockOptions.Unbounded)
            {
                //When capacity is unbounded, we only need to track the count..
                res.Entering = () =>
                {
                    lock (ValueLock)
                    {
                        count++;
                    }
                };
            }
            else
            {
                //When there is a bound, we need to check whether messages may enter as well.
                res.MayTryToEnter =
                () =>
                {
                    lock (ValueLock)
                    {
                        if (HaveSpace)
                        {
                            reserved++;
                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }
                };
                res.FailingToEnter =
                () =>
                {
                    lock (ValueLock)
                    {
                        var hadSpace = HaveSpace;
                        reserved--;
                        if (!hadSpace && HaveSpace)
                        {
                            AdmissionGateBlock.ProcessPostponedMessages();
                        }
                    }
                };
                res.Entering =
                () =>
                {
                    lock (ValueLock)
                    {
                        reserved--;
                        count++;
                    }
                };
            }

            return res;
        }

        /// <returns>Messages that leave the returned block are subtracted from Count.</returns>
        public ISourceBlock<TO> CreateExit<TO>(ISourceBlock<TO> exitBlock)
        {
            var ret = new SourceBlockWithNotification<TO>(exitBlock);

            ret.AddHooks(h =>
            {
                h.OnDeliveringMessages = ms => AdjustCount(-ms.Count);
                h.NotifySynchronously = true;
            }
            );

            var p = dispatchPendingEventsFromAllExits;
            dispatchPendingEventsFromAllExits = () =>
            {
                p();
                ret.DispatchPendingEvents();
            };
            return ret;
        }

        /// <summary>
        /// Use when you need to manually adjust the count of messages owned by the block,
        /// for instance when creating re-entry points.
        /// </summary>
        public void AdjustCount(int diff)
        {
            lock (ValueLock)
            {
                var hadSpace = HaveSpace;
                count += diff;
                if (!hadSpace && HaveSpace)
                {
                    AdmissionGateBlock.ProcessPostponedMessages();
                }
            }
        }

        /// <summary>
        /// You can dynamically adjust the bounded capacity of the block.
        /// While BoundedCapacity is less than or equal to Count, no new
        /// messages will be allowed.
        ///
        /// Set to DataflowBlockOptions.Unbounded to disable bounding.
        /// </summary>
        public int BoundedCapacity
        {
            get => Volatile.Read(ref boundedCapacity);
            set
            {
                lock (ValueLock)
                {
                    var hadSpace = HaveSpace;
                    var oldValue = boundedCapacity;
                    if (value != oldValue)
                    {
                        boundedCapacity = value;
                        if (oldValue == DataflowBlockOptions.Unbounded || value == DataflowBlockOptions.Unbounded)
                        {
                            AdmissionGateBlock.ConfigureHooks(CreateHooks());
                        }
                        if (!hadSpace && HaveSpace)
                        {
                            AdmissionGateBlock.ProcessPostponedMessages();
                        }
                    }
                }
            }
        }
    }
}
