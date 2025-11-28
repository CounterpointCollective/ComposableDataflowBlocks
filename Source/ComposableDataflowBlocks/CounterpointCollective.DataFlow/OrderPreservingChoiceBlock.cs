using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CounterpointCollective.DataFlow.Encapsulation;
using CounterpointCollective.DataFlow.Notifying;

namespace CounterpointCollective.DataFlow
{
    public class OrderPreservingChoiceBlock<I, O> : AbstractEncapsulatedPropagatorBlock<I, O>
    {
        private object Lock { get; } = new();

        internal Branch<I, O>? CurrentBranch { get; private set; }
        internal Branch<I, O> ThenBranch { get; private set; }
        internal Branch<I, O> ElseBranch { get; private set; }

        protected override ITargetBlock<I> TargetSide => _boundedPropagatorBlock;

        protected override ISourceBlock<O> SourceSide => _boundedPropagatorBlock;

        private readonly BoundedPropagatorBlock<I,O> _boundedPropagatorBlock;
        private readonly BufferBlock<O> _outputBuffer;

        public int Count => _boundedPropagatorBlock.Count;

        public int InputCount => Count - OutputCount;

        public int OutputCount => _outputBuffer.Count;

        public int BoundedCapacity
        {
            get => _boundedPropagatorBlock.BoundedCapacity;
            set => _boundedPropagatorBlock.BoundedCapacity = value;
        }

        public OrderPreservingChoiceBlock(
            Predicate<I> pred,
            IPropagatorBlock<I, O> thenBlock,
            IPropagatorBlock<I, O> elseBlock,
            ExecutionDataflowBlockOptions? options = null
        )
        {
            options ??= new();


            var entrance = new GroupAdjacentBlock<I, bool, I>(h => pred(h), h => h, new() { CancellationToken = options.CancellationToken }, flushOnIdle: true);
            _outputBuffer = new BufferBlock<O>(new() { CancellationToken = options.CancellationToken });

            _boundedPropagatorBlock = new BoundedPropagatorBlock<I, O>(
                DataflowBlock.Encapsulate(entrance, _outputBuffer),
                options.BoundedCapacity
            );

            var conditionCheckBlock =
                entrance
                .BeginEncapsulation()
                .TransformMany(g =>
                 {
                     OnMessagesFanningOutTo(g.Key ? ThenBranch! : ElseBranch!, g.Count());
                     return g.Select(e => (g.Key, e));
                 }, new() { CancellationToken = options.CancellationToken, SingleProducerConstrained = options.SingleProducerConstrained })
                .SelectLast(e => _boundedPropagatorBlock.CreateExit<(bool P, I V)>(e))
                .Build();

            conditionCheckBlock.BeginEncapsulation()
            .SynchronousFilter(t => t.P)
            .SynchronousTransform(t => t.V)
            .LinkTo(thenBlock, new() { PropagateCompletion = true });

            conditionCheckBlock.BeginEncapsulation()
            .SynchronousFilter(t => !t.P)
            .SynchronousTransform(t => t.V)
            .LinkTo(elseBlock, new() { PropagateCompletion = true });


            ThenBranch = new(Lock, BranchName.Then, options.CancellationToken);
            thenBlock = thenBlock.BeginEncapsulation()
                .LinkTo(ThenBranch.Gate, new() { PropagateCompletion = true })
                .WithNotification(h => {
                    h.OnDeliveringMessages = msg => OnMessagesFannedInFrom(ThenBranch, msg.Count);
                    h.NotifySynchronously = true;
                })
                .Build();

            ElseBranch = new(Lock, BranchName.Else, options.CancellationToken);
            elseBlock = elseBlock.BeginEncapsulation()
                .LinkTo(ElseBranch.Gate, new() { PropagateCompletion = true })
                .WithNotification(h => {
                    h.OnDeliveringMessages = msg => OnMessagesFannedInFrom(ElseBranch, msg.Count);
                    h.NotifySynchronously = true;
                }).Build();

#pragma warning disable CA2000 // Dispose objects before losing scope
            thenBlock.LinkTo(_outputBuffer, new());
            elseBlock.LinkTo(_outputBuffer, new());
#pragma warning restore CA2000 // Dispose objects before losing scope


            SetupCompletion(conditionCheckBlock.Completion, thenBlock, options.CancellationToken);
            SetupCompletion(conditionCheckBlock.Completion, elseBlock, options.CancellationToken);

            Task.Run(async () =>
            {
                var t = Task.WhenAll(
                    entrance.Completion,
                    ThenBranch.Gate.Completion,
                    ElseBranch.Gate.Completion
                );
                await Task.WhenAny(t);
                if (!options.CancellationToken.IsCancellationRequested)
                {
                    _ = t.PropagateCompletion(_outputBuffer);
                }
            });
        }

        private readonly LinkedList<(Branch<I, O> Branch, int RequiredDeliveryQuota)> _queue = [];

        private void OnMessagesFanningOutTo(Branch<I, O> branch, int count)
        {
            lock (Lock)
            {
                CurrentBranch ??= branch;
                if (CurrentBranch == branch && _queue.Count == 0)
                {
                    branch.GrantDeliveryQuota(count);
                }
                else if (_queue.Last != null && _queue.Last.Value.Branch == branch)
                {
                    _queue.Last.Value = (branch, _queue.Last.Value.RequiredDeliveryQuota + count);
                }
                else
                {
                    _queue.AddLast((branch, count));
                }
            }
        }

        private void OnMessagesFannedInFrom(Branch<I, O> b, int count)
        {
            _boundedPropagatorBlock.AdjustCount(count);
            lock (Lock)
            {
                if (CurrentBranch != b)
                {
                    throw new InvalidOperationException($"not expecting messages from {b.Name}");
                }
                b.ConsumeDeliveryQuota(count);
                if (!b.HasOutstandingDeliveries)
                {
                    var f = _queue.First;
                    if (f != null)
                    {
                        CurrentBranch = f.Value.Branch;
                        f.Value.Branch.GrantDeliveryQuota(f.Value.RequiredDeliveryQuota);
                        _queue.RemoveFirst();
                    }
                    else
                    {
                        CurrentBranch = null;
                    }
                }
            }
        }

        private void SetupCompletion(Task completionAllowed, IPropagatorBlock<I, O> myBranch, CancellationToken cancellationToken)
        {
            completionAllowed.PropagateCompletion(myBranch);
            myBranch.Completion.ContinueWith(t =>
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
                else if (t.IsFaulted)
                {
                    Fault(t.Exception);
                }
                else if (!completionAllowed.IsCompleted)
                {
                    Fault(new InvalidOperationException("Branch completed prematurely"));
                }
            }, cancellationToken);
        }
    }
    internal enum BranchName { Then, Else };

    internal record Branch<I, O>
    {
        private object Lock { get; }
        public BranchName Name { get; }

        private readonly TokenGateBlock<O> _gate;
        public IPropagatorBlock<O, O> Gate { get; }

        public int Tokens => _gate.Tokens;

        public Branch(object l, BranchName name, CancellationToken cancellationToken)
        {
            Lock = l;
            Name = name;

            var b = new BufferBlock<O>(new() { CancellationToken = cancellationToken });
            _gate = new TokenGateBlock<O>(b);
            Gate = DataflowBlock.Encapsulate(_gate,b);
        }

        public void GrantDeliveryQuota(int c)
        {
            AdjustDeliveryQuota(c);
            _gate.Allow(c);
        }

        public void ConsumeDeliveryQuota(int count) => AdjustDeliveryQuota(-count);

        public bool HasOutstandingDeliveries
        {
            get
            {
                lock (Lock)
                { return outstandingDeliveries > 0; }
            }
        }

        private int outstandingDeliveries;

        private void AdjustDeliveryQuota(int delta)
        {
            lock (Lock)
            {
                outstandingDeliveries += delta;
            }
        }
    };
}
