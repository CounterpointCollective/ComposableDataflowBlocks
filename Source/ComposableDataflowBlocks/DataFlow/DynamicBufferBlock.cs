using CounterpointCollective.DataFlow.Encapsulation;
using System;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public sealed class DynamicBufferBlock<T> : AbstractEncapsulatedPropagatorBlock<T, T>
    {
        private readonly BoundedPropagatorBlock<T,T> _inner;

        public DynamicBufferBlock(DataflowBlockOptions options, Action? onEntered = null) =>
            _inner = new(new BufferBlock<T>(new() { CancellationToken = options.CancellationToken }), options.BoundedCapacity, onEntered);

        protected override ITargetBlock<T> TargetSide => _inner;

        protected override ISourceBlock<T> SourceSide => _inner;

        public int Count => _inner.Count;

        public int BoundedCapacity {
            get => _inner.BoundedCapacity; 
            set => _inner.BoundedCapacity = value;
        }
    }
}
