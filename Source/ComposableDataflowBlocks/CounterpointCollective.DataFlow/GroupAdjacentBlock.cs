using CounterpointCollective.DataFlow.Encapsulation;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public class GroupAdjacentBlock<T, K, V> : AbstractEncapsulatedPropagatorBlock<T, IGrouping<K, V>>
    {
        private sealed class Grouping(K key, IEnumerable<V> values) : IGrouping<K, V>
        {
            private readonly K _key = key;
            private readonly IEnumerable<V> _values = [..values];

            public K Key => _key;
            public IEnumerator<V> GetEnumerator() => _values.GetEnumerator();
            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
        }

        private readonly BoundedPropagatorBlock<T, IGrouping<K, V>> _boundedPropagatorBlock;

        protected override ITargetBlock<T> TargetSide => _boundedPropagatorBlock;
        protected override ISourceBlock<IGrouping<K, V>> SourceSide => _boundedPropagatorBlock;

        public int Count => _boundedPropagatorBlock.Count;

        public GroupAdjacentBlock(
            Func<T, K> keySelector,
            Func<T, V> valueSelector,
            DataflowBlockOptions options,
            bool flushOnIdle = false
        )
        {
            K currentKey = default!;
            List<V> currentGroup = [];

            var outputBlock = new BufferBlock<IGrouping<K, V>>(new() { CancellationToken = options.CancellationToken });

            var inputBlock = new BufferBlock<T>(new() { CancellationToken = options.CancellationToken });

            var a =
                inputBlock.Action(e =>
                {
                    var k = keySelector(e);
                    var v = valueSelector(e);
                    if (currentGroup.Count > 0 && !EqualityComparer<K>.Default.Equals(currentKey, k))
                    {
                        var g = new Grouping(currentKey, currentGroup);
                        outputBlock.PostAsserted(g);
                        currentGroup.Clear();
                    }
                    currentKey = k;
                    currentGroup.Add(v);
                    if (flushOnIdle && inputBlock.Count == 0)
                    {
                        var g = new Grouping(currentKey, currentGroup);
                        outputBlock.PostAsserted(g);
                        currentGroup.Clear();
                    }
                }, new() { CancellationToken = options.CancellationToken, SingleProducerConstrained = true });

            a.Completion.ContinueWith(t =>
            {
                if (t.IsCompletedSuccessfully)
                {
                    if (currentGroup.Count > 0)
                    {
                        var g = new Grouping(currentKey, currentGroup);
                        outputBlock.PostAsserted(g);
                        currentGroup.Clear();
                    }
                    outputBlock.Complete();
                }
                else
                {
                    currentGroup.Clear();
                    if (t.IsFaulted)
                    {
                        ((IDataflowBlock)outputBlock).Fault(t.Exception!);
                    }
                }
            });

            _boundedPropagatorBlock = new BoundedPropagatorBlock<T, IGrouping<K, V>>(
                inputBlock,
                outputBlock,
                options.BoundedCapacity
            );
        }
    }
}
