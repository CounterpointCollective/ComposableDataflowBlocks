using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace CounterpointCollective.Utilities
{
#pragma warning disable CA1711 // Identifiers should not have incorrect suffix
    public class UniquePriorityQueue<K,V,P> : IEnumerable<KeyValuePair<K,(V Value, P Priority)>> where K: notnull
#pragma warning restore CA1711 // Identifiers should not have incorrect suffix
    {
        private readonly Dictionary<K, (V Value, P Priority)> _items = [];
        private readonly PriorityQueue<K, P> _priorityQueue;

        public int Count => _items.Count;

        public UniquePriorityQueue() => _priorityQueue = new();

        public UniquePriorityQueue(IComparer<P> priorityComparer) => _priorityQueue = new(comparer: priorityComparer);


        public void Enqueue(K k, V v, P p)
        {
            _items[k] = (v, p);
            _priorityQueue.Enqueue(k, p);
            AmortizedCleanupIfNecessary();
        }

        public bool TryDequeue([MaybeNullWhen(false)] out K key, [MaybeNullWhen(false)] out V value, [MaybeNullWhen(false)] out P highestPriority)
        {
            while (_priorityQueue.TryDequeue(out key, out highestPriority))
            {
                if (_items.Remove(key, out var vp))
                {
                    var currentPriority = vp.Priority;
                    if (_priorityQueue.Comparer.Compare(highestPriority, currentPriority) == 0)
                    {
                        value = vp.Value;
                        return true;
                    } else
                    {
                        _items[key] = vp; // Reinsert. The priority was stale.
                    }
                }
            }
            value = default;
            return false;
        }

        public bool TryPeek([MaybeNullWhen(false)] out K key, [MaybeNullWhen(false)] out V value, [MaybeNullWhen(false)] out P highestPriority)
        {
            while (_priorityQueue.TryPeek(out key, out highestPriority))
            {
                if (_items.TryGetValue(key, out var pv) && _priorityQueue.Comparer.Compare(highestPriority, pv.Priority) == 0)
                {
                    value = pv.Value;
                    return true;
                }
                else
                {
                    _priorityQueue.Dequeue(); //Priority was stale.
                }
            }
            value = default;
            return false;
        }

        public bool DequeueIfFirst(K k, V value)
        {
            if (TryPeek(out var actualKey, out var actualValue, out var _) &&
               EqualityComparer<K>.Default.Equals(k, actualKey) &&
               EqualityComparer<V>.Default.Equals(value, actualValue)
            )
            {
                _priorityQueue.Dequeue();
                _items.Remove(k);
                return true;
            }
            return false;
        }

        public bool Remove(K s, [MaybeNullWhen(false)] out V value, [MaybeNullWhen(false)] out P priority)
        {
            if (_items.Remove(s, out var kv))
            {
                value = kv.Value;
                priority = kv.Priority;
                AmortizedCleanupIfNecessary();
                return true;
            }
            else
            {
                value = default;
                priority= default;
                return false;
            }
        }

        public bool TryGet(K k, [MaybeNullWhen(false)] out V v, [MaybeNullWhen(false)] out P p)
        {
            if (_items.TryGetValue(k, out var vp))
            {
                v = vp.Value;
                p = vp.Priority;
                return true;
            } else
            {
                v = default;
                p = default;
                return false;
            }
        }

        public (K key, V value, P priority) Peek() =>
            TryPeek(out var key, out var value, out var priority)
            ? (key, value, priority)
            : throw new InvalidOperationException("Queue is empty");

        public (V Value, P Priority) Get(K k) =>
            TryGet(k, out var v, out var p) ? (v, p) : throw new KeyNotFoundException();

        private void AmortizedCleanupIfNecessary()
        {
            if (_priorityQueue.Count > 8 && _priorityQueue.Count > _items.Count * 2)
            {
                _priorityQueue.Clear();
                foreach (var (k, (v, p)) in _items)
                {
                    _priorityQueue.Enqueue(k, p);
                }
            }
        }

        public IEnumerator<KeyValuePair<K, (V, P)>> GetEnumerator() => _items.GetEnumerator();
         

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        public void Clear()
        {
            _priorityQueue.Clear();
            _items.Clear();
        }
    }
}
