using CounterpointCollective.Utilities;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Internal
{
    public readonly struct PostponedMessage<B, T> : System.IEquatable<PostponedMessage<B, T>> where B : ITargetBlock<T>
    {
        public ISourceBlock<T> SourceBlock { get; }
        public B Target { get; }
        public DataflowMessageHeader MessageHeader { get; }

        public PostponedMessage(ISourceBlock<T> sourceBlock, B target, DataflowMessageHeader messageHeader)
        {
            SourceBlock = sourceBlock;
            Target = target;
            MessageHeader = messageHeader;
        }

        public bool Consume([MaybeNullWhen(false)] out T messageValue)
        {
            var v = SourceBlock.ConsumeMessage(MessageHeader, Target, out var messageConsumed);
            if (messageConsumed)
            {
                messageValue = v!;
                return true;
            }
            else
            {
                messageValue = default;
                return false;
            }
        }

        public bool Reserve() => SourceBlock.ReserveMessage(MessageHeader, Target);
        public void ReleaseReservation() => SourceBlock.ReleaseReservation(MessageHeader, Target);


        public override bool Equals(object? obj)
            => obj is PostponedMessage<B, T> other && Equals(other);

        public override int GetHashCode()
            => HashCode.Combine(SourceBlock, MessageHeader, Target);

        public static bool operator ==(PostponedMessage<B, T> left, PostponedMessage<B, T> right) => left.Equals(right);

        public static bool operator !=(PostponedMessage<B, T> left, PostponedMessage<B, T> right) => !(left == right);

        public bool Equals(PostponedMessage<B, T> other) => ReferenceEquals(SourceBlock, other.SourceBlock)
                && EqualityComparer<B>.Default.Equals(Target, other.Target)
                && MessageHeader == other.MessageHeader;

    }

    public class PostponedMessages<B, T>(B owner): IEnumerable<PostponedMessage<B,T>>
    where B : ITargetBlock<T>
    {
        private readonly UniquePriorityQueue<ISourceBlock<T>, DataflowMessageHeader, long> _priorityQueue = new();
        private long i;

        public int Count => _priorityQueue.Count;

        public long Postpone(ISourceBlock<T> s, DataflowMessageHeader h)
        {
            _priorityQueue.Enqueue(s, h, ++i);
            return i;
        }

        public long Increment => ++i;

        public bool Unregister(ISourceBlock<T> s) => _priorityQueue.Remove(s, out var _, out var _);

        public bool Unregister(ISourceBlock<T> s, DataflowMessageHeader h)
        {
            if (_priorityQueue.TryGet(s, out var current, out var p) && current == h)
            {
                return Unregister(s);
            } else
            {
                return false;
            }
        }

        public bool IsEmpty => Count == 0;

        public bool TryPeekPostponed([MaybeNullWhen(false)] out PostponedMessage<B, T> postponedMessage)
        {
            if (_priorityQueue.TryPeek(out var s, out var h, out var _))
            {
                postponedMessage = new PostponedMessage<B, T>(s, owner, h);
                return true;
            }
            postponedMessage = default;
            return false;
        }

        public bool TryDequeuePostponed(out PostponedMessage<B, T> postponedMessage)
        {
            if (_priorityQueue.TryDequeue(out var s, out var h, out var _))
            {
                postponedMessage = new PostponedMessage<B, T>(s, owner, h);
                return true;
            }
            postponedMessage = default;
            return false;
        }

        public bool TryPeekPostponedFor(ISourceBlock<T> t, [MaybeNullWhen(false)] out PostponedMessage<B, T> postponedMessage)
        {
            if (_priorityQueue.TryGet(t, out var h, out var _))
            {
                postponedMessage = new PostponedMessage<B, T>(t, owner, h);
                return true;
            } else
            {
                postponedMessage = default;
                return false;
            }
        }

        public void Clear() => _priorityQueue.Clear();

        public IEnumerator<PostponedMessage<B, T>> GetEnumerator() =>
            _priorityQueue.Select(e => new PostponedMessage<B, T>(e.Key, owner, e.Value.Value)).GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
