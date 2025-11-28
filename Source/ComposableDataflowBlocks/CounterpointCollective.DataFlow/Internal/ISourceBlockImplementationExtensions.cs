using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Dataflow;
using System;
using System.Collections.Generic;

namespace CounterpointCollective.DataFlow.Internal
{
    public static class ISourceBlockImplementationExtensions
    {
        public static bool ConsumeMessageCore<T>(
            this ISourceBlock<T>? source,
            DataflowMessageHeader messageHeader,
            ITargetBlock<T> target,
            [MaybeNullWhen(false)] out T messageValue
        )
        {
            if (source == null)
            {
                throw new InvalidOperationException("Must pass source if consumeToAccept.");
            }
            else
            {
                messageValue = source.ConsumeMessage(
                    messageHeader,
                    target,
                    out var messageConsumed
                );

                if (messageConsumed && messageValue != null)
                {
                    return true;
                }
                else
                {
                    messageValue = default;
                    return false;
                }
            }
        }

        public static DataflowMessageStatus OfferMessageCore<I>(
            this ITargetBlock<I> target,
            DataflowMessageHeader messageHeader,
            I messageValue,
            ISourceBlock<I>? source,
            bool consumeToAccept,
            bool acceptSynchronously,
            Action<I> accept,
            IDictionary<ISourceBlock<I>, DataflowMessageHeader> postponed
        )
        {
            if (source != null)
            {
                _ = postponed.Remove(source);
            }
            if (postponed.Count == 0 && acceptSynchronously)
            {
                if (consumeToAccept)
                {
                    if (source.ConsumeMessageCore(messageHeader, target, out var v))
                    {
                        messageValue = v;
                    }
                    else
                    {
                        return DataflowMessageStatus.NotAvailable;
                    }
                }
                accept(messageValue);
                return DataflowMessageStatus.Accepted;
            }
            else if (source == null)
            {
                return DataflowMessageStatus.Declined;
            }
            else
            {
                postponed[source] = messageHeader;
                return DataflowMessageStatus.Postponed;
            }
        }

        public static DataflowMessageStatus OfferMessageCore<B, I>(
            this ITargetBlock<I> target,
            DataflowMessageHeader messageHeader,
            I messageValue,
            ISourceBlock<I>? source,
            bool consumeToAccept,
            bool acceptSynchronously,
            Action<I> accept,
            PostponedMessages<B, I> postponed
        ) where B: ITargetBlock<I>
        {
            if (source != null)
            {
                postponed.Unregister(source);
            }
            if (postponed.IsEmpty && acceptSynchronously)
            {
                if (consumeToAccept)
                {
                    if (source.ConsumeMessageCore(messageHeader, target, out var v))
                    {
                        messageValue = v;
                    }
                    else
                    {
                        return DataflowMessageStatus.NotAvailable;
                    }
                }
                accept(messageValue);
                return DataflowMessageStatus.Accepted;
            }
            else if (source == null)
            {
                return DataflowMessageStatus.Declined;
            }
            else
            {
                postponed.Postpone(source, messageHeader);
                return DataflowMessageStatus.Postponed;
            }
        }
    }
}
