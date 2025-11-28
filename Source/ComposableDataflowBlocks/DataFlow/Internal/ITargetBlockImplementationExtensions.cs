
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks.Dataflow;
using System;

namespace CounterpointCollective.DataFlow.Internal;


public delegate bool DequeuePostponed<I>([MaybeNullWhen(false)] out ISourceBlock<I> sourceBlock, [MaybeNullWhen(false)] out DataflowMessageHeader header);

public static class ITargetBlockImplementationExtensions
{
    public static void ConsumePostponed<I>(this ITargetBlock<I> targetBlock, DequeuePostponed<I> dequeuePostponed, Action<I> accept, Action undo)
    {
        while (dequeuePostponed(out var sourceBlock, out var messageHeader))
        {
            var messageValue = sourceBlock.ConsumeMessage(
                messageHeader,
                targetBlock,
                out var messageConsumed
            );

            if (messageConsumed && messageValue != null)
            {
                accept(messageValue);
            }
            else
            {
                undo();
            }
        }
    }
}
