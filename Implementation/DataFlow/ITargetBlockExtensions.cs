using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public static class ITargetBlockExtensions
    {
        public static void PostAsserted<T>(this ITargetBlock<T> target, T messageValue)
        {
            if (!target.Post(messageValue))
            {
                throw new InvalidOperationException("Target did not accept the message");
            }
        }
    }
}
