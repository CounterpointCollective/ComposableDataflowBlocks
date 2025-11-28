using System;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Encapsulation
{
    public static class LinkToExtensions
    {
        public static IDisposable LinkToWithCustomCompletion<T>(this ISourceBlock<T> s, Task customCompletion, ITargetBlock<T> target, DataflowLinkOptions options)
            => s.LinkTo(
                new TargetWrapper<T>(customCompletion, target),
                options
            );

        private class TargetWrapper<T>(Task customCompletion, ITargetBlock<T> target) : ITargetBlock<T>
        {
            public Task Completion => target.Completion;

            public void Complete() => customCompletion.PropagateCompletion(target);
            public void Fault(Exception exception) => customCompletion.PropagateCompletion(target);
            public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T>? source, bool consumeToAccept) 
                => target.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
            
        }
    }
}
