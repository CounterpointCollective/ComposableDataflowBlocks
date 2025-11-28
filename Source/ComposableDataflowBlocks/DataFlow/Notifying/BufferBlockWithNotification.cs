using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Notifying
{
    public sealed class BufferBlockWithNotification<T>(BufferBlock<T> innerBlock, ConfigureHooks<T> c)
        : PropagatingBlockWithNotification<T, T>(innerBlock, c)
    {
        public int Count => innerBlock.Count;
    }
}
