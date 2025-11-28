using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public static class BoundedBlockExtensions
    {
        public static BoundedPropagatorBlock<I, O> WithBoundedCapacity<I, O>(this IPropagatorBlock<I, O> b, int boundedCapacity)
        => new(b, boundedCapacity);
    }
}
