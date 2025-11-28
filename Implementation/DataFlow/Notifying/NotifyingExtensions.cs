using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow.Notifying
{
    public static class NotifyingExtensions
    {
        public static BufferBlockWithNotification<T> WithNotification<T>(this BufferBlock<T> b, ConfigureHooks<T> c)
        => new(b, c);

        public static BufferBlockWithNotification<T> WithNotification<T>(this BufferBlockWithNotification<T> b, ConfigureHooks<T> c)
            => (BufferBlockWithNotification<T>) b.AddHooks(c);

        public static PropagatingBlockWithNotification<I, O> WithNotification<I, O>(this IPropagatorBlock<I,O> b, ConfigureHooks<O> c)
        => new(b, c);

        public static SourceBlockWithNotification<T> WithNotification<T>(this ISourceBlock<T> b, ConfigureHooks<T> c)
        => new(b, c);
    }
}
