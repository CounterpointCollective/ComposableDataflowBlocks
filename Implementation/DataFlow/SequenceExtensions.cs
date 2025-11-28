using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public static class SequenceExtensions
    {
        public static async Task<IPropagatorBlock<I, O>> AndThenAsync<I, T, O>(
            this Task<IPropagatorBlock<I, T>> tFirst,
            Func<CancellationToken, Task<IPropagatorBlock<T, O>>> tSndFactory,
            CancellationToken cancellationToken = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var tSnd = tSndFactory(cts.Token);
            IPropagatorBlock<I, T> fst;
            try
            {
                fst = await tFirst;
            }
            catch (Exception e)
            {
                await cts.CancelAsync();
                try
                { 
                    var snd = await tSnd;
                    snd.Fault(e);
                } catch
                {
                    //Swallow
                }
                throw;
            }

            return await fst.AndThenAsync(tSnd);
        }

        public static Task<IPropagatorBlock<I, O>> AndThenAsync<I, T, O>(
            this Task<IPropagatorBlock<I, T>> tFirst,
            Task<IPropagatorBlock<T, O>> tSnd)
        => tFirst.AndThenAsync(_ => tSnd);

        public static async Task<IPropagatorBlock<I, O>> AndThenAsync<I, T, O>(
            this Task<IPropagatorBlock<I, T>> first,
            IPropagatorBlock<T, O> b2
        )
        {
            try
            {
                var b1 = await first;
                return b1.AndThen(b2);
            }
            catch (Exception ex)
            {
                b2.Fault(ex);
                throw;
            }
        }


        public static async Task<IPropagatorBlock<I, O>> AndThenAsync<I, T, O>(
            this IPropagatorBlock<I, T> first,
            Task<IPropagatorBlock<T, O>> next
        ) => first.AndThen(await next);

        public static IPropagatorBlock<I, O> AndThen<I, T, O>(
            this IPropagatorBlock<I, T> first,
            IPropagatorBlock<T, O> next
        )
        {
            first.LinkTo(next, new() { PropagateCompletion = true });
            return DataflowBlock.Encapsulate(first, next);
        }

        public static async Task<ISourceBlock<O>> AndThenAsync<I, O>(
            this Task<ISourceBlock<I>> tFirst,
            Func<CancellationToken, Task<IPropagatorBlock<I, O>>> tSndFactory,
            CancellationToken cancellationToken = default
        )
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var tSnd = tSndFactory(cts.Token);
            ISourceBlock<I> fst;
            try
            {
                fst = await tFirst;
            }
            catch (Exception e)
            {
                await cts.CancelAsync();
                try
                {
                    var snd = await tSnd;
                    snd.Fault(e);
                }
                catch
                {
                    //Swallow
                }
                throw;
            }

            return await fst.AndThenAsync(tSnd);
        }

        public static Task<ISourceBlock<O>> AndThenAsync<I, O>(
            this Task<ISourceBlock<I>> first,
            Task<IPropagatorBlock<I, O>> next
        ) => first.AndThenAsync(_ => next);


        public static async Task<ISourceBlock<O>> AndThenAsync<I, O>(
            this Task<ISourceBlock<I>> first,
            IPropagatorBlock<I, O> next
        )
        {
            try
            {
                var b1 = await first;
                return b1.AndThen(next);
            }
            catch (Exception ex)
            {
                next.Fault(ex);
                throw;
            }
        }


        public static async Task<ISourceBlock<O>> AndThenAsync<I, O>(
            this ISourceBlock<I> first,
            Task<IPropagatorBlock<I, O>> next
        ) => first.AndThen(await next);

        public static ISourceBlock<O> AndThen<I, O>(
            this ISourceBlock<I> first,
            IPropagatorBlock<I, O> next
        )
        {
            first.LinkTo(next, new() { PropagateCompletion = true });
            return next;
        }

    }
}
