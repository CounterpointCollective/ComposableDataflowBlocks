using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Microsoft.Graph.SecurityNamespace;
using Schema.NET;

namespace UnitTests.DataFlow
{
    public static class AsyncLinkingExtensions
    {

        public static Task<IPropagatorBlock<I, I>> AndThenAsync<I>(
            this Task<IPropagatorBlock<I, I>> tFirst,
            Task<IPropagatorBlock<I, I>> tSnd)
        => tFirst.AndThenAsync(_ => tSnd);

        public static async Task<IPropagatorBlock<I,I>> AndThenAsync<I>(
            this Task<IPropagatorBlock<I,I>> tFirst,
            Func<CancellationToken, Task<IPropagatorBlock<I,I>>> tSndFactory,
            CancellationToken cancellationToken = default)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var tSnd = tSndFactory(cts.Token);
            IPropagatorBlock<I, I> fst;
            try
            {
                fst = await tFirst;
            }
            catch (Exception e)
            {
                cts.Cancel();
                await Task.WhenAny(tSnd);
                if (tSnd.IsCompletedSuccessfully)
                {
                    tSnd.Result.Fault(e);
                }
                throw;
            }

            return await fst.AndThenAsync(tSnd);
        }

        private static async Task<IPropagatorBlock<I, I>> AndThenAsync<I>(this IPropagatorBlock<I, I> fst, Task<IPropagatorBlock<I, I>> tSnd)
        {
            var snd = await tSnd;
            fst.LinkTo(snd, new() { PropagateCompletion = true });
            return DataflowBlock.Encapsulate<I, I>(fst, snd);
        }
    }
}
