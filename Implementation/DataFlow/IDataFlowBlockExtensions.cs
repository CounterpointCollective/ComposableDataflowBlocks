using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace CounterpointCollective.DataFlow
{
    public static class IDataFlowBlockExtensions
    {
        public static Task PropagateFaultyCompletion(this Task t, params IDataflowBlock[] bs) =>
            t.ContinueWith(r =>
            {
                if (r.IsFaulted)
                {
                    foreach (var b2 in bs)
                    {
                        b2.Fault(r.Exception!);
                    }
                }
            });

        public static Task PropagateCompletion(this Task t, params IDataflowBlock[] bs) =>
            PropagateCompletion(t, TaskContinuationOptions.None, bs);

        public static async Task PropagateCompletionAsync(this Task t, TaskCompletionSource tcs)
        {
            var r = await Task.WhenAny(t);
            if (r.IsFaulted)
            {
                tcs.SetException(r.Exception!);
            } else if (r.IsCanceled)
            {
                tcs.SetCanceled();
            } else
            {
                tcs.SetResult();
            }
        }

        public static Task PropagateCompletion(
            this Task t,
            TaskContinuationOptions continuationOptions,
            params IDataflowBlock[] bs
        ) =>
            t.ContinueWith(
                r =>
                {
                    if (r.IsFaulted)
                    {
                        foreach (var b2 in bs)
                        {
                            b2.Fault(r.Exception!);
                        }
                    }
                    else
                    {
                        foreach (var b2 in bs)
                        {
                            b2.Complete();
                        }
                    }
                },
                continuationOptions: continuationOptions
            );

        public static Task PropagateCompletion(
            this IDataflowBlock b,
            TaskContinuationOptions continuationOptions,
            params IDataflowBlock[] bs
        ) => b.Completion.PropagateCompletion(continuationOptions, bs);

        public static Task PropagateCompletion(this IDataflowBlock b, params IDataflowBlock[] bs) =>
            b.PropagateCompletion(TaskContinuationOptions.None, bs);
    }
}
