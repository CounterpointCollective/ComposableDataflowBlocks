using System;
using System.Threading;
using System.Threading.Tasks;
using CounterpointCollective.Extensions;

namespace CounterpointCollective.Threading
{
#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    public class AsyncLazy<T>(Func<CancellationToken, Task<T>> taskFactory)
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        private Task<T>? tInit;
        private T? value;
        private readonly SemaphoreSlim _mutex = new(1, 1);
        private CancellationTokenSource? cts;

        private int state;

        private const int Ready = 2;
        private const int MustReset = 1;
        private const int Blank = 0;

        public AsyncLazy(Func<Task<T>> taskFactory): this(_ => taskFactory()) {
        }

        /// <summary>
        /// Returns the value. If the value has not been initialized yet, this method
        /// will start the initialization and wait for it to complete.
        /// </summary>
        /// <param name="cancellationToken">
        /// Cancelling this token will only stop waiting for the value to be ready:
        /// initialization will continue in the background.
        /// To cancel the initialization itself, call <see cref="Reset"/>.
        /// </param>
        /// <returns>The initialized value of type <typeparamref name="T"/>.</returns>
        public async ValueTask<T> GetValueAsync(CancellationToken cancellationToken = default)
        {
            if (Volatile.Read(ref state) == Ready)
            {
                return value!;
            }

            using var _lock = await _mutex.LockAsync(cancellationToken);
            if (Volatile.Read(ref state) == Ready)
            {
                return value!;
            }

            do
            {
                if (Interlocked.Exchange(ref state, Blank) == MustReset)
                {
                    tInit = Initialize();
                }
                else
                {
                    tInit ??= Initialize();

                }

                var res = await tInit.WaitAsync(cancellationToken);
                if (Interlocked.CompareExchange(ref state, Ready, Blank) == Blank)
                {
                    value = res;
                    return value;
                }

            } while (true);
        }

        /// <summary>
        /// Resets the <see cref="AsyncLazy{T}"/> instance, forcing the next call to 
        /// <see cref="GetValueAsync"/> to start a new initialization.
        /// </summary>
        /// <remarks>
        /// If an initialization is currently in progress, it will be cancelled and
        /// its results will be ignored.
        /// </remarks>
        public void Reset()
        {
            cts?.Cancel();
            Interlocked.Exchange(ref state, MustReset);
        }

        private Task<T> Initialize()
        {
            var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
            cts = new CancellationTokenSource();
            var t = taskFactory(cts.Token);

            _ = t.ContinueWith(task =>
            {
                try
                {
                    if (task.IsFaulted)
                    {
                        tcs.SetException(task.Exception!.InnerExceptions);
                    }
                    else if (task.IsCompletedSuccessfully)
                    {
                        tcs.SetResult(task.Result);
                    }

                    else
                    {
                        tcs.SetCanceled();
                    }
                }
                finally
                {
                    var prevCts = Interlocked.Exchange(ref cts, null);
                    prevCts?.Dispose();
                }
            }, TaskScheduler.Default);

            return tcs.Task;
        }
    }
}
