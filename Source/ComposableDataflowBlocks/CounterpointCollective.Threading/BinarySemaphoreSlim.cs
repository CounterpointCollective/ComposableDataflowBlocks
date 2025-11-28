using System;
using System.Threading;
using System.Threading.Tasks;

namespace CounterpointCollective.Threading
{
#pragma warning disable CA1001 // Types that own disposable fields should be disposable
    internal class BinarySemaphoreSlim(bool startOpen)
#pragma warning restore CA1001 // Types that own disposable fields should be disposable
    {
        private readonly SemaphoreSlim _sem = new(startOpen ? 1 : 0, 1);
        private int open = startOpen ? 1 : 0;
        private readonly CancellationTokenSource _cts = new();

        public void Release()
        {
            if (Interlocked.CompareExchange(ref open, 1, 0) == 0)
            {
                _ = _sem.Release();
            }
        }

        public async Task WaitAsync(CancellationToken cancellationToken = default)
        {
            CancellationTokenSource? cts = null;
            if (cancellationToken == default)
            {
                cancellationToken = _cts.Token;
            } else
            {
                cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cts.Token);
                cancellationToken = cts.Token;
            }


            using var _ = cts;

            await _sem.WaitAsync(cancellationToken);
            if (Interlocked.CompareExchange(ref open, 0, 1) != 1)
            {
                throw new InvalidOperationException("The semaphore flag was not in the expected open state. This cannot happen.");
            }
        }

        public async Task<bool> WaitAsync(TimeSpan t)
        {
            if (await _sem.WaitAsync(t, _cts.Token))
            {
                if (Interlocked.CompareExchange(ref open, 0, 1) != 1)
                {
                    throw new InvalidOperationException("The semaphore flag was not in the expected open state. This cannot happen.");
                }
                return true;
            }
            else
            {
                return false;
            }           
        }

        public void Wait()
        {
            _sem.Wait(_cts.Token);
            if (Interlocked.CompareExchange(ref open, 0, 1) != 1)
            {
                throw new InvalidOperationException("The semaphore flag was not in the expected open state. This cannot happen.");
            }
        }

        public bool Wait(TimeSpan t)
        {
            if (_sem.Wait(t, _cts.Token))
            {
                if (Interlocked.CompareExchange(ref open, 0, 1) != 1)
                {
                    throw new InvalidOperationException("The semaphore flag was not in the expected open state. This cannot happen.");
                }
                return true;
            }
            else
            {
                return false;
            }
        }

        public void Terminate() => _cts.Cancel();

        public bool Terminated => _cts.IsCancellationRequested;

        public void Reset() => Wait(TimeSpan.Zero);
    }
}
