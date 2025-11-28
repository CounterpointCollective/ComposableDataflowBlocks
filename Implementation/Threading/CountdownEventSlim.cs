using System;
using System.Threading;
using System.Threading.Tasks;

namespace CounterpointCollective.Threading
{
    public class CountdownEventSlim(int start)
    {
        private readonly BinarySemaphoreSlim _sem = new(true);
        private int remaining = start;

        public bool IsOpen => Volatile.Read(ref remaining) == 0;

        public async Task WaitAsync(CancellationToken cancellationToken = default)
        {
            while (!IsOpen)
            {
                await _sem.WaitAsync(cancellationToken);
            }
        }

        public void Increment() => Interlocked.Increment(ref remaining);

        public bool Decrement()
        {
            var newValue = Interlocked.Decrement(ref remaining);
            if (newValue == 0)
            {
                _sem.Release();
            }
            return newValue == 0;
        }

        public V SupressingCompletion<V>(Func<V> f, Func<V> onCompleted)
        {
            if (IsOpen)
            {
                //we are already completing.
                return onCompleted();
            }
            else
            {
                Increment();

                try
                {
                    return f();
                }
                finally
                {
                    Decrement();
                }
            }
        }

        public void SupressingCompletion(Action a, Action onCompleted)
        => SupressingCompletion(
            () =>
            {
                a();
                return 0;
            },
            () =>
            {
                onCompleted();
                return 0;
            }
        );
    }
}
