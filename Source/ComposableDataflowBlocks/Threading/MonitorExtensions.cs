using System;
using System.Threading;

namespace CounterpointCollective.Threading
{
    public static class MonitorExtensions
    {
        public static T TryLock<T>(this object monitor, Func<bool, T> f)
        {
            if (Monitor.TryEnter(monitor))
            {
                try
                {
                    return f(true);
                }
                finally
                {
                    Monitor.Exit(monitor);
                }
            }
            else
            {
                return f(false);
            }
        }

        public static void TryLock(this object monitor, Action<bool> f)
        => TryLock<int>(monitor, b => { f(b); return 0; });

    }
}
