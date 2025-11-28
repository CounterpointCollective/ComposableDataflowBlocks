using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
