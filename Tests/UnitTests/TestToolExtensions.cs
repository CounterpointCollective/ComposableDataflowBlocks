using System.Diagnostics;

namespace UnitTests
{
    public static class TestToolExtensions
    {
        public static Task Eventually(this Action a, int timeoutInMs = 5000, int interval = 10)
            =>
            new Func<Task>(() =>
            {
                a();
                return Task.CompletedTask;
            }).Eventually(timeoutInMs, interval);

        public async static Task Eventually(this Func<Task> a, int timeoutInMs = 5000, int interval = 10)
        {
            var s = Stopwatch.StartNew();
            Exception? e;
            do
            {
                e = null;
                try
                {
                    await a();
                }
                catch (Exception ex)
                {
                    e = ex;
                    await Task.Delay(interval);
                }
            } while (e != null && s.ElapsedMilliseconds < timeoutInMs);

            if (e != null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(e).Throw();
            }
        }

        public static Task Continuously(this Action a, int timeoutInMs = 5000, int interval = 10)
        =>
        new Func<Task>(() =>
        {
            a();
            return Task.CompletedTask;
        }).Continuously(timeoutInMs, interval);

        public async static Task Continuously(this Func<Task> a, int timeoutInMs = 5000, int interval = 10)
        {
            var s = Stopwatch.StartNew();
            do
            {
                await a();
                await Task.Delay(interval);
            } while (s.ElapsedMilliseconds < timeoutInMs);
        }
    }
}
