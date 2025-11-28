using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using CounterpointCollective.Extensions;

namespace CounterpointCollective.Threading
{
    public sealed record CallerInfo
    {
        public string? CallerFilePath { get; }

        public string? CallerMemberName { get; }
        public int CalledLineNumber { get; }

        public CallerInfo(string? callerFilePath, string? callerMemberName, int calledLineNumber)
        {
            CallerFilePath = callerFilePath;
            CallerMemberName = callerMemberName;
            CalledLineNumber = calledLineNumber;
        }
    }

    public sealed class NamedLock(
        string key,
        NamedLockHandler rkdLockHandler,
        CallerInfo callerInfo,
        Func<string>? fDebugInfo
    ) : IDisposable
    {
#pragma warning disable CA1034 // Nested types should not be visible
        public sealed record Description
#pragma warning restore CA1034 // Nested types should not be visible
        {
            public CallerInfo CallerInfo { get; }

            [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
            public string? DebugInfo { get; }

            public Description(NamedLock namedLock)
            {
                CallerInfo = namedLock.CallerInfo;
                LifeTime = DateTime.Now - namedLock.Timestamp;
                DebugInfo = namedLock.DebugInfo;
            }

            public TimeSpan LifeTime { get; }
        }

        public string Key { get; private set; } = key;

        public CallerInfo CallerInfo { get; } = callerInfo;

        public string? DebugInfo => fDebugInfo?.Invoke();

        public DateTime Timestamp { get; } = DateTime.Now;

        public void Dispose() => rkdLockHandler.Unlock(this);

        public Description Describe() => new(this);
    }

    public sealed class NamedLockHandler
    {
        private readonly record struct LockState(
            NamedLock CurrentLock,
            BlockingCollection<(NamedLock NamedLock, TaskCompletionSource Tcs)> Queue,
            int QueueCount
        );

        private readonly ConcurrentDictionary<string, LockState> _locks = [];

        public async Task<NamedLock> LockAsync(
            string key,
            Func<string>? fDebugInfo = null,
            [CallerFilePath] string? callerFilePath = null,
            [CallerMemberName] string? callerMemberName = null,
            [CallerLineNumber] int callerLineNumber = 0,
            CancellationToken cancellationToken = default
        )
        {
            var callerInfo = new CallerInfo(callerFilePath, callerMemberName, callerLineNumber);
            var res = new NamedLock(key, this, callerInfo, fDebugInfo);

            //Ensure correct QueueCount atomically, even before we write into the queue.
            var v = _locks.AddOrUpdate(key,
                _ => new LockState(res, [], 0),
                (_, existing) => existing with { QueueCount = existing.QueueCount + 1 }
            );

            if (v.CurrentLock != res)
            {
                //We didn't get the lock immediately
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                using var ctr = cancellationToken.Register(() => tcs.TrySetCanceled());
                v.Queue.Add((res, tcs), CancellationToken.None); //Now we actually write into the queue, synching QueueCount with reality again.
                await tcs.Task; //May throw OperationCanceledException
            }
            return res;
        }

        public void Unlock(NamedLock currLock)
        {
            var lockState = _locks[currLock.Key];
            if (lockState.CurrentLock != currLock)
            {
                throw new ArgumentException(
                    "Illegal locking state. Different current lock holder"
                );
            }

            while (true)
            {
                if (lockState.QueueCount > 0)
                {
                    var nextLock = lockState.Queue.Take(); //This may block very shortly when the QueueCount and actual queue length are not yet in sync in LockAsync.
                    lockState = _locks.Update(
                        currLock.Key,
                        (_, lockState) => lockState with { CurrentLock = nextLock.NamedLock, QueueCount = lockState.QueueCount - 1 },
                        lockState
                    );
                    if (nextLock.Tcs.TrySetResult())
                    {
                        break;
                    }
                } else if (_locks.TryRemove(new(currLock.Key, lockState)))
                {
                    break;
                }
                lockState = _locks[currLock.Key];
            }
        }

        public bool IsLocked(string key) => _locks.ContainsKey(key);

        public (string Name, NamedLock.Description[] Descriptions)[] DescribeLocks() => _locks
                .Select(
                    kv =>
                        (
                            kv.Key,
                            new[] { kv.Value.CurrentLock.Describe() }
                                .Concat(kv.Value.Queue.Select(q => q.NamedLock.Describe()))
                                .ToArray()
                        )
                )
                .OrderByDescending(d => d.Item2.First().LifeTime)
                .ToArray();
    }
}
