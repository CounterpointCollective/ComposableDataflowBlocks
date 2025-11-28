using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using CounterpointCollective.Threading;

namespace CounterpointCollective.DataFlow.Internal
{
    public delegate bool TryDequeuePostponedMessage<B, T>([MaybeNullWhen(false)] out PostponedMessage<B, T> postponedMessage)
    where B : ITargetBlock<T>;

    internal class PostponedMessagesManager<B, T> where B : ITargetBlock<T>
    {
        private Task PostponedMessagesLoop { get; }

        public PostponedMessagesManager(
            PostponedMessages<B, T> postponedMessages,
            Func<bool> allowToRun,
            Action? postAction = null
        ) :
            this
            (
                (out PostponedMessage<B, T> postponedMessage) =>
                {
                    if (allowToRun())
                    {
                        return postponedMessages.TryDequeuePostponed(out postponedMessage);
                    }
                    else
                    {
                        postponedMessage = default;
                        return false;
                    }
                },
                postAction
            )
        {
        }

        public PostponedMessagesManager(
            TryDequeuePostponedMessage<B, T> tryDequeuePostponedMessage,
            Action? postAction = null
        )
        {
            _tryDequeuePostponedMessage = tryDequeuePostponedMessage;
            _postAction = postAction;
            Consume = (in PostponedMessage<B, T> p) => { };
            PostponedMessagesLoop = Task.Run(() => Handle());
        }

        private readonly TryDequeuePostponedMessage<B, T> _tryDequeuePostponedMessage;

        public delegate void Consumer(in PostponedMessage<B, T> value);

        public Consumer Consume { get; set; }
        private readonly Action? _postAction;

        private readonly BinarySemaphoreSlim _sem = new(false);
        public object IncomingLock { get; } = new();

        public bool IsRunningPostponedMessages { get; private set; }

        public void ProcessPostponedMessages() => _sem.Release();

        public async Task ShutdownAsync()
        {
            _sem.Terminate();
            await PostponedMessagesLoop;
        }

        private async Task Handle()
        {

            while (true)
            {
                try
                {
                    await _sem.WaitAsync();
                } catch (OperationCanceledException)
                {
                    break;
                }

                while (!_sem.Terminated && TryDequeue(out var postponedMessage))
                {
                    Consume(in postponedMessage!);
                }
            }

            bool TryDequeue(out PostponedMessage<B, T> postponedMessage)
            {
                lock (IncomingLock)
                {
                    if (_tryDequeuePostponedMessage(out postponedMessage))
                    {
                        IsRunningPostponedMessages = true;
                        return true;
                    } else
                    {
                        if (IsRunningPostponedMessages)
                        {
                            IsRunningPostponedMessages = false;
                            _postAction?.Invoke();
                        }
                        postponedMessage = default;
                        return false;
                    }
                }
            }
        }
    }
}
