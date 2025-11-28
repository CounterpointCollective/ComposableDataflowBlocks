using CounterpointCollective.DataFlow.Encapsulation;
using System;
using System.Threading.Tasks.Dataflow;
namespace CounterpointCollective.DataFlow
{
    public class TokenGateBlock<I> : AbstractEncapsulatedTargetBlock<I>, ITargetBlock<I>
    {
        protected override ITargetBlock<I> TargetSide => _inner;
        protected override IDataflowBlock CompletionSide => _inner;

        private readonly AdmissionGateBlock<I> _inner;
        private readonly State _state;
        private readonly Action<int>? _onTokenSpent;

        public TokenGateBlock(ITargetBlock<I> inner, Action<int>? onTokenSpent = null)
        {
            _state = new(this);
            _onTokenSpent = onTokenSpent;
            var hooks = new AdmissionGateHooks()
            {
                MayTryToEnter = () => _state.TryReservePendingPayment(),
                Entering = () => _state.CommitPendingPayment(true),
                FailingToEnter = () => _state.CommitPendingPayment(false)
            };
            _inner = new AdmissionGateBlock<I>(inner, hooks);
        }

        /// <summary>
        /// Allow count new messages to be delivered (either synchronously or asynchronously) by the linked targets.
        /// /</summary>
        public void Allow(int count = 1) => _state.Allow(count);

        /// <summary>
        /// Current amount of tokens still available.
        /// </summary>
        public int Tokens => _state.Tokens;

        private class State
        {
            public State(TokenGateBlock<I> outer) => _outer = outer;

            private object Lock { get; } = new();
            private readonly TokenGateBlock<I> _outer;

            public int Tokens { get; private set; }

            private int reserved;
            private int FreeTokens => Tokens - reserved;

            /// <summary>
            /// Must be followed by a call to CommitPendingPayment.
            /// </summary>
            public bool TryReservePendingPayment()
            {
                lock (Lock)
                {
                    if (FreeTokens > 0)
                    {
                        reserved++;
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                }
            }

            public void CommitPendingPayment(bool consume)
            {
                lock (Lock)
                {
                    reserved--;
                    if (consume)
                    {
                        Tokens--;
                    } else
                    if (FreeTokens == 1)
                    {
                        _outer._inner.ProcessPostponedMessages();
                    }
                }
                if (consume)
                { 
                    _outer._onTokenSpent?.Invoke(Tokens);
                }
            }

            public void Allow(int i = 1)
            {
                lock (Lock)
                {
                    var didnt = FreeTokens == 0;
                    Tokens += i;
                    if (didnt)
                    {
                        _outer._inner.ProcessPostponedMessages();
                    }
                }
            }
        }
    }
}
