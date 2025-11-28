using System.Threading.Tasks.Dataflow;
using CounterpointCollective.DataFlow;
using CounterpointCollective.DataFlow.Encapsulation;
using CounterpointCollective.DataFlow.Notifying;
using Microsoft.Graph;
using Schema.NET;

namespace UnitTests.DataFlow
{
    public class TokenGateBlockTest
    {
        [Fact]
        public async Task TestLinkWithTokensWillDeliver()
        {
            var s = Enumerable.Range(1, 1000).AsSourceBlock();
            var b = new BufferBlock<int>();
            var testSubject = new TokenGateBlock<int>(b);
            s.LinkTo(testSubject, new DataflowLinkOptions { PropagateCompletion = true });
            testSubject.Allow();
            await b.OutputAvailableAsync();
        }

        [Fact]
        public async Task TestTokenAfterLinkWillDeliver()
        {
            var s = Enumerable.Range(1, 1000).AsSourceBlock();
            var b = new BufferBlock<int>();
            var testSubject = new TokenGateBlock<int>(b);
            s.LinkTo(testSubject, new DataflowLinkOptions { PropagateCompletion = true });

            var t = b.OutputAvailableAsync();
            await Task.Delay(100); //give some time to see if it delivers without token
            Assert.False(t.IsCompleted);
            testSubject.Allow();
            await t;
        }

        [Fact]
        public async Task TestDeliveryWithDelays()
        {
            var s = Enumerable.Range(1, 1000).AsSourceBlock();
            var gatedBuffer = new BufferBlock<int>();
            var testSubject = new TokenGateBlock<int>(gatedBuffer);
            s.LinkTo(testSubject, new DataflowLinkOptions { PropagateCompletion = true });

            var b = new BufferBlock<int>();

            gatedBuffer.LinkTo(b, new DataflowLinkOptions { PropagateCompletion = true });
            _ = Task.Run(async () =>
            {
                for (var i = 0; i < 500; i++)
                {
                    await Task.Yield();
                    testSubject.Allow(2);
                }
            });

            await testSubject.Completion;
            Assert.Equal(1000, b.Count);
            Assert.True(testSubject.Tokens == 0);
        }

        [Fact]
        public async Task TestSlowTarget()
        {
            var target = new BufferBlock<int>(new DataflowBlockOptions { BoundedCapacity = 1 });
            var total = 1000 * 1000;

            var s = Enumerable.Range(1, total).AsSourceBlock();
            var gatedBuffer = new BufferBlock<int>();
            var testSubject = new TokenGateBlock<int>(gatedBuffer);
            s.LinkTo(testSubject, new DataflowLinkOptions { PropagateCompletion = true });
            gatedBuffer.LinkTo(target, new DataflowLinkOptions { PropagateCompletion = true });

            var i = 0;
            var tokensGiven = 0;

            _ = Task.Run(async () =>
            {
                while (true)
                {
                    await Task.Delay(1000);
                    var t = testSubject;
                    var t2 = target;
                    var g = tokensGiven;
                    var j = i;
                }
            });

            while (i < total)
            {
                if (testSubject.Tokens == 0 && tokensGiven < total)
                {
                    var t = Math.Min(total - tokensGiven, 10);
                    tokensGiven += t;
                    testSubject.Allow(t);
                }
                await target.OutputAvailableAsync();
                if (target.TryReceive(out var _))
                {
                    i++;
                }
            }

            await target.Completion;
            Assert.Equal(total, i);

            Assert.Equal(i, tokensGiven);

            if (testSubject.Tokens > 0)
            {
                var t = testSubject;
            }
            Assert.False(testSubject.Tokens > 0);
        }
    }
}
