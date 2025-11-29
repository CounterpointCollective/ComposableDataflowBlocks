using CounterpointCollective.DataFlow;
using CounterpointCollective.DataFlow.Notifying;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow.Notifying
{
    public class SourceBlockNotificationTests
    {
        [Fact]
        public async Task TestSourceBlockNotification()
        {
            var i = 0;

            void OnMessage() => i++;

            var b = new BufferBlock<int>().WithNotification(h => h.OnDeliveringMessages = _ => OnMessage());
            await b.SendAsync(1);
            await b.SendAsync(2);
            b.Complete();

            Assert.Equal(0, i);

            var t = new TransformBlock<int, int>(
                async e =>
                {
                    await Task.Delay(100);
                    return e;
                },
                new ExecutionDataflowBlockOptions() { BoundedCapacity = 1 }
            );

            var o = t.Buffer();

            b.LinkTo(t, new DataflowLinkOptions() { PropagateCompletion = true });
            await b.Completion;
            Assert.Equal(2, i);
            await t.Completion;
            Assert.Equal(2, o.Count);
        }

        [Fact]
        public async Task TestSynchronousMode()
        {
            var i = 0;
            AutoResetEvent are = new(false);

            void OnMessage(DeliveringMessagesEvent msgs)
            {
                are.WaitOne(); //Make it block; easier for testing async mode.
                i += msgs.Count;
            }


            var b = new BufferBlock<int>().WithNotification(h => {
                h.OnDeliveringMessages = msgs => OnMessage(msgs);
                h.NotifySynchronously = true;
                ;
            });

            await b.SendAsync(1);
            await b.SendAsync(2);

            var target1 = new BufferBlock<int>();
            var linkTask = Task.Run(() => b.LinkTo(target1, new DataflowLinkOptions() { PropagateCompletion = true }));

            b.Complete();
            await Task.Delay(10);

            Assert.Equal(0, i); //should be blocked.
            Assert.False(linkTask.IsCompleted); //should be blocked.
            Assert.False(b.Completion.IsCompleted); //should be blocked.

            are.Set(); //Unblock
            await linkTask;
            Assert.Equal(1, i);//should have been done synchronously with setting up the link.

            are.Set(); //Unblock for the second event

            await b.Completion;
            Assert.Equal(2, i);
        }

        [Fact]
        public async Task TestAsyncMode()
        {
            var i = 0;
            AutoResetEvent are = new(false);

            void OnMessage(DeliveringMessagesEvent msgs)
            {
                are.WaitOne(); //Make it block; easier for testing async mode.
                i += msgs.Count;
            }


            var b = new BufferBlock<int>().WithNotification(h => {
                h.OnDeliveringMessages = msgs => OnMessage(msgs);
                h.NotifySynchronously = false;
                ;
            });

            for (var n = 0; n < 1000; n++)
            {
                await b.SendAsync(n);
                await b.ReceiveAsync();
            }

            b.Complete();
            //we now have 1000 delivery events, and they are blocked. But we just proved that the block can accept new messages.
            Assert.False(b.Completion.IsCompleted);
            var prevI = i;
            var numberOfBatches = 0;
            while (i < 1000)
            {
                Assert.False(b.Completion.IsCompleted);
                are.Set(); //Unblock the first delivery
                await UnitTests.TestExtensions.Eventually(() => Assert.True(i > prevI));
                prevI = i;
                numberOfBatches++;
            }
            Assert.True(numberOfBatches <= 2); //we expect the clogging to make the delivery system batch up events.
            await b.Completion;
            await TestExtensions.Eventually(() => Assert.Equal(1000, i));
        }
    }
}
