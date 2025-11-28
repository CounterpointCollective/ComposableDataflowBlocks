using CounterpointCollective.DataFlow;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow
{
    public class BoundedBlockTests
    {
        [Fact]
        public async Task ShowCaseBoundedPropagatorBlock()
        {
            //First we create a DataflowBlock ourselves, composed of multiple subblocks.
            var b = new BufferBlock<int>(new() { BoundedCapacity = DataflowBlockOptions.Unbounded });
            var t = new TransformBlock<int, int>(async i =>
            {
                await Task.Yield(); //simulate some work.
                return i + 1;
            }, new() { BoundedCapacity = DataflowBlockOptions.Unbounded });
            b.LinkTo(t, new DataflowLinkOptions() { PropagateCompletion = true });
            var ourOwnDataflowBlock = DataflowBlock.Encapsulate(b, t);


            //Now we want to
            // - put a boundedCapacity on our new block and
            // - be able to count how many items are contained with it, at any given time
            //This is what we will use a BoundedPropagatorBlock for.

            var testSubject = new BoundedPropagatorBlock<int,int>(ourOwnDataflowBlock, boundedCapacity: 2000);
            //Thus we enabled a bounded capacity of 2000 messasge, and realtime counting on our own custom DataflowBlock!

            Assert.Equal(0, testSubject.Count);

            //we should be able to push synchronously up to the bounded capacity.
            for (var i = 0; i < 2000; i++)
            {
                Assert.True(testSubject.Post(i));
                Assert.Equal(i + 1, testSubject.Count); //count is administered properly
            }

            //now we have hit the boundedCapacity.
            Assert.Equal(2000, testSubject.Count);
            Assert.False(testSubject.Post(2001)); //should be rejected.

            for (var i = 1; i < 100; i++)
            {
                var r = await testSubject.ReceiveAsync(); //receive one message, making space for one more.
                Assert.Equal(i, r);
                Assert.Equal(2000 - i, testSubject.Count); //also when consuming messages, count is administered properly
            }

            testSubject.Post(2000); //Now that there is space we can post again.

            //IReceivableSourceBlock methods works here, because we put a TransformBlock at the end, which is an
            //IReceivableSourceBlock.
            await testSubject.OutputAvailableAsync();
            var beforeCount = testSubject.Count;
            Assert.True(testSubject.TryReceiveAll(out var items));
            Assert.Equal(beforeCount, testSubject.Count + items.Count);
        }

        [Fact]
        public async Task ShowCaseBoundedTargetBlock()
        {
            //we can also use BoundedTargetBlock to put a boundedCapacity and count items in a "target only" block.
            //For example:

            var composeEmailBlock = new TransformBlock<string, object>(async userName =>
            {
                await Task.Yield(); //simulate some work.
                return new { UserName = userName, EmailAddress = "email@address", Body = "Hello!11" };
            }, new());

            using SemaphoreSlim semAllowedToSendEmail = new(0);
            using SemaphoreSlim semEmailIsSent = new(0);
            List<Action<object>> sendEmailActions = [];

            var sendMailBlock = new ActionBlock<object>(async email =>
            {
                await semAllowedToSendEmail.WaitAsync();
                foreach (var a in sendEmailActions)
                {
                    a(email);
                }
                semEmailIsSent.Release();
            });
            composeEmailBlock.LinkTo(sendMailBlock, new DataflowLinkOptions() { PropagateCompletion = true });

            //Now suppose we want to limit how many emails are being handled at any given time.
            var boundedEmailBlock = new BoundedTargetBlock<string>(composeEmailBlock, boundedCapacity: 100);
            //Messages entering the composeEmailBlock will now be automatically counted.
            //However, we still need to ensure that when an email is sent, the count is decremented. We can use
            // the AdjustCount method for that.
            sendEmailActions.Add(email => boundedEmailBlock.AdjustCount(-1));


            for (var i = 0; i < 100; i++)
            {
                Assert.True(boundedEmailBlock.Post($"User{i}"));
                Assert.Equal(i + 1, boundedEmailBlock.Count); //count is administered properly
            }

            //We have hit the boundedCapacity.
            Assert.False(boundedEmailBlock.Post($"User{101}"));

            //Now let's allow some emails to be sent.
            semAllowedToSendEmail.Release(10);
            for (var i = 0; i < 10; i++)
            {
                await semEmailIsSent.WaitAsync();
            }
            Assert.Equal(90, boundedEmailBlock.Count); //count is administered properly
        }
    }
}
