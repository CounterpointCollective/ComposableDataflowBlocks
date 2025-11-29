using CounterpointCollective.DataFlow;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow
{
    public class ResizableBufferBlockTests
    {
        [Fact]
        public async Task TestResizableBufferBlock()
        {
            var b = new ResizableBufferBlock<int>(new() { BoundedCapacity = 1 });
            Assert.True(b.Post(1));
            Assert.False(b.Post(2));

            b.BoundedCapacity = 2;
            await b.SendAsync(2);

            b.BoundedCapacity = 1;
            await Task.Delay(100);
            Assert.False(b.Post(3));
            await b.ReceiveAsync();
            await b.ReceiveAsync();

            await b.SendAsync(1);
            await Task.Delay(100);
            Assert.False(b.Post(0));
            b.BoundedCapacity = 3;

            await b.SendAsync(2);
            await b.SendAsync(3);
            await Task.Delay(100);
            Assert.False(b.Post(4));
        }

        [Fact]
        public async Task TestResizableBufferBlockPicksUpPostponedMessages()
        {
            var input = new BufferBlock<int>();

            var testSubject = new ResizableBufferBlock<int>(new() { BoundedCapacity = 1 });
            input.LinkTo(testSubject, new DataflowLinkOptions { PropagateCompletion = true });
            input.Post(1);
            input.Post(2);
            while (input.Count > 1)
            {
                await Task.Delay(10);
            }
            Assert.Equal(1, testSubject.Count);
            testSubject.BoundedCapacity = 2;
            Assert.Equal(1, testSubject.Receive());
            Assert.Equal(2, await testSubject.ReceiveAsync());
            Assert.Equal(0, testSubject.Count);
        }

        [Fact]
        public void UnboundedNeverPostpones()
        {
            var testSubject = new ResizableBufferBlock<int>(new() { BoundedCapacity = -1 });
            testSubject.LinkTo(new BufferBlock<int>(new DataflowBlockOptions { BoundedCapacity = 10 }));
            for (var i = 0; i < 1000; i++)
            {
                Assert.True(testSubject.Post(i));
            }
        }

        [Fact]
        public async Task SupportsCancellation()
        {
            var cts = new CancellationTokenSource();
            var testSubject = new ResizableBufferBlock<int>(new() { CancellationToken = cts.Token });
            await testSubject.TestCancellationAsync(cts);
        }

        [Fact]
        public async Task CheckCount()
        {
            var testSubject = new ResizableBufferBlock<int>(new());
            var c = 0;
            var r = new Random();
            for (var i = 0; i < 100; i++)
            {
                if (c == 0)
                {
                    Assert.True(testSubject.Post(i));
                    c++;
                }
                if (r.NextDouble() < .5)
                {
                    Assert.True(testSubject.Post(1));
                    c++;
                }
                else
                {
                    await testSubject.ReceiveAsync();
                    c--;
                }
                Assert.Equal(c, testSubject.Count);
            }
        }

        [Fact]
        public async Task CheckCountAsynchronously()
        {
            var testSubject = new ResizableBufferBlock<int>(new() { BoundedCapacity = 25 });
            var c = 0;
            var r = new Random();
            for (var i = 0; i < 100; i++)
            {
                if (c == 0)
                {
                    await testSubject.SendAsync(i);
                    c++;
                } else if (c == 25)
                {
                    await testSubject.ReceiveAsync();
                    c--;
                } else if (r.NextDouble() < .5)
                {
                    await testSubject.SendAsync(i);
                    c++;
                }
                else
                {
                    await testSubject.ReceiveAsync();
                    c--;
                }
                Assert.Equal(c, testSubject.Count);
            }
        }
    }
}
