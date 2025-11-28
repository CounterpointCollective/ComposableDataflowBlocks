using System.Threading.Tasks.Dataflow;
using CounterpointCollective.DataFlow;

namespace UnitTests.DataFlow
{
    public class PassthroughBlockTests
    {
        [Fact]
        public async Task MessagesAreForwardedToSingleTarget()
        {
            var block = new PassthroughBlock<int>();
            var received = new List<int>();
            var target = new ActionBlock<int>(i => received.Add(i));

            block.LinkTo(target, new DataflowLinkOptions { PropagateCompletion = true });

            block.OfferMessage(new DataflowMessageHeader(1), 42, null, false);
            block.OfferMessage(new DataflowMessageHeader(2), 99, null, false);

            block.Complete();
            await target.Completion;

            Assert.Contains(42, received);
            Assert.Contains(99, received);
            Assert.Equal(2, received.Count);
        }

        [Fact]
        public async Task MultipleSourcesMergedCorrectly()
        {
            var source1 = new BufferBlock<int>();
            var source2 = new BufferBlock<int>();
            var testSubject = new PassthroughBlock<int>();
            var received = new List<int>();
            var target = new ActionBlock<int>(i => received.Add(i));

            testSubject.LinkTo(target, new DataflowLinkOptions() { PropagateCompletion = true });

            source1.LinkTo(testSubject);
            source2.LinkTo(testSubject);
            _ = Task.WhenAll(source1.Completion, source2.Completion).ContinueWith(_ => testSubject.Complete());


            source1.Post(1);
            source2.Post(2);
            source1.Complete();
            source2.Complete();


            await target.Completion;

            Assert.Contains(1, received);
            Assert.Contains(2, received);
            Assert.Equal(2, received.Count);
        }

        private class SourceStub : ISourceBlock<int>
        {
            private readonly TaskCompletionSource _tcsFinishConsume = new();
            private readonly TaskCompletionSource _tcsConsumeStarted = new();

            public void FinishConsume() => _tcsFinishConsume.SetResult();
            public Task ConsumeStarted => _tcsConsumeStarted.Task;


            public int ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<int> target, out bool messageConsumed)
            {
                _tcsConsumeStarted.SetResult();
                _tcsFinishConsume.Task.Wait();
                messageConsumed = true;
                return 1;
            }

            public Task Completion => throw new NotImplementedException();

            public void Complete() => throw new NotImplementedException();
            public void Fault(Exception exception) => throw new NotImplementedException();
            public IDisposable LinkTo(ITargetBlock<int> target, DataflowLinkOptions linkOptions) => throw new NotImplementedException();
            public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<int> target) => throw new NotImplementedException();
            public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<int> target) => throw new NotImplementedException();
        }

        private class TargetStub : ITargetBlock<int>
        {
            private readonly TaskCompletionSource<ISourceBlock<int>?> _tcsSource = new();
            public Task<ISourceBlock<int>?> Source => _tcsSource.Task;

            public Task Completion => throw new NotImplementedException();

            public void Complete() => throw new NotImplementedException();
            public void Fault(Exception exception) => throw new NotImplementedException();
            public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, int messageValue, ISourceBlock<int>? source, bool consumeToAccept)
            {
                _tcsSource.SetResult(source);
                return DataflowMessageStatus.Postponed;

            }

        }

        [Fact]
        public async Task CompletionWaitsForInFlightWrapper()
        {
            var block = new PassthroughBlock<int>();
            var sourceStub = new SourceStub();
            var targetStub = new TargetStub();

            block.LinkTo(targetStub);
            var h = new DataflowMessageHeader(1);

            var r = block.OfferMessage(h, 1, sourceStub, true);
            Assert.Equal(DataflowMessageStatus.Postponed, r);
            var wrappedSource = await targetStub.Source;
            Assert.NotNull(wrappedSource);

            var tConsume = Task.Run(() =>
            {
                wrappedSource.ConsumeMessage(h, targetStub, out var messageConsumed);
                return messageConsumed;
            });

            await sourceStub.ConsumeStarted;
            block.Complete();

            await Task.Delay(100);
            Assert.False(block.Completion.IsCompleted);
            sourceStub.FinishConsume();

            await block.Completion;
            Assert.True(await tConsume);

        }


        [Fact]
        public async Task CanLinkAndUnlinkTargetsDynamically()
        {
            var block = new PassthroughBlock<int>();
            var received = 0;
            var target1 = new ActionBlock<int>(async i =>
            {
                await Task.Yield();
                Interlocked.Increment(ref received);
            });
            var target2 = new ActionBlock<int>(async i =>
            {
                await Task.Yield();
                Interlocked.Increment(ref received);
            });


            var s = Enumerable.Range(1, 100000).AsSourceBlock();
            s.LinkTo(block, new() { PropagateCompletion = true });
            var its = 0;
            while (!target2.Completion.IsCompleted || !target1.Completion.IsCompleted)
            {
                its++;
                var link1 = block.LinkTo(target1, new() { PropagateCompletion = true });
                await Task.Yield();
                var link2 = block.LinkTo(target2, new() { PropagateCompletion = true }); //we let the remainder be offered to target2
                await Task.Yield();
                link1.Dispose();
                await Task.Yield();
                link2.Dispose();
            }

            //by now we'd expect a number of messages to have been offered to target1.


            s.Complete();
            await target1.Completion;
            await target2.Completion;
            Assert.Equal(100000, received);
        }

        [Fact]
        public async Task TestSignificantAmountWithBackPressure()
        {
            var block = new PassthroughBlock<int>();
            var received = 0;
            var target = new ActionBlock<int>(i => Interlocked.Increment(ref received), new ExecutionDataflowBlockOptions { BoundedCapacity = 10 });


            var s = Enumerable.Range(1, 1000).AsSourceBlock();

            s.LinkTo(block, new DataflowLinkOptions() { PropagateCompletion = true });
            var link1 = block.LinkTo(target, new() { PropagateCompletion = true });

            s.Complete();
            await target.Completion;
            Assert.Equal(1000, received);
        }
    }
}
