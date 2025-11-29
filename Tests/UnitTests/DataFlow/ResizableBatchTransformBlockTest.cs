using CounterpointCollective.DataFlow;
using System.Threading.Tasks.Dataflow;

namespace UnitTests.DataFlow
{
    public class ResizableBatchTransformBlockTest
    {
        [Fact]
        public async Task WillRunBatch()
        {
            var testSubject = new ResizableBatchTransformBlock<int, int>(
                async batch => batch,
                2
            );

            testSubject.PostAsserted(1);
            testSubject.PostAsserted(2);
            await TestExtensions.Eventually(() => Assert.Equal(2, testSubject.OutputCount));
        }

        [Fact]
        public async Task TestCounts()
        {
            TaskCompletionSource tcs = new();
            var testSubject = new ResizableBatchTransformBlock<int, int>(
                async batch => {
                    await tcs.Task;
                    return batch;
                },
                2
            );

            testSubject.PostAsserted(1);
            Assert.Equal(1, testSubject.InputCount);

            testSubject.PostAsserted(2);

            await UnitTests.TestExtensions.Eventually(() =>
            {
                Assert.Equal(2, testSubject.Count);
                Assert.Equal(0, testSubject.OutputCount);
                Assert.Equal(0, testSubject.InputCount);
                Assert.Equal(2, testSubject.InProgressCount);
            });
            tcs.SetResult();

            await TestExtensions.Eventually(() => {
                Assert.Equal(2, testSubject.OutputCount);
                Assert.Equal(2, testSubject.Count);
                Assert.Equal(0, testSubject.InputCount);
            });

        }

        [Fact]
        public async Task StopsAcceptingAfterFaulting()
        {
            var testSubject = new ResizableBatchTransformBlock<int, int>(
                async batch =>
                {
                    await Task.Delay(10);
                    throw new Exception();
                },
                10
            );

            var s = Enumerable.Range(1, 100).AsSourceBlock();
            s.LinkTo(testSubject, new DataflowLinkOptions() { PropagateCompletion = false });
            var faulted = false;
            try
            {
                await testSubject.Completion;
            }
            catch
            {
                faulted = true;
            }
            Assert.True(faulted);

            Assert.False(await testSubject.SendAsync(1));
        }

        [Fact]
        public async Task CanRunBatchAfterBatchCompleted()
        {
            var testSubject = new ResizableBatchTransformBlock<int, int>(
                async batch =>
                {
                    await Task.Delay(10);
                    return batch;
                },
                2
            );

            var inputBlock = new BufferBlock<int>();
            inputBlock.LinkTo(testSubject, new() { PropagateCompletion = true });
            for (var i = 0; i < 10; i++)
            {
                inputBlock.PostAsserted(i);
            }
            inputBlock.Complete();

            var expected = Enumerable.Range(0, 10).ToHashSet();
            for (var i = 0; i < 10; i++)
            {
                Assert.True(expected.Remove(await testSubject.ReceiveAsync()));
            }
            Assert.Empty(expected);
            await testSubject.Completion;
        }

        [Fact]
        public async Task RunsRemainingItemsAfterCompletionRequested()
        {
            var testSubject = new ResizableBatchTransformBlock<int, int>(
                async batch =>
                {
                    await Task.Delay(10);
                    return batch;
                },
                2
            );

            await testSubject.SendAsync(1);
            await testSubject.SendAsync(2);
            await testSubject.ReceiveAsync();
            //the first batch is not finished yet now, because there is 1 item remaining.
            //but the block can accept a new message.
            await testSubject.SendAsync(3);
            testSubject.Complete();
            await Task.Delay(100); //give it some time to swallow. Completion should not stop it from processing the final incomplete batch.

            await testSubject.ReceiveAsync(); //now the first batch is finished.            
            await testSubject.ReceiveAsync(); //it should now process the final incomplete batch.
            await testSubject.Completion;
        }

        [Fact]
        public async Task CanCancel()
        {
            using var cts = new CancellationTokenSource();
            var testSubject = new ResizableBatchTransformBlock<int, int>(
                async batch =>
                {
                    await Task.Delay(1000);
                    return batch;
                },
                2,
                new() { CancellationToken = cts.Token }
            );

            var inputBlock = new BufferBlock<int>();
            inputBlock.LinkTo(testSubject, new() { PropagateCompletion = true });
            for (var i = 0; i < 10; i++)
            {
                inputBlock.PostAsserted(i);
            }
            inputBlock.Complete();
            await Task.Delay(10); //give the transform function a chance to start.
            cts.Cancel();
            await testSubject.Completion.ContinueWith(t => Assert.True(t.IsCanceled));
        }

        [Fact]
        public async Task WillOnlyCompleteWhenQueueIsEmpty()
        {
            var testSubject = new ResizableBatchTransformBlock<int, int>(
            async batch =>
            {
                await Task.Delay(1000);
                return batch;
            },
                2
            );

            var output = new BufferBlock<int>();
            testSubject.LinkTo(output, new() { PropagateCompletion = true });

            await testSubject.SendAsync(1);
            await testSubject.SendAsync(2); //Will start a batch.

            await testSubject.SendAsync(3); //Batch has now started.
            testSubject.Complete();         //Should not go immediately, because first the second batch needs to run.
            await testSubject.Completion;
            Assert.True(output.Count == 3);
        }

        [Fact]
        public async Task WillProcessPostponedMessagesAfterBatchSizeGrowth()
        {
            var source = new BufferBlock<int>();
            for (var i = 0; i < 20; i++)
            {
                source.Post(i);
            }

            var testSubject = new ResizableBatchTransformBlock<int, int>(
                async batch =>
                {
                    await Task.Delay(1);
                    return batch;
                },
                2
            );
            source.LinkTo(testSubject);

            //Wait for the first batch to have been processed.
            while(testSubject.OutputCount < 2)
            {
                await Task.Delay(2);
            }

            testSubject.BatchSize = 10;
            while (testSubject.OutputCount < 10)
            {
                await Task.Delay(2);
            }
        }

        [Fact]
        public async Task WillTryRunBatchAfterBatchSizeShrinkage()
        {
            var source = new BufferBlock<int>();
            for (var i = 0; i < 5; i++)
            {
                source.Post(i);
            }

            var testSubject = new ResizableBatchTransformBlock<int, int>(
                async batch =>
                {
                    await Task.Delay(1);
                    return batch;
                },
                10
            );
            source.LinkTo(testSubject);

            while (testSubject.InputCount < 5)
            {
                await Task.Delay(2);
            }

            testSubject.BatchSize = 5;
            while (testSubject.OutputCount < 5)
            {
                await Task.Delay(2);
            }
        }

    }
}
