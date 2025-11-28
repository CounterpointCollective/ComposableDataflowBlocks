using System.Threading.Tasks.Dataflow;
using CounterpointCollective.DataFlow;

namespace UnitTests.DataFlow;

public class OrderPreservingChoiceBlockTests
{
    [Fact]
    public async Task TestOrderPreservingChoiceBlock()
    {
        var isEven = new Predicate<int>(i => i % 2 == 0);

        var prevEven = 0;
        var prevOdd = -1;


        var thenBlock = new TransformBlock<int, int>(async i =>
        {
            var p = Interlocked.Exchange(ref prevEven, i);
            if (p != i - 2)
            {
                throw new Exception($"out of order in then block: {p} followed by {i}");
            }
            if (!isEven(i))
            {
                throw new Exception("should be even");
            }
            await Task.Yield();
            return i;
        });
        var elseBlock = new TransformBlock<int, int>(async i =>
        {
            var p = Interlocked.Exchange(ref prevOdd, i);
            if (p != i - 2)
            {
                throw new Exception($"out of order in else block: {p} followed by {i}");
            }
            await Task.Yield();
            if (isEven(i))
            {
                throw new Exception($"should be odd: {i}");
            }
            return i;
        });

        var testSubject = new OrderPreservingChoiceBlock<int, int>(isEven, thenBlock, elseBlock, new());

        var inputs = Enumerable.Range(1, 50);

        var done = false;
        _ = Task.Run(async () =>
        {
            while (true)
            {
                await Task.Delay(1000);
                if(done)
                {
                    break;
                }
                var t = thenBlock;
                var e = elseBlock;
                var a = testSubject;
            }
        });

        _ = Task.Run(async () =>
        {
            foreach (var i in inputs)
            {
                await testSubject.SendAsync(i);
            }
            testSubject.Complete();
        });

        var res = await testSubject.AsAsyncEnumerable().ToListAsync();
        Assert.Equal(inputs, res);
        done = true;
    }

    [Fact]
    public async Task TestParWithChoice()
    {
        var r = new Random();

        TransformBlock<int, int> ReturnWithDelay() =>
            new(async i =>
            {
                await Task.Delay(r.Next(100));
                return i;
            });

        TransformBlock<int, int> ReturnWithBigDelay() =>
            new(async i =>
            {
                await Task.Delay(r.Next(100, 400));
                return i;
            });

        var thenBlock = ReturnWithDelay();
        var elseBlock = ReturnWithDelay();
        var branch1 = new OrderPreservingChoiceBlock<int, int>(i => i % 2 == 0, thenBlock, elseBlock);

        thenBlock = ReturnWithDelay();
        elseBlock = ReturnWithBigDelay();
        var branch2 = new OrderPreservingChoiceBlock<int, int>(i => i % 2 == 0, thenBlock, elseBlock);

        var p = new[] { branch1, branch2 }.Par();

        for (var i = 0; i < 10; i++)
        {
            await p.SendAsync(i);
        }
        p.Complete();

        var l = await p.AsAsyncEnumerable().ToListAsync();
        Assert.Equal(Enumerable.Range(0, 10).ToList(), l);
    }

    private static async IAsyncEnumerable<int> FaultingNumbers()
    {
        for (var i = 0; i < 40; i++)
        {
            await Task.Delay(1);
            yield return i;
        }
        throw new Exception("fault from source");
    }

    [Fact]
    public async Task SourceFaultGetsPropagaged()
    {
        var thenBlock = new TransformBlock<int, int>(i => i);
        var elseBlock = new TransformBlock<int, int>(i => i);
        var testSubject = new OrderPreservingChoiceBlock<int, int>(t => true, thenBlock, elseBlock);

        var b = FaultingNumbers()
            .AsSourceBlock()
            .LinkTo(testSubject, new() { PropagateCompletion = true });
        var faulted = false;
        try
        {
            await testSubject.AsAsyncEnumerable().ToListAsync();
        }
        catch (Exception)
        {
            faulted = true;
        }
        Assert.True(faulted);
    }


    [Fact]
    public async Task SupportsCancellation()
    {
        var cts = new CancellationTokenSource();
        var thenBlock = new TransformBlock<int, int>(e => e);
        var elseBlock = new TransformBlock<int, int>(e => e);
        var testSubject = new OrderPreservingChoiceBlock<int, int>(e => true, thenBlock, elseBlock, new() { CancellationToken = cts.Token });
        await testSubject.TestCancellationAsync(cts);
    }

    [Fact]
    public async Task GetsPostponedMessages()
    {
        var input = new BufferBlock<int>();

        var thenBlock = new BufferBlock<int>(new() { BoundedCapacity = 1 });
        var elseBlock = new TransformBlock<int, int>(e => e, new());
        var testSubject = new OrderPreservingChoiceBlock<int, int>(i => true, thenBlock, elseBlock);

        input.LinkTo(testSubject, new DataflowLinkOptions() { PropagateCompletion = true });

        var l0 = Enumerable.Range(0, 100).ToList();
        var r = new Random();
        _ = Task.Run(async () =>
        {
            foreach (var i in l0)
            {
                await Task.Delay(r.Next(5));
                input.Post(i);
            }
            input.Complete();
        });

        var l1 = await testSubject.AsAsyncEnumerable().ToListAsync();
        Assert.Equal(l0, l1);
    }

    [Fact]
    public async Task OpsOfDifferentThroughputTest()
    {
        var input = new BufferBlock<int>();
        var r = new Random();
        var thenBlock = new TransformBlock<int, int>(
            async e =>
            {
                await Task.Delay(r.Next(10));
                return e;
            },
            new() { BoundedCapacity = 10 }
        );
        var elseBlock = new TransformBlock<int, int>(
            async e =>
            {
                await Task.Delay(r.Next(40));
                return e;
            },
            new() { BoundedCapacity = 100 }
        );

        for (var i = 0; i < 100; i++)
        {
            Assert.True(input.Post(i));
        }

        var testSubject = new OrderPreservingChoiceBlock<int, int>(
            i => i % 3 == 0,
            thenBlock,
            elseBlock
        );
        input.LinkTo(testSubject, new() { PropagateCompletion = true });
        input.Complete();

        var done = false;
        _ = Task.Run(async () =>
        {
            while (true)
            {
                await Task.Delay(4000);
                var s = testSubject;
                var t = thenBlock;
                var e = elseBlock;
                if (done)
                {
                    break;
                }
            }
        });
        var l = await testSubject.AsAsyncEnumerable().ToListAsync();
        Assert.Equal(Enumerable.Range(0, 100).ToList(), l);
        done = true;
    }

    [Fact]
    public async Task PrematureCancellationThrows()
    {
        var cts = new CancellationTokenSource();
        var thenBlock = new TransformBlock<int, int>(
            e => e,
            new() { CancellationToken = cts.Token }
        );
        var elseBlock = new TransformBlock<int, int>(e => e);
        var testSubject = new OrderPreservingChoiceBlock<int, int>(i => i % 2 == 0, thenBlock, elseBlock);

        cts.Cancel();
        var thrown = false;
        try
        {
            await testSubject.Completion;
        }
        catch
        {
            thrown = true;
        }
        Assert.True(thrown);
    }

    [Fact]
    public async Task PrematureCompletionThrows()
    {
        var completedThenPart = new BufferBlock<int>();
        completedThenPart.Complete();
        var elsePart = new BufferBlock<int>();

        var testSubject = new OrderPreservingChoiceBlock<int, int>(i => i % 2 == 0, completedThenPart, elsePart);
        var thrown = false;
        try
        {
            await testSubject.Completion;
        }
        catch
        {
            thrown = true;
        }
        Assert.True(thrown);
    }

    [Fact]
    public async Task CorrectCompletionOrderDoesNotThrow()
    {
        var thenPart = new BufferBlock<int>();
        var elsePart = new BufferBlock<int>();
        var testSubject = new OrderPreservingChoiceBlock<int, int>(i => i % 2 == 0, thenPart, elsePart);
        testSubject.Complete();
        await thenPart.Completion;
        await elsePart.Completion;
        await testSubject.Completion;
    }

    [Fact]
    public async Task StopsAcceptingMessagesWhenTargetSideFaults()
    {
        var thenPart = new BufferBlock<int>();
        var elsePart = new BufferBlock<int>();
        var testSubject = new OrderPreservingChoiceBlock<int, int>(i => i % 2 == 0, thenPart, elsePart);
        ((IDataflowBlock)thenPart).Fault(new Exception("test"));
        await Task.WhenAny([testSubject.Completion]);
        Assert.False(testSubject.Post(1));
    }

    [Fact]
    public async Task BothBranchesGetFilledUpMaximallyEvenWhenNotConsuming()
    {
        SemaphoreSlim thenRelease = new(0);
        //give then branch a low capacity.
        var thenBranch = new TransformBlock<(bool, int),int>(
            async e => {
                await thenRelease.WaitAsync();
                return e.Item2;
            },
            new  ExecutionDataflowBlockOptions { BoundedCapacity = 1 }
        );
        //give else branch a high capacity
        var elseBranch = new TransformBlock<(bool, int),int>(
            e => e.Item2,
            new ExecutionDataflowBlockOptions { BoundedCapacity = DataflowBlockOptions.Unbounded }
        );

        var testSubject = new OrderPreservingChoiceBlock<(bool Predicate, int Value), int>(
            m => m.Predicate,
            thenBranch,
            elseBranch,
            new() { BoundedCapacity = 1 }
        );


        testSubject.PostAsserted((true, 1));
            //send it to the then branch
        await TestToolExtensions.Eventually(() =>
        {
            Assert.Equal(1, testSubject.ThenBranch.Tokens); //block should be waiting for the then branch to finish.
            Assert.Equal(0, testSubject.Count);  //while the message is parked in the then branch, it should not be counted by the testSubject.
        });

        //Send a bunch to the else branch. Even though the then branch is clogged, it should be all accepted,
        //because the else block has unlimited capacity.
        for (var i = 0; i < 50; i++)
        {
            await testSubject.SendAsync((false, 2));
        }
        await testSubject.SendAsync((true, 1)); //send one more to the then branch, which is clogged.

        await TestToolExtensions.Eventually(() =>
        {
            Assert.Equal(1, testSubject.ThenBranch.Tokens); //then branch is still clogged
            Assert.Equal(1, testSubject.InputCount);
            Assert.Equal(0, testSubject.OutputCount); 
        });

        thenRelease.Release(); //Allow the then branch to process its first message.
        await TestToolExtensions.Eventually(() =>
        {
            Assert.Equal(51, testSubject.OutputCount); //first then branch message + 50 else branch messages
            Assert.Equal(1, testSubject.ThenBranch.Tokens); //then branch clogged by second message
            Assert.Equal(0, testSubject.InputCount);
        });
        Assert.Equal(1, await testSubject.ReceiveAsync()); //We should be reading from the then branch first.
        Assert.True(testSubject.TryReceiveAll(out var items));
        Assert.Equal(50, items.Count);
    }
}

