using CounterpointCollective.Threading;

namespace UnitTests.Threading;

public class AsyncLazyTests
{
    [Fact]
    public async Task GetValueAsync_ReturnsResult_WhenTaskCompletesSuccessfully()
    {
        var lazy = new AsyncLazy<int>(async ct =>
        {
            await Task.Delay(50, ct);
            return 42;
        });

        var result = await lazy.GetValueAsync();
        Assert.Equal(42, result);
    }

    [Fact]
    public async Task GetValueAsync_CanBeCalledMultipleTimes_ReturnsSameValue()
    {
        var count = 0;
        var lazy = new AsyncLazy<int>(async ct =>
        {
            await Task.Delay(50, ct);
            return Interlocked.Increment(ref count);
        });

        var r1 = await lazy.GetValueAsync();
        var r2 = await lazy.GetValueAsync();
        Assert.Equal(r1, r2);
        Assert.Equal(1, r1); // factory only called once
    }

    [Fact]
    public async Task Reset_StartsNewInitialization()
    {
        var count = 0;
        var lazy = new AsyncLazy<int>(async ct =>
        {
            await Task.Delay(50, ct);
            return Interlocked.Increment(ref count);
        });

        var r1 = await lazy.GetValueAsync();
        lazy.Reset();
        var r2 = await lazy.GetValueAsync();

        Assert.NotEqual(r1, r2);
        Assert.Equal(1, r1);
        Assert.Equal(2, r2);
    }

    [Fact]
    public async Task GetValueAsync_CancellationToken_CancelsWaitingCall()
    {
        var lazy = new AsyncLazy<int>(async ct =>
        {
            await Task.Delay(500, ct);
            return 42;
        });

        using var cts = new CancellationTokenSource(100); // cancel after 100ms

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await lazy.GetValueAsync(cts.Token));
    }

    [Fact]
    public async Task GetValueAsync_CancellingWontStopInitialization()
    {
        var count = 0;
        var lazy = new AsyncLazy<int>(async ct =>
        {
            await Task.Delay(500, ct);
            return Interlocked.Increment(ref count);
        });

        using var cts = new CancellationTokenSource(100); // cancel after 100ms

        await Task.WhenAny(lazy.GetValueAsync(cts.Token).AsTask());
        await Task.Delay(600); // wait enough time for initialization to complete
        Assert.Equal(1, await lazy.GetValueAsync()); // initialization completed despite cancellation
    }

    [Fact]
    public async Task GetValueAsync_ResetAfterCancellationIsRespected()
    {
        var count = 0;
        var lazy = new AsyncLazy<int>(async ct =>
        {
            await Task.Delay(500, ct);
            return Interlocked.Increment(ref count);
        });

        using var cts = new CancellationTokenSource(100);

        await Task.WhenAny(lazy.GetValueAsync(cts.Token).AsTask());
        await Task.Delay(600);
        lazy.Reset();
        Assert.Equal(2, await lazy.GetValueAsync());
    }

    [Fact]
    public async Task ResetDuringInitialization_CancelsTaskAndStartsNew()
    {
        var started = 0;
        var lazy = new AsyncLazy<int>(async ct =>
        {
            Interlocked.Increment(ref started);
            await Task.Delay(200, ct);
            return started;
        });

        var t1 = lazy.GetValueAsync();
        await Task.Delay(50); // let initialization start
        lazy.Reset();
        var t2 = lazy.GetValueAsync();

        var r2 = await t2;
        Assert.Equal(2, r2); // new task
    }

    [Fact]
    public async Task FaultedFactory_ThrowsException()
    {
        var lazy = new AsyncLazy<int>(ct => Task.FromException<int>(new InvalidOperationException("fail")));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () => await lazy.GetValueAsync());
        Assert.Equal("fail", ex.Message);
    }
}
