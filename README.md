# ComposableDataflowBlocks

ComposableDataflowBlocks is a library designed to simplify the construction DataflowBlock pipelines.
It provides a collection of modular, reusable blocks for processing and transforming data, enabling developers to build complex workflows 
with composability, scalability, and clarity.

## Purpose

The ComposableDataflowBlocks library exists to make building complex, high-performance data processing pipelines in .NET easier, safer, 
and more flexible. It extends TPL Dataflow with a set of modular building blocks that solve real-world problems developers frequently face
but that the base library does not address, such as bounding, dynamic resizing, priority handling, batching, and adaptive throughput 
control.

By wrapping and enhancing existing blocks (including user-composed ones), the library lets developers add sophisticated behavior without
rewriting pipelines.


In short, the library helps you construct powerful, adaptable, and production-grade Dataflow solutions with minimal friction, while staying 
fully compatible with the familiar TPL Dataflow model.

## Features

- **BoundedBlocks:** Allow adding bounded capacity and real-time item counting to any Dataflow propagator block, including user-composed
or encapsulated blocks, enabling true compositionality.
- **ResizableBlocks:** Dynamically adjusts the bounded capacity of any block at runtime.
- **AutoScalingBlock:** Automatically determines and optimizes batch sizes at runtime by measuring throughput, ensuring efficient processing 
without manual tuning.
- **OrderPreservingChoiceBlock:** Routes items to different targets based on a predicate while preserving the original order of the items.
- **ParallelBlock:** Sends messages to multiple blocks in parallel and recombines the results, facilitating concurrent processing.
- **GroupAdjacentBlock:** Groups consecutive items that share a key or predicate into batches, simplifying aggregation and batch processing.
- **PriorityBufferBlock:** Delivers highest-priority messages first, dynamically reordering when new higher-priority items arrive.

## Installation

Clone the repository:
```sh
git clone https://github.com/CounterpointCollective/ComposableDataflowBlocks.git
```
Install dependencies (if any):
```sh
# Example for .NET/C# projects
dotnet restore
```

## Usage Example

### Example 1: Bounding Your Own Composed Block

```csharp
//Example of using a BoundedPropagatorBlock to add bounded capacity and item counting

using CounterpointCollective.DataFlow;

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

//Thus we enabled a bounded capacity of 2000 messages, and real-time counting on our own custom DataflowBlock!

Assert.Equal(0, testSubject.Count);

//we should be able to push synchronously up to the bounded capacity.
for (var i = 0; i < 2000; i++)
{
    Assert.True(testSubject.Post(i));
    Assert.Equal(i + 1, testSubject.Count); //count is administered properly
}
```

### Example 2: Making any DataflowBlock dynamically resizable
```csharp
//Example showing that you can dynamically resize the bounded capacity of any block by wrapping it into a BoundedPropagatorBlock
var bufferBlock = new BufferBlock<int>(new() { BoundedCapacity = DataflowBlockOptions.Unbounded });
var dynamicBufferBlock = new BoundedPropagatorBlock<int,int>(bufferBlock);

//We did not specify a bounded capacity, so it defaults to DataflowBlockOptions.Unbounded

Assert.True(dynamicBufferBlock.Post(1));

//But we can dynamically set the bounded capacity at any point.
dynamicBufferBlock.BoundedCapacity = 2;
Assert.True(dynamicBufferBlock.Post(2));
Assert.False(dynamicBufferBlock.Post(3));

dynamicBufferBlock.BoundedCapacity = 3;
Assert.True(dynamicBufferBlock.Post(3));
```

### Example 3: Automatically optimizing batch sizes in real-time
```csharp
// Example: Using AutoScaling on a ResizableBatchTransformBlock
// For demonstration: assume our workload performs best when batch size is ~100 items.
async Task<IEnumerable<int>> ProcessBatch(int[] batch)
{
    var distanceFromOptimal = Math.Abs(batch.Length - 100);
    await Task.Delay(distanceFromOptimal * batch.Length); // Simulate slower processing when batch isn't optimal
    return batch;
}

// Create a ResizableBatchTransformBlock using our ProcessBatch function.
var testSubject = new ResizableBatchTransformBlock<int, int>(
    ProcessBatch,
    initialBatchSize: 1,
    new ExecutionDataflowBlockOptions { BoundedCapacity = 10000 }
);

// Batch size can be manually adjusted:
testSubject.BatchSize = 5;

// Or automatically optimized using AutoScaling:
testSubject.EnableAutoScaling(
    new DefaultBatchSizeStrategy(minBatchSize: 1, maxBatchSize: 200)
);

// Send some work:
for (var i = 0; i < 10000; i++)
{
    await testSubject.SendAsync(i);
}

// Process outputs while AutoScaling gradually converges toward the optimal batch size (~100).
for (var i = 0; i < 10000; i++)
{
    var result = await testSubject.ReceiveAsync();
}
```

## Language Composition

_ComposableDataflowBlocks_ is written in **C#**.

## Contributing

Contributions are welcome! If you'd like to suggest improvements, report bugs, or contribute new dataflow blocks, please open an issue or submit a pull request.

1. Fork the repo.
2. Create your feature branch (`git checkout -b feature/MyFeature`).
3. Commit your changes (`git commit -am 'Add new feature'`).
4. Push to the branch (`git push origin feature/MyFeature`).
5. Open a Pull Request.

## License

Distributed under the MIT License. See `LICENSE` for more information.

## Resources

- [Issues](https://github.com/CounterpointCollective/ComposableDataflowBlocks/issues)
- [Pull Requests](https://github.com/CounterpointCollective/ComposableDataflowBlocks/pulls)

