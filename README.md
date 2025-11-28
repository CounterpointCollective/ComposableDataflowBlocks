# ComposableDataflowBlocks

ComposableDataflowBlocks is a library designed to simplify the construction DataflowBlock pipelines.
It provides a collection of modular, reusable blocks for processing and transforming data, enabling developers to build complex workflows 
with composability, scalability, and clarity.

## Purpose

The goal of ComposableDataflowBlocks is to accelerate development of robust data pipelines, workflow engines, and real-time processing 
applications. By offering ready-to-use, extensible blocks, the library empowers users to define custom data transformations, 
branching logic, event handling, and more, all while maintaining a clean and maintainable codebase.

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
s
###BoundedPropagatorBlock Example
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

//Thus we enabled a bounded capacity of 2000 messasge, and realtime counting on our own custom DataflowBlock!

Assert.Equal(0, testSubject.Count);

//we should be able to push synchronously up to the bounded capacity.
for (var i = 0; i < 2000; i++)
{
    Assert.True(testSubject.Post(i));
    Assert.Equal(i + 1, testSubject.Count); //count is administered properly
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

