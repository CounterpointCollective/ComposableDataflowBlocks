using ComposableDataflowBlocks.CounterpointCollective.Utilities;
using CounterpointCollective.Utilities;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;

namespace CounterpointCollective.DataFlow
{
    public record BatchRunEvent<T>(ResizableBatchTransformBlock<T, T> Source, int BatchSize, double RunMillis, int NextBatchSize);

    public static class AutoScalingBlockExtensions
    {
        public static bool DebugBeeps { get; set; }

        private static int c;

        [SuppressMessage("Interoperability", "CA1416:Validate platform compatibility", Justification = "Non critical")]
        public static void EnableAutoScaling<T>(
            this ResizableBatchTransformBlock<T, T> block,
            IBatchSizeStrategy batchSizeStrategy,
            Action<BatchRunEvent<T>>? onBatchRun = null
        )
        {
            var i = c++;
            var initialBatchSize = batchSizeStrategy.BatchSize;
            block.BatchSize = initialBatchSize;

            block.OnBatch = (_, batch) =>
            {
                var sw = new Stopwatch();

                if (DebugBeeps)
                {
                    Console.Beep(490 + (i * 50), 100);
                }

                sw.Start();

                return new ActionDisposable(() =>
                {
                    var lag = sw.Elapsed.TotalMilliseconds;
                    var newBatchSize = batchSizeStrategy.UpdateBatchSize(batch.Count, lag);
                    block.BatchSize = newBatchSize;

                    onBatchRun?.Invoke(new(block, batch.Count, lag, newBatchSize));
                });

            };
        }
    }


    public record BatchSizeCalculation
    {
        public int MinBatchSize { get; set; }
        public int MaxBatchSize { get; set; }

        public int AllowedBatchSizeRange => MaxBatchSize - MinBatchSize;

        public int? OldBatchSize { get; set; }
        public int DeltaBatch { get; set; }
        public double DeltaThroughput { get; set; }
        public double DeltaThroughputPerDeltaBatch =>
            DeltaBatch == 0 ? 0 : DeltaThroughput / DeltaBatch;
#pragma warning disable CA1822 // Mark members as static
        public double LearningFactor => 0.01;
#pragma warning restore CA1822 // Mark members as static

        public double ScaledAdjustment => LearningFactor * DeltaThroughputPerDeltaBatch * AllowedBatchSizeRange;

        public double S1Calculated { get; set; }
        public double S2Allowed { get; set; }
        public double S3FromLowPass { get; set; }
        public int S4Clamped => (int)Math.Clamp(S3FromLowPass, MinBatchSize, MaxBatchSize);
        public double? S5EnsureChangedBatchSize { get; set; }
        public IEnumerable<double> LowPassBuffer { get; set; } = [];
        public int NewBatchSize { get; set; }
    }

    public interface IBatchSizeStrategy
    {
        public int UpdateBatchSize(int batchSize, double totalRunMillis);
        public int BatchSize { get; }

        public object DebugView { get; }
    }

    public class DefaultBatchSizeStrategy(int minBatchSize = 1, int maxBatchSize = 50, int initialBatchSize = 50, int maxQueryTimeSeconds = 60): IBatchSizeStrategy
    {
        private record BatchStat(int BatchSize, double TotalRunMillis)
        {
            public double Throughput => BatchSize * 1000 / (TotalRunMillis + 1);
        }


        private readonly Random _rndm = new();
        private BatchStat? prevBatchStat;
        private readonly LowPassFilter _lowPassFilter = new(5, initialBatchSize);

        public int BatchSize { get; private set; } = initialBatchSize;

        object IBatchSizeStrategy.DebugView => DebugView;

        public BatchSizeCalculation DebugView { get; private set; } = new BatchSizeCalculation();

        public int UpdateBatchSize(int batchSize, double totalRunMillis)
        {
            var currStat = new BatchStat(batchSize, totalRunMillis);
            DebugView = CalculateNewBatchSize(currStat);
            prevBatchStat = currStat;

            BatchSize = DebugView.NewBatchSize;
            return BatchSize;
        }

        private BatchSizeCalculation CalculateNewBatchSize(BatchStat currStat)
        {
            var bsc = new BatchSizeCalculation()
            {
                MinBatchSize = minBatchSize,
                MaxBatchSize = maxBatchSize,
            };
            var newBatchSize =
                prevBatchStat == null ? currStat.BatchSize : CalculateNextBatchSize(currStat, bsc);

            if (newBatchSize == currStat.BatchSize)
            {
                newBatchSize = EnsureChange(newBatchSize);
                bsc.S5EnsureChangedBatchSize = newBatchSize;
            }

            bsc.NewBatchSize = newBatchSize;
            return bsc;
        }

        private int CalculateNextBatchSize(BatchStat currStat, BatchSizeCalculation bsc)
        {
            bsc.OldBatchSize = prevBatchStat!.BatchSize;
            bsc.DeltaBatch = currStat.BatchSize - prevBatchStat.BatchSize;
            bsc.DeltaThroughput = currStat.Throughput - prevBatchStat.Throughput;
            var setpoint = currStat.BatchSize + bsc.ScaledAdjustment;
            bsc.S1Calculated = setpoint;
            if (setpoint > maxQueryTimeSeconds * currStat.Throughput)
            {
                setpoint = maxQueryTimeSeconds * currStat.Throughput;
            }

            if (setpoint < 1)
            {
                setpoint = 1;
            }

            bsc.S2Allowed = setpoint;


            var newBatchSize = (int)_lowPassFilter.Next(setpoint);
            bsc.LowPassBuffer = _lowPassFilter.GetBuffer();
            bsc.S3FromLowPass = newBatchSize;
            return bsc.S4Clamped;
        }

        private int EnsureChange(int input)
        {
            var larger = (int)Math.Round(input * 1.1) + 1;
            var smaller = (int)Math.Round(input * (1 / 1.1)) - 1;

            if (larger > maxBatchSize)
            {
                return smaller;
            }
            else if (smaller <= minBatchSize)
            {
                return larger;
            }
            else
#pragma warning disable CA5394 // Do not use insecure randomness
            if (_rndm.NextDouble() >= .25)
            {
#pragma warning disable CA5394 // Do not use insecure randomness
                return larger;
            }
            else
            {
                return smaller;
            }
        }
    }

    public delegate Task<List<T>> BatchAction<T>(List<T> batch);
}
