using System;
using System.Collections.Generic;

namespace CounterpointCollective.Utilities
{
    /// <summary>
    /// _____R__
    ///    |
    ///    C
    /// ___|____
    /// -------------
    /// </summary>
    public class LowPassFilter
    {
        private readonly int _size;
        private readonly Queue<double> _points = new();
        private double avg;
        private object Lock { get; } = new();

        public LowPassFilter(int size)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(size, 2);

            _size = size;
        }

        public LowPassFilter(int size, double init)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(size, 2);

            _size = size;
            _points.Enqueue(init);
            avg = init;
        }

        public double Next(double numIn)
        {
            lock (Lock)
            { 
                _points.Enqueue(numIn);
                if (_points.Count > _size)
                {
                    avg -= _points.Dequeue() / _size;
                    avg += numIn / _size;
                }
                else
                {
                    avg = ((avg * (_points.Count - 1)) + numIn) / _points.Count;
                }
                return avg;
            }
        }

        public double[] GetBuffer()
        {
            lock (Lock)
            {
                return [.. _points];
            }
        }
    }
}
