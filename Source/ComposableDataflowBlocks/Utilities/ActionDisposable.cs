using System;

namespace CounterpointCollective.Utilities
{
    public sealed class ActionDisposable : IDisposable
    {
        private readonly Action _dispose;
        private bool disposed;
        public ActionDisposable(Action dispose) => _dispose = dispose ?? throw new ArgumentNullException(nameof(dispose));
        public void Dispose() { if (!disposed) { disposed = true; _dispose(); } }
    }
}
