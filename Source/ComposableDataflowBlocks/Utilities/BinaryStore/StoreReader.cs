using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EntisCore.Utilities.BinaryStore
{
    public partial class BinaryStore
    {
        /// <summary>
        /// Little extension to the .NET BinaryWriter.
        /// </summary>
        public class StoreReader : BinaryReader
        {
            public StoreReader(Stream input)
                : base(input) { }

            public StoreReader(Stream input, Encoding encoding)
                : base(input, encoding) { }

            public StoreReader(Stream input, Encoding encoding, bool leaveOpen)
                : base(input, encoding, leaveOpen) { }

            public new int Read7BitEncodedInt()
            {
                return base.Read7BitEncodedInt();
            }
        }
    }
}
