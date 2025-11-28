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
        public class CompactStoreWriter : BinaryWriter
        {
            public CompactStoreWriter(Stream output)
                : base(output) { }

            public CompactStoreWriter(Stream output, Encoding encoding)
                : base(output, encoding) { }

            public CompactStoreWriter(Stream output, Encoding encoding, bool leaveOpen)
                : base(output, encoding, leaveOpen) { }

            public new void Write7BitEncodedInt(int value)
            {
                base.Write7BitEncodedInt(value);
            }
        }

        /// <summary>
        /// Little extension to the .NET BinaryWriter.
        /// </summary>
        public class StoreWriter : BinaryWriter
        {
            /// <summary>
            /// </summary>
            /// <param name="store"></param>
            /// <param name="stream"></param>
            internal StoreWriter(BinaryStore store, Stream stream)
                : base(stream)
            {
                Store = store ?? throw new ArgumentNullException("store");
            }

            /// <summary>
            /// Writes a record to a binarywriter. The writer must be instantiated
            /// by the  <see cref="GetWriter{T}(Stream, bool)"/> method.
            /// <para>To write a list of records, use <see cref="WriteToFile{T}(string, List{T}, bool)"/>
            /// instead.</para>
            /// </summary>
            /// <typeparam name="T"></typeparam>
            /// <param name="rec"></param>
            /// <param name="serial"></param>
            public void WriteRecord<T>(T rec, int serial)
            {
                byte[] signalBytes;
                foreach (
                    var frameDef in Store.Configuration.FrameDefinitions.Where(
                        f => f.Name == typeof(T).Name && f.Signals.Count > 0
                    )
                )
                {
                    signalBytes = Store.SignalsToBytes(frameDef, rec, BaseStream.Position);

                    Write7BitEncodedInt(frameDef.ID); //FrameID
                    Write7BitEncodedInt(signalBytes.Count()); //FrameSize
                    Write7BitEncodedInt(serial); //Serial
                    //Write(DateTime.Now.ToBinary());               //BinaryTime
                    Write(signalBytes); //Data
                    Write(Crc16.ComputeChecksum(signalBytes)); //Crc
                }
            }

            /// <summary>
            /// Writes every index into it's own file.
            /// (key-numpositions-positions... (long).
            /// </summary>
            private void WriteIndicesData()
            {
                foreach (var typeIndexData in Store.IndicesData)
                {
                    foreach (var indexData in typeIndexData.Value)
                    {
                        var indexFilepath = IndexFilename(
                            Store.Filepath,
                            typeIndexData.Key,
                            indexData.Key
                        );
                        using (var sm = File.Open(indexFilepath, FileMode.Create, FileAccess.Write))
                        using (var ss = new BinaryWriter(sm))
                        {
                            IEnumerable<long> positions;
                            foreach (var id in indexData.Value) //see ReadIndexData
                            {
                                positions = id.Value.Distinct();
                                ss.Write(id.Key); //value
                                ss.Write(positions.Count()); //number of positions
                                foreach (var p in positions)
                                    ss.Write(p); //positions
                            }
                        }
                    }
                }
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    WriteIndicesData();
                }
                base.Dispose(disposing);
            }

            BinaryStore Store;
        }
    }
}
