using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

/*
 * xxxxx frame frame indicesdata
 *
 * append item:
 *
 * xxxxx frame frame indiceddata(=n frames) frame
 * NOT GOOD
 *
 * so we need to shift indicesdata. How to do that efficiently?
 *
 */



namespace EntisCore.Utilities.BinaryStore
{
    public interface IBinaryStore
    {
        string Filepath { get; set; }
        BinaryStore Init(params Type[] types);
        FrameConfiguration Configuration { get; set; }
        BinaryStore EncryptConfig();
        BinaryStore AddIndexFor<T>(string propName);
        BinaryStore BuildConfigurationFromTypes(params Type[] types);
        void AddClassPropertiesAsSignalsToFrameDef(ref FrameDef dynSize, Type classType);
        List<T> SearchInIndex<T>(
            string filepath,
            string indexName,
            object value,
            bool exactFind = false
        )
            where T : new();
        List<T> SearchInIndex<T>(
            Stream indexStream,
            Stream dataStream,
            string indexName,
            object value,
            bool exactFind = false
        )
            where T : new();
        List<T> ReadFromFile<T>(string filepath)
            where T : new();
        List<T> ReadFromStream<T>(Stream stream)
            where T : new();
        void WriteToFile<T>(string filepath, List<T> items);
        void WriteToFile<T>(string filepath, List<T> items, bool append);
        void WriteToStream<T>(Stream stream, List<T> items, bool append);
        BinaryStore.StoreWriter GetWriter<T>(string filepath);
    }

    /// <summary>
    /// Datastorage engine to store and retrieve (multiple) lists of classes into a binary file.
    /// <para>The configuration of the data  as well as the data itself, is stored in the binary file.</para>
    /// </summary>
    public partial class BinaryStore : IBinaryStore
    {
        const string Fid = "<<DE_BS>>";
        public string Filepath { get; set; }

        /// <summary>
        /// The configuration of the frames. Leave null to be autodetected from write input.
        /// </summary>
        public FrameConfiguration Configuration { get; set; }

        /// <summary>
        /// Instantiate a store, without a Configuration.
        /// </summary>
        public BinaryStore() { }

        /// <summary>
        /// Instantiate and create a configuration for the provided types.
        /// </summary>
        /// <param name="types"></param>
        public BinaryStore Init(params Type[] types)
        {
            BuildConfigurationFromTypes(types);
            return this;
        }

        /// <summary>
        /// Will encrypt the configuration inside the binary file. If not called, the configuration can be read from the stream directly.
        /// </summary>
        /// <returns></returns>
        public BinaryStore EncryptConfig()
        {
            EncryptConfiguration = true;
            return this;
        }

        /// <summary>
        /// Create an index for the entered property. It will make searching the data
        /// drastically faster using <see cref="SearchInIndex{T}(Stream, string, object)"/>.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="propName">Must be available as a property in T.</param>
        /// <returns></returns>
        public BinaryStore AddIndexFor<T>(string propName)
        {
            if (string.IsNullOrWhiteSpace(propName))
                throw new ArgumentException("propName should be a property available in the Type.");
            var t = typeof(T).Name;
            if (!Indices.ContainsKey(t))
                Indices.Add(t, new List<string>());
            Indices[t].Add(propName);
            return this;
        }

        /// <summary>
        /// Builds a configuration for a number of Types.
        /// </summary>
        /// <param name="types"></param>
        public BinaryStore BuildConfigurationFromTypes(params Type[] types)
        {
            Configuration = new FrameConfiguration();
            for (int i = 0, j = 0; i < types.Length; i++)
            {
                var unmanagedTypes = new FrameDef
                {
                    ID = (short)(++j),
                    Name = types[i].Name,
                    Version = 1,
                };
                AddClassPropertiesAsSignalsToFrameDef(ref unmanagedTypes, types[i]);
                Configuration.FrameDefinitions.Add(unmanagedTypes);
            }
            return this;
        }

        /// <summary>
        /// Adds SignalDef items to the FrameDefinition, based on class properties.
        /// </summary>
        /// <param name="frameDef">The frame to add the signals to.</param>
        /// <param name="classType">The type of the class holding the properties to map.</param>
        public void AddClassPropertiesAsSignalsToFrameDef(ref FrameDef frameDef, Type classType)
        {
            if (classType == null)
                throw new ArgumentNullException("classType");
            if (!PropStore.ContainsKey(classType))
                PropStore.Add(classType, classType.GetProperties());
            var props = PropStore[classType].ToArray();
            PropertyInfo prop;
            SignalDef signal;
            for (int i = 0, m = props.Length; i < m; i++)
            {
                prop = props[i];
                signal = new SignalDef
                {
                    Name = prop.Name,
                    DataType = prop.PropertyType,
                    ID = (frameDef.ID * 1000) + i,
                };
                frameDef.Signals.Add(signal);
            }
        }

        public static string IndexFilename(string storeFp, string typeName, string indexName)
        {
            return Path.Combine(
                Path.GetDirectoryName(storeFp),
                string.Format(
                    "{0}_{1}_{2}.binx",
                    Path.GetFileNameWithoutExtension(storeFp),
                    General.CleanFilename(typeName),
                    General.CleanFilename(indexName)
                )
            );
        }

        public static string IndexFilename<T>(string storeFp, string indexName)
        {
            return IndexFilename(storeFp, typeof(T).Name, indexName);
        }

        private SortedDictionary<string, List<long>> ReadIndex(StoreReader ir)
        {
            return ReadIndex(ir, null);
        }

        private SortedDictionary<string, List<long>> ReadIndex(StoreReader ir, string key)
        {
            var indexData = new SortedDictionary<string, List<long>>();
            string iname;
            int numPositions;
            ir.BaseStream.Seek(0, SeekOrigin.Begin);
            while (ir.BaseStream.Position < ir.BaseStream.Length)
            {
                iname = ir.ReadString();
                numPositions = ir.ReadInt32();
                indexData.Add(iname, new List<long>());
                for (int i = 0; i < numPositions; i++)
                    indexData[iname].Add(ir.ReadInt64());
                if (key != null && iname.Equals(key))
                {
                    indexData = new SortedDictionary<string, List<long>>()
                    {
                        { key, indexData[key] }
                    };
                    break;
                }
                ;
            }
            return indexData;
        }

        /// <summary>
        /// Searches for a value in an index and returns all records
        /// that are a match.
        /// <para>The search is advanced: space-split the value, foreach term
        /// test startswith or contains.</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filepath"></param>
        /// <param name="indexName">The name of the index (=property name) to search in.</param>
        /// <param name="value">The value to find.</param>
        /// <returns></returns>
        public List<T> SearchInIndex<T>(
            string filepath,
            string indexName,
            object value,
            bool exactFind = false
        )
            where T : new()
        {
            if (string.IsNullOrEmpty(filepath))
                throw new ArgumentNullException("filepath");
            if (!File.Exists(filepath))
                throw new FileNotFoundException(filepath);
            if (string.IsNullOrEmpty(indexName))
                throw new ArgumentNullException("indexName");
            var indexFilepath = IndexFilename<T>(filepath, indexName);
            if (!File.Exists(indexFilepath))
                throw new FileNotFoundException(indexFilepath);
            if (value == null)
                throw new ArgumentNullException("value");
            Filepath = filepath;
            return SearchInIndex<T>(
                File.Open(indexFilepath, FileMode.Open, FileAccess.Read),
                File.Open(filepath, FileMode.Open, FileAccess.Read),
                indexName,
                value,
                exactFind
            );
        }

        /// <summary>
        /// Searches for a value in an index and returns all records
        /// that are a match.
        /// <para>The search is advanced: space-split the value, foreach term
        /// test startswith or contains.</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dataStream"></param>
        /// <param name="indexName">The name of the index (=property name) to search in.</param>
        /// <param name="value">The value to find.</param>
        /// <param name="exactFind">If true will use value as key of the index, no searching for part of a key.</param>
        /// <returns></returns>
        public List<T> SearchInIndex<T>(
            Stream indexStream,
            Stream dataStream,
            string indexName,
            object value,
            bool exactFind = false
        )
            where T : new()
        {
            if (indexStream == null)
                throw new ArgumentNullException("stream");
            if (indexStream.CanRead == false)
                throw new ArgumentException("Stream not readable.");
            if (dataStream == null)
                throw new ArgumentNullException("stream");
            if (dataStream.CanRead == false)
                throw new ArgumentException("Stream not readable.");
            if (string.IsNullOrEmpty(indexName))
                throw new ArgumentNullException("indexName");
            if (value == null)
                throw new ArgumentNullException("value");
            var list = new List<T>();
            //first read (part of) indexdata
            SortedDictionary<string, List<long>> indexData =
                new SortedDictionary<string, List<long>>();
            using (var ir = new StoreReader(indexStream))
            {
                string key = value is string && !exactFind ? null : Convert.ToString(value);
                indexData = ReadIndex(ir, key);
            }
            //now (maybe fuzzy) search in keys and lookup data in dataStream
            using (var r = new StoreReader(dataStream))
            {
                if (Configuration == null)
                    ReadHeadAndConfig(r);
                //fuzzy search
                if (value is string && !exactFind)
                {
                    //lookup positions
                    var svalue = ((string)value).ToLower();
                    var terms = svalue.Split(' ').Where(t => !string.IsNullOrEmpty(t));
                    var qry = indexData.Where(
                        k =>
                            terms.All(
                                t => k.Key.Equals(t) || k.Key.StartsWith(t) || k.Key.Contains(t)
                            )
                    );
                    if (qry.Count() > 0)
                    {
                        var framePositions = qry.Select(k => k.Value)
                            .Aggregate(
                                (v, w) =>
                                {
                                    v.AddRange(w);
                                    return v;
                                }
                            )
                            .Distinct()
                            .ToList();
                        foreach (var fpos in framePositions)
                        {
                            r.BaseStream.Seek(fpos, SeekOrigin.Begin);
                            ReadFrame(r, list);
                        }
                    }
                }
                else //direct lookup
                {
                    string key = Convert.ToString(value);
                    if (indexData.ContainsKey(key))
                    {
                        indexData[key].ForEach(fpos =>
                        {
                            r.BaseStream.Seek(fpos, SeekOrigin.Begin);
                            ReadFrame(r, list);
                        });
                    }
                }
            }

            return list;
        }

        /// <summary>
        /// Reads the contents of a BinaryStore bin-file into the corresponding list of items of type T.
        /// </summary>
        /// <typeparam name="T">The type of class to retrieve from the file.</typeparam>
        /// <param name="filepath"></param>
        /// <returns></returns>
        public List<T> ReadFromFile<T>(string filepath)
            where T : new()
        {
            if (string.IsNullOrEmpty(filepath))
                throw new ArgumentNullException("filepath");
            if (!File.Exists(filepath))
                throw new FileNotFoundException(filepath);
            Filepath = filepath;
            return ReadFromStream<T>(File.Open(filepath, FileMode.Open, FileAccess.Read));
        }

        /// <summary>
        /// Reads the Stream into the corresponding list of items of type T.
        /// </summary>
        /// <typeparam name="T">The type of class to retrieve from the file.</typeparam>
        /// <param name="stream"></param>
        /// <returns></returns>
        public List<T> ReadFromStream<T>(Stream stream)
            where T : new()
        {
            if (stream == null)
                throw new ArgumentNullException("stream");
            if (stream.CanRead == false)
                throw new ArgumentException("Stream not readable.");
            var list = new List<T>();
            using (var r = new StoreReader(stream))
            {
                ReadHeadAndConfig(r);
                ReadFrames(r, list);
            }
            return list;
        }

        private void ReadHeadAndConfig(StoreReader r)
        {
            //read fid
            var fid = r.ReadChars(Fid.Length);
            if (new string(fid) != Fid)
                throw new Exception("Unknown file type.");
            //read head
            var head = r.ReadString();
            //read config
            string config = r.ReadString();
            try
            {
                config = this.Obfuscator.Decrypt(config);
            }
            catch (Exception) { } //probably not encrypted.
            Configuration = JsonConvert.DeserializeObject<FrameConfiguration>(config);
        }

        private void ReadFrames<T>(StoreReader r, List<T> list)
            where T : new()
        {
            do
            {
                if (!ReadFrame(r, list))
                    break;
            } while (r.BaseStream.Length - 6 > r.BaseStream.Position);
        }

        private bool ReadFrame<T>(StoreReader r, List<T> list)
            where T : new()
        {
            var frame = new Frame
            {
                FrameID = (short)r.Read7BitEncodedInt(),
                FrameSize = r.Read7BitEncodedInt(),
                Serial = r.Read7BitEncodedInt(),
                //BinaryTime = r.ReadInt64(),
            };
            if (r.BaseStream.Length - frame.FrameSize - 2 >= r.BaseStream.Position)
            {
                var frameDef = Configuration.FrameDefinitions.First(d => d.ID == frame.FrameID);
                if (frameDef.Name == typeof(T).Name)
                {
                    frame.Data = r.ReadBytes(frame.FrameSize);
                    frame.Crc = r.ReadUInt16();

                    if (frame.CrcOk)
                    {
                        //int at = -1;
                        T item = default(T);
                        item = BytesToSignals(frameDef, frame, new T());
                        list.Add(item);
                        /*
                        if (frame.Serial <= list.Count)
                        {
                            at = frame.Serial;
                            item = list.ElementAtOrDefault(frame.Serial);
                        }
                        if (item == null)
                        {
                            item = BytesToSignals(frameDef, frame, new T());
                            if (at < 0) list.Add(item);
                            else list.Insert(at, item);
                        }
                        else
                        {
                            BytesToSignals(frameDef, frame, item);
                        }
                        */
                    }
                }
                else
                {
                    //skip frame
                    r.BaseStream.Seek(frame.FrameSize + 2, SeekOrigin.Current);
                }
                return true;
            }
            return false;
        }

        /// <summary>
        /// Writes the list of items to a binary file. This will overwrite an exisiting file.
        /// <para>When no configuration is set, it will be auto-created based on the class of the items in the list.</para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filepath"></param>
        /// <param name="items"></param>
        public void WriteToFile<T>(string filepath, List<T> items)
        {
            WriteToFile(filepath, items, false);
        }

        /// <summary>
        /// Writes the list of items to a binary file.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filepath"></param>
        /// <param name="items"></param>
        /// <param name="append">The configuration should have been set before appending (ie. by supplying the type in the constructor). If not, an exception will be thrown.</param>
        public void WriteToFile<T>(string filepath, List<T> items, bool append)
        {
            if (string.IsNullOrEmpty(filepath))
                throw new ArgumentNullException("filepath");
            Filepath = filepath;
            var typeName = typeof(T).Name;
            var typeIndices = Indices.FirstOrDefault(i => i.Key.Equals(typeName)).Value;
            if (append && typeIndices != null) //then we need to read current index.
            {
                foreach (var index in typeIndices)
                {
                    if (!IndicesData.ContainsKey(typeName))
                        IndicesData.Add(
                            typeName,
                            new Dictionary<string, SortedDictionary<string, List<long>>>()
                        );
                    if (!IndicesData[typeName].ContainsKey(index))
                        IndicesData[typeName].Add(
                            index,
                            new SortedDictionary<string, List<long>>()
                        );

                    var ifp = IndexFilename(filepath, typeName, index);
                    if (File.Exists(ifp))
                    {
                        using (
                            var r = new StoreReader(File.Open(ifp, FileMode.Open, FileAccess.Read))
                        )
                        {
                            IndicesData[typeName][index] = ReadIndex(r);
                        }
                    }
                }
            }
            WriteToStream(
                File.Open(
                    filepath,
                    append ? FileMode.OpenOrCreate : FileMode.Create,
                    FileAccess.ReadWrite
                ),
                items,
                append
            );
        }

        /// <summary>
        /// Writes the list of items to a stream.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="stream">Must be writable.</param>
        /// <param name="items">The list of items to write to the stream.</param>
        /// <param name="append">The configuration should have been set before appending (ie. by supplying the type in the constructor). If not, an exception will be thrown.</param>
        public void WriteToStream<T>(Stream stream, List<T> items, bool append)
        {
            if (items == null)
                throw new ArgumentNullException("items");
            using (var wl = GetWriter<T>(stream, append))
            {
                //write data frames
                for (int i = 0, m = items.Count; i < m; i++)
                {
                    wl.WriteRecord(items[i], i);
                }
            }
        }

        /// <summary>
        /// Prepares the binarywriter, adds the correct headers etc.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filepath"></param>
        /// <returns></returns>
        public StoreWriter GetWriter<T>(string filepath)
        {
            return GetWriter<T>(filepath, false);
        }

        /// <summary>
        /// Prepares the binarywriter, adds the correct headers etc.
        /// <para>Always dispose of the writer, please use
        /// <code>using (var writer = store.GetWriter&lt;T&gt;()){
        ///     writer.WriteRecord(...);
        /// }</code></para>
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="filepath"></param>
        /// <param name="append">Default is false.</param>
        /// <returns></returns>
        public StoreWriter GetWriter<T>(string filepath, bool append)
        {
            if (string.IsNullOrEmpty(filepath))
                throw new ArgumentNullException("filepath");
            Filepath = filepath;
            return GetWriter<T>(
                File.Open(
                    filepath,
                    append ? FileMode.OpenOrCreate : FileMode.Create,
                    FileAccess.ReadWrite
                ),
                append
            );
        }

        /// <summary>
        /// Returns the stores' binarywriter, writes the correct headers etc.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="stream"></param>
        /// <param name="append"></param>
        /// <returns></returns>
        public StoreWriter GetWriter<T>(Stream stream, bool append)
        {
            if (stream == null)
                throw new ArgumentNullException("stream");
            if (stream.CanWrite == false)
                throw new ArgumentException("Stream, not writable.");
            if (!append && Configuration == null)
            {
                BuildConfigurationFromTypes(typeof(T));
            }
            if (Configuration == null)
                throw new Exception(
                    "Configuration cannot be null when Appending frames to the file."
                );
            var dt = typeof(T);
            if (Indices.Count > 0)
            {
                foreach (var index in Indices)
                {
                    if (index.Value.Count > 0)
                    {
                        if (!IndicesData.ContainsKey(index.Key))
                            IndicesData.Add(
                                index.Key,
                                new Dictionary<string, SortedDictionary<string, List<long>>>()
                            );

                        index.Value.ForEach(propName =>
                        {
                            if (!IndicesData[index.Key].ContainsKey(propName))
                                IndicesData[index.Key].Add(
                                    propName,
                                    new SortedDictionary<string, List<long>>()
                                );
                        });
                    }
                }
            }
            var w = new StoreWriter(this, stream);
            if (!append || stream.Length == 0)
            {
                //write fid
                w.Write(Fid.ToCharArray());
                //write head
                w.Write(string.Format("-Deloitte Engine {0}-", GetType().Name));
                //write config
                if (EncryptConfiguration)
                    w.Write(this.Obfuscator.Encrypt(JsonConvert.SerializeObject(Configuration)));
                else
                    w.Write(JsonConvert.SerializeObject(Configuration));
            }
            w.Seek(0, SeekOrigin.End);
            return w;
        }

        private byte[] SignalsToBytes<T>(FrameDef frameDef, T item, long frameOffset)
        {
            if (frameDef == null)
                throw new ArgumentNullException("frameDef");
            if (item == null)
                throw new ArgumentNullException("item");
            var ct = typeof(T);
            if (!PropStore.ContainsKey(ct))
                PropStore.Add(ct, ct.GetProperties());
            var props = PropStore[ct];
            using (var mem = new MemoryStream())
            using (var w = new CompactStoreWriter(mem))
            {
                PropertyInfo prop;
                dynamic value = null;
                frameDef.Signals.ForEach(s =>
                {
                    prop = props.FirstOrDefault(p => p.Name == s.Name);
                    if (prop != null)
                    {
                        value = prop.GetValue(item, null);
                        byte[] sigValue;
                        if (prop.PropertyType == typeof(bool))
                        {
                            sigValue = BitConverter.GetBytes((bool)value);
                        }
                        else if (
                            prop.PropertyType == typeof(short)
                            || prop.PropertyType == typeof(short?)
                            || prop.PropertyType == typeof(int)
                            || prop.PropertyType == typeof(int?)
                        )
                        {
                            if (value == null)
                                sigValue = null;
                            else
                            {
                                using (var vm = new MemoryStream())
                                using (var vw = new CompactStoreWriter(vm))
                                {
                                    vw.Write7BitEncodedInt(value);
                                    sigValue = vm.ToArray();
                                }
                            }
                        }
                        else if (prop.PropertyType == typeof(long))
                        {
                            sigValue = BitConverter.GetBytes((long)value);
                        }
                        else if (
                            prop.PropertyType == typeof(DateTime)
                            || prop.PropertyType == typeof(DateTime?)
                        )
                        {
                            if (value == null)
                                sigValue = null;
                            else
                            {
                                sigValue = BitConverter.GetBytes(((DateTime)value).ToBinary());
                            }
                        }
                        else if (prop.PropertyType.IsSerializable)
                        {
                            sigValue = Serialize(value);
                        }
                        else
                        {
                            sigValue = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(value));
                        }

                        if (
                            Indices != null
                            && Indices.ContainsKey(ct.Name)
                            && Indices[ct.Name].Any(p => p.Equals(s.Name))
                        )
                        {
                            if (value is string)
                            {
                                value = (string)value.ToLower();
                            }
                            else
                                value = Convert.ToString(value);
                            if (value != null)
                            {
                                if (!IndicesData[ct.Name].ContainsKey(s.Name))
                                    IndicesData[ct.Name].Add(
                                        s.Name,
                                        new SortedDictionary<string, List<long>>()
                                    );
                                if (!IndicesData[ct.Name][s.Name].ContainsKey(value))
                                    IndicesData[ct.Name][s.Name].Add(value, new List<long>());
                                IndicesData[ct.Name][s.Name][value].Add(frameOffset);
                            }
                        }
                        w.Write7BitEncodedInt(s.ID);
                        w.Write7BitEncodedInt(sigValue?.Length ?? 0);
                        if (sigValue != null)
                            w.Write(sigValue);
                        value = null;
                    }
                    if (value != null)
                    {
                        w.Write(value);
                    }
                });
                return mem.ToArray();
            }
        }

        private T BytesToSignals<T>(FrameDef frameDef, Frame frame, T item)
            where T : new()
        {
            if (frameDef == null)
                throw new ArgumentNullException("frameDef");
            if (frame == null)
                throw new ArgumentNullException("frame");
            var ct = typeof(T);
            if (!PropStore.ContainsKey(ct))
                PropStore.Add(ct, ct.GetProperties());
            var props = PropStore[ct];
            using (var mem = new MemoryStream(frame.Data))
            using (var r = new StoreReader(mem))
            {
                PropertyInfo prop;
                byte[] buf;
                int len;
                int sid;
                frameDef.Signals.ForEach(s =>
                {
                    prop = props.FirstOrDefault(p => p.Name == s.Name);
                    if (prop != null)
                    {
                        sid = r.Read7BitEncodedInt();
                        len = r.Read7BitEncodedInt();
                        buf = r.ReadBytes(len);
                        if (sid == s.ID)
                        {
                            if (prop.PropertyType == typeof(bool))
                            {
                                prop.SetValue(item, BitConverter.ToBoolean(buf, 0), null);
                            }
                            else if (
                                prop.PropertyType == typeof(short)
                                || prop.PropertyType == typeof(short?)
                                || prop.PropertyType == typeof(int)
                                || prop.PropertyType == typeof(int?)
                            )
                            {
                                if (buf.Length == 0)
                                    prop.SetValue(item, null, null);
                                else
                                {
                                    using (var vm = new MemoryStream(buf))
                                    using (var vr = new StoreReader(vm))
                                    {
                                        if (prop.PropertyType == typeof(short?))
                                            prop.SetValue(
                                                item,
                                                (short?)vr.Read7BitEncodedInt(),
                                                null
                                            );
                                        else if (prop.PropertyType == typeof(short))
                                            prop.SetValue(
                                                item,
                                                (short)vr.Read7BitEncodedInt(),
                                                null
                                            );
                                        else if (prop.PropertyType == typeof(int?))
                                            prop.SetValue(
                                                item,
                                                (int?)vr.Read7BitEncodedInt(),
                                                null
                                            );
                                        else if (prop.PropertyType == typeof(int))
                                            prop.SetValue(item, vr.Read7BitEncodedInt(), null);
                                    }
                                }
                            }
                            else if (prop.PropertyType == typeof(long))
                            {
                                prop.SetValue(item, BitConverter.ToInt64(buf, 0), null);
                            }
                            else if (
                                prop.PropertyType == typeof(DateTime)
                                || prop.PropertyType == typeof(DateTime?)
                            )
                            {
                                if (buf.Length == 0)
                                    prop.SetValue(item, null, null);
                                else
                                {
                                    prop.SetValue(
                                        item,
                                        DateTime.FromBinary(BitConverter.ToInt64(buf, 0)),
                                        null
                                    );
                                }
                            }
                            else if (prop.PropertyType.IsSerializable)
                            {
                                prop.SetValue(item, Deserialize(buf), null);
                            }
                            else
                            {
                                prop.SetValue(
                                    item,
                                    JsonConvert.DeserializeObject(Encoding.UTF8.GetString(buf)),
                                    null
                                );
                            }
                        }
                    }
                });
            }
            return item;
        }

        static IFormatter BinFormatter = new BinaryFormatter();

        static byte[] Serialize(object source)
        {
            if (source == null)
                return null;
            using (var stream = new MemoryStream())
            {
                BinFormatter.Serialize(stream, source);
                return stream.ToArray();
            }
        }

        static object Deserialize(byte[] bytes)
        {
            if (bytes == null || bytes.Length == 0)
                return null;
            using (var stream = new MemoryStream(bytes))
            {
                return BinFormatter.Deserialize(stream);
            }
        }

        bool EncryptConfiguration = false;
        Obfuscator Obfuscator = new Obfuscator();
        static Dictionary<Type, IEnumerable<PropertyInfo>> PropStore =
            new Dictionary<Type, IEnumerable<PropertyInfo>>();
        Dictionary<string, List<string>> Indices = new Dictionary<string, List<string>>();

        /// <summary>
        /// Contains pointers to frame start positions.
        /// typeName, propName, propValue, framePositions[]
        /// </summary>
        Dictionary<string, Dictionary<string, SortedDictionary<string, List<long>>>> IndicesData =
            new Dictionary<string, Dictionary<string, SortedDictionary<string, List<long>>>>();
    }
}
