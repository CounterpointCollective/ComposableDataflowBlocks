using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EntisCore.Utilities.BinaryStore
{
    /// <summary>
    /// Helper class to create FrameConfiguration.
    /// </summary>
    public class FrameConfiguration
    {
        /// <summary>
        /// Set of frame definitions that could be stored in the data.
        /// </summary>
        public List<FrameDef> FrameDefinitions { get; set; } = new List<FrameDef>();
    }

    /// <summary>
    /// Helper class to create FrameDef.
    /// </summary>
    public class FrameDef
    {
        /// <summary>
        /// An identifier for the framedefinition.
        /// </summary>
        public short ID { get; set; }

        /// <summary>
        ///
        /// </summary>
        public short Version { get; set; }

        /// <summary>
        /// Name of a frame. (Normally this would equal the name of the class holding the properties to store).
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// List if Signals (normally class-properties) this frame contains.
        /// </summary>
        public List<SignalDef> Signals { get; set; } = new List<SignalDef>();
    }

    /// <summary>
    /// Helper class to create SignalDefinition.
    /// </summary>
    public class SignalDef
    {
        public int ID { get; set; }
        public string Name { get; set; }
        public Type DataType { get; set; }
    }
}
