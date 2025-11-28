namespace EntisCore.Utilities.BinaryStore
{
    /// <summary>
    /// struct that represents a frame written to the binary file.
    /// </summary>
    public class Frame
    {
        /// <summary>
        /// Identifier of the frame. This id should correspond with a definition in the framedefinition.
        /// </summary>
        public short FrameID; //2

        /// <summary>
        /// The size of the frame's data in number of bytes.
        /// </summary>
        public int FrameSize; //4

        /// <summary>
        /// Identifier to be used to link link frames together.
        /// </summary>
        public int Serial; //4

        /// <summary>
        /// The DateTime.ToBinary() of the time the frame was written.
        /// </summary>
        //public long BinaryTime;//8
        /// <summary>
        /// The data inside the frame.
        /// </summary>
        public byte[] Data; //FrameSize

        /// <summary>
        /// 16 bit Crc check on the Data.
        /// </summary>
        public ushort Crc; //2

        /// <summary>
        /// Indicator that checks if the current Crc equals the calculated Crc16 of the Data. Data
        /// could be interpreted as correct when true.
        /// </summary>
        public bool CrcOk
        {
            get { return Data != null && Crc16.ComputeChecksum(Data) == Crc; }
        }
    }
}
