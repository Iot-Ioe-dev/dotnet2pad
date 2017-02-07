#if (!MF_FRAMEWORK_VERSION_V4_2 && !MF_FRAMEWORK_VERSION_V4_3)
using System;
#else
using Microsoft.SPOT;
#endif

namespace uPLibrary.Networking.M2Mqtt.Messages
{
    /// <summary>
    /// Event Args class for PUBLISH message received from broker
    /// </summary>
    public class MqttMsgPublishEventArgs : EventArgs
    {
        #region Properties...

        /// <summary>
        /// Message topic
        /// </summary>
        public string Topic
        {
            get { return this.topic; }
            internal set { this.topic = value; }
        }

        /// <summary>
        /// Message data
        /// </summary>
        public byte[] Message
        {
            get { return this.message; }
            internal set { this.message = value; }
        }

        /// <summary>
        /// Quality of Service level
        /// </summary>
        public byte QosLevel
        {
            get { return this.qosLevel; }
            internal set { this.qosLevel = value; }
        }

        /// <summary>
        /// Retain message flag
        /// </summary>
        public bool Retain
        {
            get { return this.retain; }
            internal set { this.retain = value; }
        }

        #endregion

        // message topic
        private string topic;
        // message data
        private byte[] message;
        // quality of service level
        protected byte qosLevel;
        // retain flag
        protected bool retain;       

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="topic">Message topic</param>
        /// <param name="message">Message data</param>
        /// <param name="qosLevel">Quality of Service level</param>
        /// <param name="retain">Retain flag</param>
        public MqttMsgPublishEventArgs(string topic,
            byte[] message,
            byte qosLevel,
            bool retain)
        {
            this.topic = topic;
            this.message = message;
            this.qosLevel = qosLevel;
            this.retain = retain;
        }
    }
}
