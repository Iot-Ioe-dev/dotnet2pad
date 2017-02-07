using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace uPLibrary.Networking.M2Mqtt.Messages
{
    /// <summary>
    /// Event Args class for any message received from client
    /// </summary>
    public class MqttMsgReceivedEventArgs : EventArgs
    {
        /// <summary>
        /// Message received from client
        /// </summary>
        public MqttMsgBase Msg { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="msg">Message received from client</param>
        public MqttMsgReceivedEventArgs(MqttMsgBase msg)
        {
            this.Msg = msg;
        }
    }
} 
