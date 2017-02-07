using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MqttBroker
{
    /// <summary>
    /// Delegate event handler for MQTT client connected event
    /// </summary>
    /// <param name="sender">The object which raises event</param>
    /// <param name="e">Event args</param>
    public delegate void MqttClientConnectedEventHandler(object sender, MqttClientConnectedEventArgs e);

    /// <summary>
    /// MQTT client connected event args
    /// </summary>
    public class MqttClientConnectedEventArgs : EventArgs
    {
        /// <summary>
        /// Connected client
        /// </summary>
        public MqttClientContext Client { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="client">Connected client</param>
        public MqttClientConnectedEventArgs(MqttClientContext client)
        {
            this.Client = client;
        }
    }
}
