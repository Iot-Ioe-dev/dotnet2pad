using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MqttBroker
{
    /// <summary>
    /// Server main class for MQTT broker
    /// </summary>
    public class MqttServer
    {
        // MQTT broker settings
        private MqttSettings settings;

        // MQTT communication layer
        private MqttCommunicationLayer commLayer;

        // MQTT controller
        public MqttController controller;

        

        public MqttServer()
        {
            this.settings = MqttSettings.Instance;
            this.commLayer = new MqttCommunicationLayer(this.settings.Port);
            this.controller = new MqttController();
            this.commLayer.ClientConnected += this.controller.ClientConnected;
        }

        public void GetClient()
        { 
        
        }

        public void Start()
        {
            this.commLayer.Start();
        }

        public void Stop()
        {
            this.commLayer.Stop();
        }
    }
}
