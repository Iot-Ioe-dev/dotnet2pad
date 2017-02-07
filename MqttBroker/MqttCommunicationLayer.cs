using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace MqttBroker
{
    /// <summary>
    /// MQTT communication layer
    /// </summary>
    public class MqttCommunicationLayer
    {
        // client connected event
        public event MqttClientConnectedEventHandler ClientConnected;

        // name for listener thread
        private const string LISTENER_THREAD_NAME = "MqttListenerThread";

        /// <summary>
        /// TCP listening port
        /// </summary>
        public int Port { get; private set; }

        // TCP listener for incoming connection requests
        private TcpListener listener;

        // TCP listener thread
        private Thread thread;
        private bool isRunning;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="port">TCP listening port</param>
        public MqttCommunicationLayer(int port)
        {
            this.Port = port;
        }

        /// <summary>
        /// Start communication layer listening
        /// </summary>
        public void Start()
        {
            // create and start listener thread
            this.thread = new Thread(this.ListenerThread);
            this.thread.Name = LISTENER_THREAD_NAME;
            this.isRunning = true;
            this.thread.Start();
        }

        /// <summary>
        /// Stop communication layer listening
        /// </summary>
        public void Stop()
        {
            this.isRunning = false;

            this.listener.Stop();

            // wait for thread
            this.thread.Join();
        }

        /// <summary>
        /// Listener thread for incoming connection requests
        /// </summary>
        private void ListenerThread()
        {
            // create listener...
            this.listener = new TcpListener(IPAddress.Any, this.Port);
            // ...and start it
            this.listener.Start();

            while (this.isRunning)
            {
                try
                {
                    // blocking call to wait for client connection
                    Socket socketClient = this.listener.AcceptSocket();

                    // manage socket client connected
                    if (socketClient.Connected)
                    {
                        MqttClientContext client = new MqttClientContext(socketClient);
                        // raise client raw connection event
                        this.OnClientConnected(client);
                    }
                }
                catch (Exception)
                {
                    if (!this.isRunning)
                        return;
                }
            }
        }

        /// <summary>
        /// Raise client connected event
        /// </summary>
        /// <param name="e">Event args</param>
        private void OnClientConnected(MqttClientContext client)
        {
            if (this.ClientConnected != null)
                this.ClientConnected(this, new MqttClientConnectedEventArgs(client));
        }
    }
}
