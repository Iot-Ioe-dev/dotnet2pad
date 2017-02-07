using System.Net.Sockets;
using System.Net;

namespace uPLibrary.Networking.M2Mqtt
{
    /// <summary>
    /// Channel to communicate over the network
    /// </summary>
    public class MqttNetworkChannel : IMqttNetworkChannel
    {
        // socket for communication with the client
        private Socket socket;

        /// <summary>
        /// Data available on the channel
        /// </summary>
        public bool DataAvailable
        {
            get
            {
                return (this.socket.Available > 0);
            }
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="socket">Socket opened with the client</param>
        public MqttNetworkChannel(Socket socket)
        {
            this.socket = socket;
        }

        /// <summary>
        /// Send data on the network channel to the client
        /// </summary>
        /// <param name="buffer">Data buffer to send</param>
        /// <returns>Number of byte sent</returns>
        public int Send(byte[] buffer)
        {
            return this.socket.Send(buffer, 0, buffer.Length, SocketFlags.None);
        }

        /// <summary>
        /// Receive data from the network channel (from client)
        /// </summary>
        /// <param name="buffer">Data buffer for receiving data</param>
        /// <returns>Number of bytes received</returns>
        public int Receive(byte[] buffer)
        {
            // read all data needed (until fill buffer)
            int idx = 0;
            while (idx < buffer.Length)
            {
                idx += this.socket.Receive(buffer, idx, buffer.Length - idx, SocketFlags.None);
            }
            return buffer.Length;
        }

        /// <summary>
        /// Close the network channel
        /// </summary>
        public void Close()
        {
            this.socket.Close();
        }


        public void Connect()
        {
            throw new System.NotImplementedException();
        }
    }
}
