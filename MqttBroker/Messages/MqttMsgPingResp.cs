using System;

namespace uPLibrary.Networking.M2Mqtt.Messages
{
    /// <summary>
    /// Class for PINGRESP message from client to broker
    /// </summary>
    public class MqttMsgPingResp : MqttMsgBase
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public MqttMsgPingResp()
        {
            this.type = MQTT_MSG_PINGRESP_TYPE;
        }

        /// <summary>
        /// Parse bytes for a PINGRESP message
        /// </summary>
        /// <param name="fixedHeaderFirstByte">First fixed header byte</param>
        /// <param name="channel">Channel connected to the broker</param>
        /// <returns>PINGRESP message instance</returns>
        public static MqttMsgPingResp Parse(byte fixedHeaderFirstByte, IMqttNetworkChannel channel)
        {
            MqttMsgPingResp msg = new MqttMsgPingResp();

            // already know remaininglength is zero (MQTT specification),
            // so it isn't necessary to read other data from socket
            int remainingLength = MqttMsgBase.decodeRemainingLength(channel);
            
            return msg;
        }

        public override byte[] GetBytes()
        {
            byte[] buffer = new byte[2];
            int index = 0;

            // first fixed header byte
            buffer[index++] = (MQTT_MSG_PINGRESP_TYPE << MSG_TYPE_OFFSET);
            buffer[index++] = 0x00;

            return buffer;
        }
    }
}
