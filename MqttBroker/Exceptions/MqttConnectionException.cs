using System;

namespace uPLibrary.Networking.M2Mqtt.Exceptions
{
    /// <summary>
    /// Connection to the broker exception
    /// </summary>
    public class MqttConnectionException : ApplicationException
    {
        public MqttConnectionException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
