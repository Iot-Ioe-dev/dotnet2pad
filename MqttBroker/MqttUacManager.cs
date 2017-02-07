using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MqttBroker
{
    /// <summary>
    /// Manager for User Access Control
    /// </summary>
    public class MqttUacManager
    {
        /// <summary>
        /// Execute user authentication
        /// </summary>
        /// <param name="username">Username</param>
        /// <param name="password">Password</param>
        /// <returns>Access granted or not</returns>
        public bool UserAuthentication(string username, string password)
        {
            // TODO : developing true User Access Control
            return true;
        }
    }
}
