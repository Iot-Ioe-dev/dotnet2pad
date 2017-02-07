using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MqttBroker;
using System.ServiceModel;

namespace MqttBrokerServer
{
    class Program
    {
        static void Main(string[] args)
        {
            MqttServer server = new MqttServer();
            server.controller.PadInfoHandleEvent += new MqttController.PadInfoHandleDeleagate(controller_PadInfoHandleEvent);
            server.Start();
            Console.WriteLine("System Messages：MQTT Service has started！");
            Console.ReadLine();
            server.Stop();
        }

        static void controller_PadInfoHandleEvent(string clientid, string username, int clienttype)
        {
            var padclient = new PadClient();
            var ctx = new InstanceContext(padclient);
            var padsvc = new padserver.ServiceOfSepcialInfoClient(ctx);
            padsvc.RegisterClient(clientid, username, clienttype);
           
        }
    }
    public class PadClient : padserver.IServiceOfSepcialInfoCallback
    {
        public void SendOnLine(string ClientID, string UserName, int ClientType)
        {
        }
    }

}
