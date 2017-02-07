using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using uPLibrary.Networking.M2Mqtt.Messages;
using uPLibrary.Networking.M2Mqtt.Exceptions;

namespace MqttBroker
{
    /// <summary>
    /// MQTT controller with broker business logic
    /// </summary>
    public class MqttController
    {
        // clients connected list
        private List<MqttClientContext> clients;

        // reference to subscriber manager
        private MqttSubscriberManager subscriberManager;

        // reference to User Access Control manager
        private MqttUacManager uacManager;

        // queue messages to publish
        private Queue<MqttMsgBase> publishQueue;

        // thread for publishing
        private Thread publishThread;
        // event for starting publish
        private AutoResetEvent publishQueueWaitHandle;
        private bool isRunning;

        public MqttController()
        {
            this.clients = new List<MqttClientContext>();
            this.subscriberManager = new MqttSubscriberManager();
            
            this.uacManager = new MqttUacManager();

            this.publishQueue = new Queue<MqttMsgBase>();
            this.publishQueueWaitHandle = new AutoResetEvent(false);

            this.isRunning = true;
            this.publishThread = new Thread(this.PublishThread);
            this.publishThread.Start();
        }

        /// <summary>
        /// Process the message queue to publish
        /// </summary>
        public void PublishThread()
        {
            while (this.isRunning)
            {
                // wait on message queueud to publish
                this.publishQueueWaitHandle.WaitOne();

                lock (this.publishQueue)
                {
                    MqttMsgPublish publish = null;

                    int count = this.publishQueue.Count;
                    // publish all queued messages
                    while (count > 0)
                    {
                        count--;
                        publish = (MqttMsgPublish)this.publishQueue.Dequeue();

                        if (publish != null)
                        {
                            // get all subscriptions for a topic with a QoS level specified
                            List<MqttSubscription> subscriptions = this.subscriberManager.GetSubscriptions(publish.Topic, publish.QosLevel);

                            if ((subscriptions != null) && (subscriptions.Count != 0))
                            {
                                foreach (MqttSubscription subscription in subscriptions)
                                {
                                    // send PUBLISH message to the current subscriber
                                    subscription.Client.Publish(publish.Topic, publish.Message, publish.QosLevel, publish.Retain);
                                }
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Add client to connected client list
        /// </summary>
        /// <param name="client">Client to add</param>
        private void AddMqttClientContext(MqttClientContext client)
        {
            lock (this.clients)
            {
                this.clients.Add(client);
                Console.WriteLine("New client connected from " + client.ClientIP + " as " + client.ClientId);
            }
        }

        /// <summary>
        /// Remove client from connected client list
        /// </summary>
        /// <param name="client">Client to remove</param>
        private void RemoveMqttClientContext(MqttClientContext client)
        {
            lock (this.clients)
            {
                this.clients.Remove(client);
            }

            // if not clean session
            if (!client.CleanSession)
            {
                // TODO : persist client subscription ? if broker close
            }

            // delete client from runtime subscription
            this.subscriberManager.Unsubscribe(client);
        }

        /// <summary>
        /// Close a client
        /// </summary>
        /// <param name="client">Client to close</param>
        private void CloseClient(MqttClientContext client)
        {
            // if client is connected and it has a will message
            if (client.IsConnected && client.WillFlag)
            {
                // create the will PUBLISH message
                MqttMsgPublish publish =
                    new MqttMsgPublish(client.WillTopic, Encoding.UTF8.GetBytes(client.WillMessage), false, client.WillQosLevel, false);

                // enqueue
                lock (this.publishQueue)
                {
                    this.publishQueue.Enqueue(publish);
                }

                // unlock thread for sending messages to the subscribers
                this.publishQueueWaitHandle.Set();
            }

            // close the client
            client.Close();

            // remove client from the collection
            this.RemoveMqttClientContext(client);
        }

        

        public void ClientConnected(object sender, MqttClientConnectedEventArgs e)
        {
            // register event handlers from client
            e.Client.MqttMsgDisconnected += Client_MqttMsgDisconnected;
            e.Client.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;
            e.Client.MqttMsgConnected += Client_MqttMsgConnected;
            e.Client.MqttMsgSubscribeReceived += Client_MqttMsgSubscribeReceived;
            e.Client.MqttMsgUnsubscribeReceived += Client_MqttMsgUnsubscribeReceived;
            // add client to the collection
            //this.AddMqttClientContext(e.Client);

            // start client threads
            e.Client.Open();
        }

        bool isClientReg = true;

        void Client_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
        {
            MqttClientContext client = (MqttClientContext)sender;
            if (e.Topic.ToString().Contains("FZJC"))
            {
                isClientReg = false;
            }
            //System.Diagnostics.Debug.WriteLine(e.Topic + " " + Encoding.UTF8.GetString(e.Message));
            Console.WriteLine("DeviceID：" + e.Topic + "  And  Send content：" + Encoding.Default.GetString(e.Message));
            // TODO : verify dup flag
            MqttMsgPublish publish = new MqttMsgPublish(e.Topic, e.Message, false, e.QosLevel, e.Retain);

            // TODO : manage retain flag

            // message acquired...
            lock (this.publishQueue)
            {
                this.publishQueue.Enqueue(publish);
            }

            // unlock thread for sending messages to the subscribers
            this.publishQueueWaitHandle.Set();
        } 

        void Client_MqttMsgUnsubscribeReceived(object sender, MqttMsgUnsubscribeEventArgs e)
        {
            MqttClientContext client = (MqttClientContext)sender;

            for (int i = 0; i < e.Topics.Length; i++)
            {
                // unsubscribe client for each topic requested
                this.subscriberManager.Unsubscribe(e.Topics[i], client);
            }

            try
            {
                // send UNSUBACK message to the client
                client.Unsuback(e.MessageId);
            }
            catch (MqttCommunicationException)
            {
                this.CloseClient(client);
            }
        }

        public delegate void PadInfoHandleDeleagate(string clientid, string username, int clienttype);
        public event PadInfoHandleDeleagate PadInfoHandleEvent;

        void Client_MqttMsgSubscribeReceived(object sender, MqttMsgSubscribeEventArgs e)
        {
            MqttClientContext client = (MqttClientContext)sender;
            string strTopic = string.Empty;
            for (int i = 0; i < e.Topics.Length; i++)
            {
                // TODO : business logic to grant QoS levels based on some conditions ?
                //        now the broker granted the QoS levels requested by client

                // subscribe client for each topic and QoS level requested
                strTopic = e.Topics[i].ToString();
                this.subscriberManager.Subscribe(strTopic, e.QoSLevels[i], client);
                client.ClientId = strTopic;
                
                if (isClientReg)
                {
                    if (PadInfoHandleEvent != null)
                    {
                        PadInfoHandleEvent(e.Topics[i].ToString(), string.Empty, 1);
                    }
                    this.AddMqttClientContext(client);
                }
                
            }

            try
            {
                // send SUBACK message to the client
                client.Suback(e.MessageId, e.QoSLevels);
            }
            catch (MqttCommunicationException)
            {
                this.CloseClient(client);
            }
        }

        void Client_MqttMsgConnected(object sender, MqttMsgConnectEventArgs e)
        {
            MqttClientContext client = (MqttClientContext)sender;

            // verify message to determine CONNACK message return code to the client
            byte returnCode = this.MqttConnectVerify(e.Message);

            // connection "could" be accepted
            if (returnCode == MqttMsgConnack.CONN_ACCEPTED)
            {
                // check if there is a client already connected with same client Id
                MqttClientContext clientConnected = this.GetClient(e.Message.ClientId);

                // force connection close to the existing client (MQTT protocol)
                if (clientConnected != null)
                {
                    this.CloseClient(clientConnected);
                }
            }

            // TODO : manage received will message

            try
            {
                // send CONNACK message to the client
                client.Connack(returnCode, e.Message);
            }
            catch (MqttCommunicationException)
            {
                this.CloseClient(client);
            }
        }

        void Client_MqttMsgDisconnected(object sender, EventArgs e)
        {
            MqttClientContext client = (MqttClientContext)sender;

            // close the client
            this.CloseClient(client);
        }

        /// <summary>
        /// Check CONNECT message to accept or not the connection request 
        /// </summary>
        /// <param name="connect">CONNECT message received from client</param>
        /// <returns>Return code for CONNACK message</returns>
        private byte MqttConnectVerify(MqttMsgConnect connect)
        {
            byte returnCode = MqttMsgConnack.CONN_ACCEPTED;

            // unacceptable protocol version
            if (connect.ProtocolVersion != MqttMsgConnect.PROTOCOL_VERSION)
                returnCode = MqttMsgConnack.CONN_REFUSED_PROT_VERS;
            else
            {
                // client id length exceeded
                if (connect.ClientId.Length > MqttMsgConnect.CLIENT_ID_MAX_LENGTH)
                    returnCode = MqttMsgConnack.CONN_REFUSED_IDENT_REJECTED;
                else
                {
                    // check user authentication
                    if (!this.uacManager.UserAuthentication(connect.Username, connect.Password))
                        returnCode = MqttMsgConnack.CONN_REFUSED_USERNAME_PASSWORD;
                    // server unavailable and not authorized ?
                    else
                    {
                        // TODO : other checks on CONNECT message
                    }
                }
            }

            return returnCode;
        }

        /// <summary>
        /// Return reference to a client with a specified Id is already connected
        /// </summary>
        /// <param name="clientId">Client Id to verify</param>
        /// <returns>Reference to client</returns>
        private MqttClientContext GetClient(string clientId)
        {
            var query = from c in this.clients
                        where c.ClientId == clientId
                        select c;

            return query.FirstOrDefault();
        }
    }
}
