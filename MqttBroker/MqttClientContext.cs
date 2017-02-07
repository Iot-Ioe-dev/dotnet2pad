using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Exceptions;
using uPLibrary.Networking.M2Mqtt.Messages;
using System.Collections;
using System.Net;

namespace MqttBroker
{
    /// <summary>
    /// Context for an MQTT connected client
    /// </summary>
    public class MqttClientContext
    {
        // thread names
        private const string RECEIVE_THREAD_NAME = "ReceiveThread";
        private const string RECEIVE_EVENT_THREAD_NAME = "ReceiveEventThread";
        private const string PROCESS_INFLIGHT_THREAD_NAME = "ProcessInflightThread";
        private const string KEEP_ALIVE_THREAD = "KeepAliveThread";

        /// <summary>
        /// Delagate that defines event handler for PUBLISH message received from client
        /// </summary>
        public delegate void MqttMsgPublishEventHandler(object sender, MqttMsgPublishEventArgs e);

        /// <summary>
        /// Delegate that defines event handler for client disconnection
        /// </summary>
        public delegate void MqttMsgDisconnectEventHandler(object sender, EventArgs e);

        /// <summary>
        /// Delagate that defines event handler for connect message from a client
        /// </summary>
        public delegate void MqttMsgConnectEventHandler(object sender, MqttMsgConnectEventArgs e);

        /// <summary>
        /// Delagate that defines event handler for subscribe message from a client
        /// </summary>
        public delegate void MqttMsgSubscribeEventHandler(object sender, MqttMsgSubscribeEventArgs e);

        /// <summary>
        /// Delagate that defines event handler for unsubscribe message from a client
        /// </summary>
        public delegate void MqttMsgUnsubscribeEventHandler(object sender, MqttMsgUnsubscribeEventArgs e);

        // thread for receiving incoming message from client
        private Thread receiveThread;
        // thread for raising received message from client event
        private Thread receiveEventThread;
        private bool isRunning;
        // event for raising received message from client event
        private AutoResetEvent receiveEventWaitHandle;

        // thread for handling inflight messages queue to client asynchronously
        private Thread processInflightThread;
        // event for starting process inflight queue to client asynchronously
        private AutoResetEvent inflightWaitHandle;

        // message received from client
        MqttMsgBase msgReceived;

        // exeption thrown during receiving from client
        Exception exReceiving;

        // keep alive period (in ms)
        private int keepAlivePeriod;
        // thread for sending keep alive message
        private Thread keepAliveThread;
        private AutoResetEvent keepAliveEvent;
        // keep alive timeout expired
        private bool isKeepAliveTimeout;
        // last communication time in ticks
        private long lastCommTime;

        // event for PUBLISH message received from client
        public event MqttMsgPublishEventHandler MqttMsgPublishReceived;
        // event for client disconnection
        public event MqttMsgDisconnectEventHandler MqttMsgDisconnected;
        // event for connect message from a client
        public event MqttMsgConnectEventHandler MqttMsgConnected;
        // event for subscribe message from a client
        public event MqttMsgSubscribeEventHandler MqttMsgSubscribeReceived;
        // event for unsubscribe message from a client
        public event MqttMsgUnsubscribeEventHandler MqttMsgUnsubscribeReceived;

        

        // communication channel with the client
        private IMqttNetworkChannel channel;

        // inflight messages queue
        private Queue inflightQueue;
        // internal queue for received messages about inflight messages
        private Queue internalQueue;
        // receive queue for received messages
        private Queue receiveQueue;

        // reference to avoid access to singleton via property
        private MqttSettings settings;

        // current message identifier generated
        private ushort messageIdCounter = 0;

        /// <summary>
        /// Connection status between client and broker
        /// </summary>
        public bool IsConnected { get; private set; }

        /// <summary>
        /// Client identifier
        /// </summary>
        public string ClientId { get;  set; }

        /// <summary>
        /// Clean session flag
        /// </summary>
        public bool CleanSession { get; private set; }

        /// <summary>
        /// Will flag
        /// </summary>
        public bool WillFlag { get; private set; }

        /// <summary>
        /// Will QOS level
        /// </summary>
        public byte WillQosLevel { get; private set; }

        /// <summary>
        /// Will topic
        /// </summary>
        public string WillTopic { get; private set; }

        /// <summary>
        /// Will message
        /// </summary>
        public string WillMessage { get; private set; }
        /// <summary>
        /// 客户端IP
        /// </summary>
        public string ClientIP { get;private set; }
        /// <summary>
        /// 是否已经注册
        /// </summary>
        public bool IsRegister { get; private set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="socket">Raw socket for communication</param>
        public MqttClientContext(Socket socket)
        {
            this.channel = new MqttNetworkChannel(socket);

            // reference to MQTT settings
            this.settings = MqttSettings.Instance;

            // client not connected yet (CONNACK not send from client), some default values
            this.IsConnected = false;
            this.ClientId = null;
            this.ClientIP = ((System.Net.IPEndPoint)(socket.RemoteEndPoint)).Address.ToString();
            this.CleanSession = true;

            this.keepAliveEvent = new AutoResetEvent(false);

            // queue for handling inflight messages (publishing and acknowledge)
            this.inflightWaitHandle = new AutoResetEvent(false);
            this.inflightQueue = new Queue();
            
            // queue for received message
            this.receiveEventWaitHandle = new AutoResetEvent(false);
            this.receiveQueue = new Queue();
            this.internalQueue = new Queue();
        }

        /// <summary>
        /// Open client communication
        /// </summary>
        public void Open()
        {
            this.isRunning = true;

            // start thread for receiving messages from client
            this.receiveThread = new Thread(this.ReceiveThread);
            this.receiveThread.Name = RECEIVE_THREAD_NAME;
            this.receiveThread.Start();

            // start thread for raising received message event from client
            this.receiveEventThread = new Thread(this.ReceiveEventThread);
            this.receiveEventThread.Name = RECEIVE_EVENT_THREAD_NAME;
            this.receiveEventThread.Start();

            // start thread for handling inflight messages queue to client asynchronously (publish and acknowledge)
            this.processInflightThread = new Thread(this.ProcessInflightThread);
            this.processInflightThread.Name = PROCESS_INFLIGHT_THREAD_NAME;
            this.processInflightThread.Start();   
        }

        /// <summary>
        /// Close client communication
        /// </summary>
        public void Close()
        {
            // stop receiving thread
            this.isRunning = false;
                        
            // wait end receive thread
            //if (this.receiveThread != null)
            //    this.receiveThread.Join();

            // wait end receive event thread
            if (this.receiveEventThread != null)
            {
                this.receiveEventWaitHandle.Set();
                // NOTE : no join because Close() could be called inside ReceiveEventThread
                //        so we have to avoid deadlock
                //this.receiveEventThread.Join();
            }

            // waint end process inflight thread
            if (this.processInflightThread != null)
            {
                this.inflightWaitHandle.Set();
                // NOTE : no join because Close() could be called inside ProcessInflightThread
                //        so we have to avoid deadlock
                //this.processInflightThread.Join();
            }

            // avoid deadlock if keep alive timeout expired
            if (!this.isKeepAliveTimeout)
            {
                // unlock keep alive thread and wait
                if (this.keepAliveThread != null)
                    this.keepAliveEvent.Set();
            }
            
            // close network channel
            this.channel.Close();

            // keep alive thread will set it gracefully
            if (!this.isKeepAliveTimeout)
                this.IsConnected = false;
        }

#if BROKER
        /// <summary>
        /// Send CONNACK message to the client (connection accepted or not)
        /// </summary>
        /// <param name="returnCode">Return code for CONNACK message</param>
        /// <param name="connect">CONNECT message with all client information</param>
        public void Connack(byte returnCode, MqttMsgConnect connect)
        {
            this.lastCommTime = 0;

            // create CONNACK message and ...
            MqttMsgConnack connack = new MqttMsgConnack();
            connack.ReturnCode = returnCode;
            // ... send it to the client
            this.Send(connack.GetBytes());

            // connection accepted, start keep alive thread checking
            if (connack.ReturnCode == MqttMsgConnack.CONN_ACCEPTED)
            {
                this.ClientId = connect.ClientId;
                this.CleanSession = connect.CleanSession;
                this.WillFlag = connect.WillFlag;
                this.WillTopic = connect.WillTopic;
                this.WillMessage = connect.WillMessage;
                this.WillQosLevel = connect.WillQosLevel;

                this.keepAlivePeriod = connect.KeepAlivePeriod * 1000; // convert in ms
                // broker has a tolerance of 1.5 specified keep alive period
                this.keepAlivePeriod += (this.keepAlivePeriod / 2);

                // start thread for checking keep alive period timeout
                this.keepAliveThread = new Thread(this.KeepAliveThread);
                this.keepAliveThread.Name = KEEP_ALIVE_THREAD;
                this.keepAliveThread.Start();

                this.IsConnected = true;
            }
            // connection refused, close TCP/IP channel
            else
            {
                this.Close();
            }
        }

        /// <summary>
        /// Send SUBACK message to the client
        /// </summary>
        /// <param name="messageId">Message Id for the SUBSCRIBE message that is being acknowledged</param>
        /// <param name="grantedQosLevels">Granted QoS Levels</param>
        public void Suback(ushort messageId, byte[] grantedQosLevels)
        {
            MqttMsgSuback suback = new MqttMsgSuback();
            suback.MessageId = messageId;
            suback.GrantedQoSLevels = grantedQosLevels;

            this.Send(suback.GetBytes());
        }

        /// <summary>
        /// Send UNSUBACK message to the client
        /// </summary>
        /// <param name="messageId">Message Id for the UNSUBSCRIBE message that is being acknowledged</param>
        public void Unsuback(ushort messageId)
        {
            MqttMsgUnsuback unsuback = new MqttMsgUnsuback();
            unsuback.MessageId = messageId;

            this.Send(unsuback.GetBytes());
        }
#endif


        /// <summary>
        /// Publish a message to the client asynchronously
        /// </summary>
        /// <param name="topic">Message topic</param>
        /// <param name="message">Message data (payload)</param>
        /// <param name="qosLevel">QoS Level</param>
        /// <param name="retain">Retain flag</param>
        /// <returns>Message Id related to PUBLISH message</returns>
        public ushort Publish(string topic, byte[] message,
            byte qosLevel = MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE,
            bool retain = false)
        {
            MqttMsgPublish publish =
                    new MqttMsgPublish(topic, message, false, qosLevel, retain);
            publish.MessageId = this.GetMessageId();

            // set a default state
            MqttMsgState state = MqttMsgState.QueuedQos0;

            // based on QoS level, the messages flow between broker and client changes
            switch (qosLevel)
            {
                // QoS Level 0
                case MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE:

                    state = MqttMsgState.QueuedQos0;
                    break;

                // QoS Level 1
                case MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE:

                    state = MqttMsgState.QueuedQos1;
                    break;

                // QoS Level 2
                case MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE:

                    state = MqttMsgState.QueuedQos2;
                    break;
            }

            // queue message context
            MqttMsgContext msgContext = new MqttMsgContext()
            {
                Message = publish,
                State = state,
                Flow = MqttMsgFlow.ToPublish,
                Attempt = 0
            };

            lock (this.inflightQueue)
            {
                // enqueue message and unlock send thread
                this.inflightQueue.Enqueue(msgContext);
            }
            this.inflightWaitHandle.Set();
            
            return publish.MessageId;
        }

        /// <summary>
        /// Wrapper method for raising message received event
        /// </summary>
        /// <param name="msg">Message received</param>
        private void OnMqttMsgReceived(MqttMsgBase msg)
        {
            lock (this.receiveQueue)
            {
                this.receiveQueue.Enqueue(msg);
            }

            this.receiveEventWaitHandle.Set();
        }

        /// <summary>
        /// Wrapper method for raising PUBLISH message received event
        /// </summary>
        /// <param name="publish">PUBLISH message received</param>
        private void OnMqttMsgPublishReceived(MqttMsgPublish publish)
        {
            if (this.MqttMsgPublishReceived != null)
            {
                this.MqttMsgPublishReceived(this,
                    new MqttMsgPublishEventArgs(publish.Topic, publish.Message, publish.QosLevel, publish.Retain));
            }
        }

        /// <summary>
        /// Wrapper method for raising subscribe message event
        /// </summary>
        /// <param name="messageId">Message identifier for subscribe topics request</param>
        /// <param name="topics">Topics requested to subscribe</param>
        /// <param name="qosLevels">List of QOS Levels requested</param>
        private void OnMqttMsgSubscribeReceived(ushort messageId, string[] topics, byte[] qosLevels)
        {
            if (this.MqttMsgSubscribeReceived != null)
            {
                this.MqttMsgSubscribeReceived(this,
                    new MqttMsgSubscribeEventArgs(messageId, topics, qosLevels));
            }
        }

        /// <summary>
        /// Wrapper method for raising unsubscribe message event
        /// </summary>
        /// <param name="messageId">Message identifier for unsubscribe topics request</param>
        /// <param name="topics">Topics requested to unsubscribe</param>
        private void OnMqttMsgUnsubscribeReceived(ushort messageId, string[] topics)
        {
            if (this.MqttMsgUnsubscribeReceived != null)
            {
                this.MqttMsgUnsubscribeReceived(this,
                    new MqttMsgUnsubscribeEventArgs(messageId, topics));
            }
        }

        /// <summary>
        /// Wrapper method for client disconnection event
        /// </summary>
        private void OnMqttMsgDisconnected()
        {
            if (this.MqttMsgDisconnected != null)
            {
                this.MqttMsgDisconnected(this, EventArgs.Empty);
            }
        }

        /// <summary>
        /// Wrapper method for client connection event
        /// </summary>
        private void OnMqttMsgConnected(MqttMsgConnect connect)
        {
            if (this.MqttMsgConnected != null)
            {
                this.MqttMsgConnected(this, new MqttMsgConnectEventArgs(connect));
            }
        }

        /// <summary>
        /// Send a message to the client
        /// </summary>
        /// <param name="msgBytes">Message bytes</param>
        private void Send(byte[] msgBytes)
        {
            try
            {
                // send message
                this.channel.Send(msgBytes);
            }
            catch (Exception e)
            {
                throw new MqttCommunicationException();
            }
        }

        /// <summary>
        /// Enqueue a message into the inflight queue
        /// </summary>
        /// <param name="msg">Message to enqueue</param>
        /// <param name="flow">Message flow (publish, acknowledge)</param>
        private void EnqueueInflight(MqttMsgBase msg, MqttMsgFlow flow)
        {
            // enqueue is needed (or not)
            bool enqueue = true;

            // if it is a PUBLISH message with QoS Level 2
            if ((msg.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) &&
                (msg.QosLevel == MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE))
            {
                lock (this.inflightQueue)
                {
                    // if it is a PUBLISH message already received (it is in the inflight queue), the publisher
                    // re-sent it because it didn't received the PUBREC. In this case, we have to re-send PUBREC
                    MqttMsgContextFinder msgCtxFinder = new MqttMsgContextFinder(((MqttMsgPublish)msg).MessageId);
                    MqttMsgContext msgCtx = (MqttMsgContext)this.inflightQueue.Get(msgCtxFinder.Find);

                    // the PUBLISH message is alredy in the inflight queue, we don't need to re-enqueue but we need
                    // to change state to re-send PUBREC
                    if (msgCtx != null)
                    {
                        msgCtx.State = MqttMsgState.QueuedQos2;
                        msgCtx.Flow = MqttMsgFlow.ToAcknowledge;
                        enqueue = false;
                    }
                }
            }

            if (enqueue)
            {
                // set a default state
                MqttMsgState state = MqttMsgState.QueuedQos0;

                // based on QoS level, the messages flow between broker and client changes
                switch (msg.QosLevel)
                {
                    // QoS Level 0
                    case MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE:

                        state = MqttMsgState.QueuedQos0;
                        break;

                    // QoS Level 1
                    case MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE:

                        state = MqttMsgState.QueuedQos1;
                        break;

                    // QoS Level 2
                    case MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE:

                        state = MqttMsgState.QueuedQos2;
                        break;
                }

                // queue message context
                MqttMsgContext msgContext = new MqttMsgContext()
                {
                    Message = msg,
                    State = state,
                    Flow = flow,
                    Attempt = 0
                };

                lock (this.inflightQueue)
                {
                    // enqueue message and unlock send thread
                    this.inflightQueue.Enqueue(msgContext);
                }
            }

            this.inflightWaitHandle.Set();
        }

        /// <summary>
        /// Enqueue a message into the internal queue
        /// </summary>
        /// <param name="msg">Message to enqueue</param>
        private void EnqueueInternal(MqttMsgBase msg)
        {
            // enqueue is needed (or not)
            bool enqueue = true;

            // if it is a PUBREL message (for QoS Level 2)
            if (msg.Type == MqttMsgBase.MQTT_MSG_PUBREL_TYPE)
            {
                lock (this.inflightQueue)
                {
                    // if it is a PUBREL but the corresponding PUBLISH isn't in the inflight queue,
                    // it means that we processed PUBLISH message and received PUBREL and we sent PUBCOMP
                    // but publisher didn't receive PUBCOMP so it re-sent PUBREL. We need only to re-send PUBCOMP.

                    MqttMsgContextFinder msgCtxFinder = new MqttMsgContextFinder(((MqttMsgPubrel)msg).MessageId);
                    MqttMsgContext msgCtx = (MqttMsgContext)this.inflightQueue.Get(msgCtxFinder.Find);

                    // the PUBLISH message isn't in the inflight queue, it was already processed so
                    // we need to re-send PUBCOMP only
                    if (msgCtx == null)
                    {
                        MqttMsgPubcomp pubcomp = new MqttMsgPubcomp();
                        pubcomp.MessageId = ((MqttMsgPubrel)msg).MessageId;

                        this.Send(pubcomp.GetBytes());

                        enqueue = false;
                    }
                }
            }

            if (enqueue)
            {
                lock (this.internalQueue)
                {
                    this.internalQueue.Enqueue(msg);
                    this.inflightWaitHandle.Set();
                }
            }
        }

        /// <summary>
        /// Thread for receiving messages from client
        /// </summary>
        private void ReceiveThread()
        {
            int readBytes = 0;
            byte[] fixedHeaderFirstByte = new byte[1];
            byte msgType;
            
#if BROKER
            long now = 0;

            // receive thread started, broker need to receive the first message
            // (CONNECT) within a reasonable amount of time after TCP/IP connection 
            long connectTime = Environment.TickCount;
#endif

            while (this.isRunning)
            {
                try
                {
                    if (this.channel.DataAvailable)
                        // read first byte (fixed header)
                        readBytes = this.channel.Receive(fixedHeaderFirstByte);
                    else
                    {
#if BROKER
                        // client not connected (client didn't send CONNECT yet)
                        if (!this.IsConnected)
                        {
                            now = Environment.TickCount;

                            // if connect timeout exceeded ... 
                            if ((now - connectTime) >= this.settings.TimeoutOnConnection)
                            {
                                // client must close connection
                                this.Close();

                                // client raw disconnection
                                this.OnMqttMsgDisconnected();
                            }
                        }
#endif
                        // no bytes available, sleep before retry
                        readBytes = 0;
                        Thread.Sleep(10);
                    }

                    if (readBytes > 0)
                    {
#if BROKER
                        // update last message received ticks
                        this.lastCommTime = Environment.TickCount;
#endif

                        // extract message type from received byte
                        msgType = (byte)((fixedHeaderFirstByte[0] & MqttMsgBase.MSG_TYPE_MASK) >> MqttMsgBase.MSG_TYPE_OFFSET);

                        switch (msgType)
                        {
                            // CONNECT message received from client
                            case MqttMsgBase.MQTT_MSG_CONNECT_TYPE:

#if BROKER
                                MqttMsgConnect connect = MqttMsgConnect.Parse(fixedHeaderFirstByte[0], this.channel);
                                
                                // raise message received event
                                this.OnMqttMsgReceived(connect);
                                break;
#else
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
#endif
                                
                            // impossible, broker can't received CONNACK message
                            case MqttMsgBase.MQTT_MSG_CONNACK_TYPE:

#if BROKER
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
#else
                                this.msgReceived = MqttMsgConnack.Parse(fixedHeaderFirstByte[0], this.channel);
                                this.syncEndReceiving.Set();
                                break;
#endif

                            // PINGREQ message received from client
                            case MqttMsgBase.MQTT_MSG_PINGREQ_TYPE:

#if BROKER
                                this.msgReceived = MqttMsgPingReq.Parse(fixedHeaderFirstByte[0], this.channel);

                                MqttMsgPingResp pingresp = new MqttMsgPingResp();
                                this.Send(pingresp.GetBytes());

                                // raise message received event
                                //this.OnMqttMsgReceived(this.msgReceived);
                                break;
#else
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
#endif

                            // impossible, broker can't receive PINGRESP message
                            case MqttMsgBase.MQTT_MSG_PINGRESP_TYPE:

#if BROKER
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
#else
                                this.msgReceived = MqttMsgPingResp.Parse(fixedHeaderFirstByte[0], this.channel);
                                this.syncEndReceiving.Set();
                                break;
#endif

                            // SUBSCRIBE message received from client
                            case MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE:

#if BROKER
                                MqttMsgSubscribe subscribe = MqttMsgSubscribe.Parse(fixedHeaderFirstByte[0], this.channel);

                                // raise message received event
                                this.OnMqttMsgReceived(subscribe);

                                break;
#else
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
#endif

                            // impossible, broker can't receive SUBACK message
                            case MqttMsgBase.MQTT_MSG_SUBACK_TYPE:

#if BROKER
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
#else
                                // enqueue SUBACK message received (for QoS Level 1) into the internal queue
                                MqttMsgSuback suback = MqttMsgSuback.Parse(fixedHeaderFirstByte[0], this.channel);

                                // enqueue SUBACK message into the internal queue
                                this.EnqueueInternal(suback);

                                break;
#endif

                            // PUBLISH message received from client
                            case MqttMsgBase.MQTT_MSG_PUBLISH_TYPE:

                                MqttMsgPublish publish = MqttMsgPublish.Parse(fixedHeaderFirstByte[0], this.channel);

                                // enqueue PUBLISH message to acknowledge into the inflight queue
                                this.EnqueueInflight(publish, MqttMsgFlow.ToAcknowledge);

                                break;

                            // PUBACK message received from client
                            case MqttMsgBase.MQTT_MSG_PUBACK_TYPE:

                                // enqueue PUBACK message received (for QoS Level 1) into the internal queue
                                MqttMsgPuback puback = MqttMsgPuback.Parse(fixedHeaderFirstByte[0], this.channel);

                                // enqueue PUBACK message into the internal queue
                                this.EnqueueInternal(puback);

                                break;

                            // PUBREC message received from client
                            case MqttMsgBase.MQTT_MSG_PUBREC_TYPE:

                                // enqueue PUBREC message received (for QoS Level 2) into the internal queue
                                MqttMsgPubrec pubrec = MqttMsgPubrec.Parse(fixedHeaderFirstByte[0], this.channel);

                                // enqueue PUBREC message into the internal queue
                                this.EnqueueInternal(pubrec);

                                break;

                            // PUBREL message received from client
                            case MqttMsgBase.MQTT_MSG_PUBREL_TYPE:

                                // enqueue PUBREL message received (for QoS Level 2) into the internal queue
                                MqttMsgPubrel pubrel = MqttMsgPubrel.Parse(fixedHeaderFirstByte[0], this.channel);

                                // enqueue PUBREL message into the internal queue
                                this.EnqueueInternal(pubrel);

                                break;

                            //  PUBCOMP message received from client
                            case MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE:

                                // enqueue PUBCOMP message received (for QoS Level 2) into the internal queue
                                MqttMsgPubcomp pubcomp = MqttMsgPubcomp.Parse(fixedHeaderFirstByte[0], this.channel);

                                // enqueue PUBCOMP message into the internal queue
                                this.EnqueueInternal(pubcomp);

                                break;

                            // UNSUBSCRIBE message received from client
                            case MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE:

#if BROKER
                                MqttMsgUnsubscribe unsubscribe = MqttMsgUnsubscribe.Parse(fixedHeaderFirstByte[0], this.channel);

                                // raise message received event
                                this.OnMqttMsgReceived(unsubscribe);

                                break;
#else
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
#endif

                            // impossible, broker can't receive UNSUBACK message
                            case MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE:

#if BROKER
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
#else
                                // enqueue UNSUBACK message received (for QoS Level 1) into the internal queue
                                MqttMsgUnsuback unsuback = MqttMsgUnsuback.Parse(fixedHeaderFirstByte[0], this.channel);

                                // enqueue UNSUBACK message into the internal queue
                                this.EnqueueInternal(unsuback);

                                break;
#endif

                            // DISCONNECT message received from client
                            case MqttMsgDisconnect.MQTT_MSG_DISCONNECT_TYPE:

#if BROKER
                                MqttMsgDisconnect disconnect = MqttMsgDisconnect.Parse(fixedHeaderFirstByte[0], this.channel);

                                // raise message received event
                                this.OnMqttMsgReceived(disconnect);

                                break;
#else
                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
#endif

                            default:

                                throw new MqttClientException(MqttClientErrorCode.WrongBrokerMessage);
                        }

                        this.exReceiving = null;
                    }

                }
                catch (Exception)
                {
                    this.exReceiving = new MqttCommunicationException();
                }
            }
        }

        /// <summary>
        /// Thread for check keep alive period
        /// </summary>
        private void KeepAliveThread()
        {
            long now = 0;
            int wait = this.keepAlivePeriod;
            this.isKeepAliveTimeout = false;

            while (this.isRunning)
            {
                // waiting...
                this.keepAliveEvent.WaitOne(wait, false);

                if (this.isRunning)
                {
                    now = Environment.TickCount;

                    // if timeout exceeded ... (keep alive period converted in ticks)
                    if ((now - this.lastCommTime) >= this.keepAlivePeriod)
                    {
                        this.isKeepAliveTimeout = true;
                        // client must close connection
                        this.Close();

                        this.OnMqttMsgDisconnected();
                    }
                    else
                    {
                        // update waiting time (convert ticks in milliseconds)
                        wait = (int)(this.keepAlivePeriod - (now - this.lastCommTime));
                    }
                }
            }

            if (this.isKeepAliveTimeout)
                this.IsConnected = false;
        }

        /// <summary>
        /// Thread for raising received message event
        /// </summary>
        private void ReceiveEventThread()
        {
            while (this.isRunning)
            {
                if (this.receiveQueue.Count == 0)
                    // wait on receiving message from client
                    this.receiveEventWaitHandle.WaitOne();

                // check if it is running or we are closing client
                if (this.isRunning)
                {
                    // get message from queue
                    MqttMsgBase msg = null;
                    lock (this.receiveQueue)
                    {
                        if (this.receiveQueue.Count > 0)
                            msg = (MqttMsgBase)this.receiveQueue.Dequeue();
                    }

                    if (msg != null)
                    {
                        switch (msg.Type)
                        {
                            // CONNECT message received from client
                            case MqttMsgBase.MQTT_MSG_CONNECT_TYPE:

                                // raise connected client event (CONNECT message received)
                                this.OnMqttMsgConnected((MqttMsgConnect)msg);
                                break;

                            // SUBSCRIBE message received from client
                            case MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE:

                                MqttMsgSubscribe subscribe = (MqttMsgSubscribe)msg;
                                // raise subscribe topic event (SUBSCRIBE message received)
                                this.OnMqttMsgSubscribeReceived(subscribe.MessageId, subscribe.Topics, subscribe.QoSLevels);

                                break;

                            // SUBACK message received from broker
                            case MqttMsgBase.MQTT_MSG_SUBACK_TYPE:

                                // raise subscribed topic event (SUBACK message received)
                                //this.OnMqttMsgSubscribed((MqttMsgSuback)msg);
                                break;

                            // PUBLISH message received from broker
                            case MqttMsgBase.MQTT_MSG_PUBLISH_TYPE:

                                // raise PUBLISH message received event 
                                this.OnMqttMsgPublishReceived((MqttMsgPublish)msg);
                                break;

                            // PUBACK message received from broker
                            case MqttMsgBase.MQTT_MSG_PUBACK_TYPE:

                                // raise published message event
                                // (PUBACK received for QoS Level 1)
                                //this.OnMqttMsgPublished(((MqttMsgPuback)msg).MessageId);
                                break;

                            // PUBREL message received from broker
                            case MqttMsgBase.MQTT_MSG_PUBREL_TYPE:

                                // raise message received event 
                                // (PUBREL received for QoS Level 2)
                                this.OnMqttMsgPublishReceived((MqttMsgPublish)msg);
                                break;

                            // PUBCOMP message received from broker
                            case MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE:

                                // raise published message event
                                // (PUBCOMP received for QoS Level 2)
                                //this.OnMqttMsgPublished(((MqttMsgPubcomp)msg).MessageId);
                                break;

                            // UNSUBSCRIBE message received from client
                            case MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE:

                                MqttMsgUnsubscribe unsubscribe = (MqttMsgUnsubscribe)msg;
                                // raise unsubscribe topic event (UNSUBSCRIBE message received)
                                this.OnMqttMsgUnsubscribeReceived(unsubscribe.MessageId, unsubscribe.Topics);
                                break;

                            // UNSUBACK message received from broker
                            case MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE:

                                // raise unsubscribed topic event
                                //this.OnMqttMsgUnsubscribed(((MqttMsgUnsuback)msg).MessageId);
                                break;

                            // DISCONNECT message received from client
                            case MqttMsgDisconnect.MQTT_MSG_DISCONNECT_TYPE:

                                // raise disconnected client event (DISCONNECT message received)
                                this.OnMqttMsgDisconnected();
                                break;
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Process inflight messages queue
        /// </summary>
        private void ProcessInflightThread()
        {
            MqttMsgContext msgContext = null;
            MqttMsgBase msgInflight = null;
            MqttMsgBase msgReceived = null;
            bool acknowledge = false;
            int timeout = Timeout.Infinite;

            try
            {
                while (this.isRunning)
                {
#if (MF_FRAMEWORK_VERSION_V4_2 || MF_FRAMEWORK_VERSION_V4_3 || COMPACT_FRAMEWORK)
                    // wait on message queueud to inflight
                    this.inflightWaitHandle.WaitOne(timeout, false);
#else
                    // wait on message queueud to inflight
                    //this.inflightWaitHandle.WaitOne(timeout);
                    Thread.Sleep(10);
#endif

                    // it could be unblocked because Close() method is joining
                    if (this.isRunning)
                    {
                        lock (this.inflightQueue)
                        {
                            // set timeout tu MaxValue instead of Infinte (-1) to perform
                            // compare with calcultad current msgTimeout
                            timeout = Int32.MaxValue;

                            // a message inflight could be re-enqueued but we have to
                            // analyze it only just one time for cycle
                            int count = this.inflightQueue.Count;
                            // process all inflight queued messages
                            while (count > 0)
                            {
                                count--;
                                acknowledge = false;
                                msgReceived = null;

                                // dequeue message context from queue
                                msgContext = (MqttMsgContext)this.inflightQueue.Dequeue();

                                // get inflight message
                                msgInflight = (MqttMsgBase)msgContext.Message;

                                switch (msgContext.State)
                                {
                                    case MqttMsgState.QueuedQos0:

                                        // QoS 0, PUBLISH message to send to broker, no state change, no acknowledge
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            this.Send(msgInflight.GetBytes());
                                        }
                                        // QoS 0, no need acknowledge
                                        else if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
                                        {
                                            // notify published message from broker (no need acknowledged)
                                            this.OnMqttMsgReceived(msgInflight);
                                        }
                                        break;

                                    case MqttMsgState.QueuedQos1:

                                        // QoS 1, PUBLISH or SUBSCRIBE/UNSUBSCRIBE message to send to broker, state change to wait PUBACK or SUBACK/UNSUBACK
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            if (msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE)
                                                // PUBLISH message to send, wait for PUBACK
                                                msgContext.State = MqttMsgState.WaitForPuback;
                                            else if (msgInflight.Type == MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE)
                                                // SUBSCRIBE message to send, wait for SUBACK
                                                msgContext.State = MqttMsgState.WaitForSuback;
                                            else if (msgInflight.Type == MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE)
                                                // UNSUBSCRIBE message to send, wait for UNSUBACK
                                                msgContext.State = MqttMsgState.WaitForUnsuback;

                                            msgContext.Timestamp = Environment.TickCount;
                                            msgContext.Attempt++;
                                            // retry ? set dup flag
                                            if (msgContext.Attempt > 1)
                                                msgInflight.DupFlag = true;

                                            this.Send(msgInflight.GetBytes());

                                            // update timeout
                                            int msgTimeout = (this.settings.DelayOnRetry - (Environment.TickCount - msgContext.Timestamp));
                                            timeout = (msgTimeout < timeout) ? msgTimeout : timeout;

                                            // re-enqueue message (I have to re-analyze for receiving PUBACK, SUBACK or UNSUBACK)
                                            this.inflightQueue.Enqueue(msgContext);
                                        }
                                        // QoS 1, PUBLISH message received from broker to acknowledge, send PUBACK
                                        else if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
                                        {
                                            MqttMsgPuback puback = new MqttMsgPuback();
                                            puback.MessageId = ((MqttMsgPublish)msgInflight).MessageId;

                                            this.Send(puback.GetBytes());

                                            // notify published message from broker and acknowledged
                                            this.OnMqttMsgReceived(msgInflight);
                                        }
                                        break;

                                    case MqttMsgState.QueuedQos2:

                                        // QoS 2, PUBLISH message to send to broker, state change to wait PUBREC
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            msgContext.State = MqttMsgState.WaitForPubrec;
                                            msgContext.Timestamp = Environment.TickCount;
                                            msgContext.Attempt++;
                                            // retry ? set dup flag
                                            if (msgContext.Attempt > 1)
                                                msgInflight.DupFlag = true;

                                            this.Send(msgInflight.GetBytes());

                                            // update timeout
                                            int msgTimeout = (this.settings.DelayOnRetry - (Environment.TickCount - msgContext.Timestamp));
                                            timeout = (msgTimeout < timeout) ? msgTimeout : timeout;

                                            // re-enqueue message (I have to re-analyze for receiving PUBREC)
                                            this.inflightQueue.Enqueue(msgContext);
                                        }
                                        // QoS 2, PUBLISH message received from broker to acknowledge, send PUBREC, state change to wait PUBREL
                                        else if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
                                        {
                                            MqttMsgPubrec pubrec = new MqttMsgPubrec();
                                            pubrec.MessageId = ((MqttMsgPublish)msgInflight).MessageId;

                                            msgContext.State = MqttMsgState.WaitForPubrel;

                                            this.Send(pubrec.GetBytes());

                                            // re-enqueue message (I have to re-analyze for receiving PUBREL)
                                            this.inflightQueue.Enqueue(msgContext);
                                        }
                                        break;

                                    case MqttMsgState.WaitForPuback:
                                    case MqttMsgState.WaitForSuback:
                                    case MqttMsgState.WaitForUnsuback:

                                        // QoS 1, waiting for PUBACK of a PUBLISH message sent or
                                        //        waiting for SUBACK of a SUBSCRIBE message sent or
                                        //        waiting for UNSUBACK of a UNSUBSCRIBE message sent or
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            acknowledge = false;
                                            lock (this.internalQueue)
                                            {
                                                if (this.internalQueue.Count > 0)
                                                    msgReceived = (MqttMsgBase)this.internalQueue.Peek();
                                            }

                                            // it is a PUBACK message or a SUBACK/UNSUBACK message
                                            if ((msgReceived != null) && ((msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBACK_TYPE) ||
                                                                          (msgReceived.Type == MqttMsgBase.MQTT_MSG_SUBACK_TYPE) ||
                                                                          (msgReceived.Type == MqttMsgBase.MQTT_MSG_UNSUBACK_TYPE)))
                                            {
                                                // PUBACK message or SUBACK message for the current message
                                                if (((msgInflight.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) && (((MqttMsgPuback)msgReceived).MessageId == ((MqttMsgPublish)msgInflight).MessageId)) ||
                                                    ((msgInflight.Type == MqttMsgBase.MQTT_MSG_SUBSCRIBE_TYPE) && (((MqttMsgSuback)msgReceived).MessageId == ((MqttMsgSubscribe)msgInflight).MessageId)) ||
                                                    ((msgInflight.Type == MqttMsgBase.MQTT_MSG_UNSUBSCRIBE_TYPE) && (((MqttMsgUnsuback)msgReceived).MessageId == ((MqttMsgUnsubscribe)msgInflight).MessageId)))
                                                {
                                                    lock (this.internalQueue)
                                                    {
                                                        // received message processed
                                                        this.internalQueue.Dequeue();
                                                        acknowledge = true;
                                                    }

                                                    // notify received acknowledge from broker of a published message or subscribe/unsubscribe message
                                                    this.OnMqttMsgReceived(msgReceived);
                                                }
                                            }

                                            // current message not acknowledged, no PUBACK or SUBACK/UNSUBACK or not equal messageid 
                                            if (!acknowledge)
                                            {
                                                // check timeout for receiving PUBACK since PUBLISH was sent or
                                                // for receiving SUBACK since SUBSCRIBE was sent or
                                                // for receiving UNSUBACK since UNSUBSCRIBE was sent
                                                if ((Environment.TickCount - msgContext.Timestamp) >= this.settings.DelayOnRetry)
                                                {
                                                    // max retry not reached, resend
                                                    if (msgContext.Attempt <= this.settings.AttemptsOnRetry)
                                                    {
                                                        msgContext.State = MqttMsgState.QueuedQos1;

                                                        // re-enqueue message
                                                        this.inflightQueue.Enqueue(msgContext);

                                                        // update timeout (0 -> reanalyze queue immediately)
                                                        timeout = 0;
                                                    }
                                                }
                                                else
                                                {
                                                    // re-enqueue message (I have to re-analyze for receiving PUBACK, SUBACK or UNSUBACK)
                                                    this.inflightQueue.Enqueue(msgContext);

                                                    // update timeout
                                                    int msgTimeout = (this.settings.DelayOnRetry - (Environment.TickCount - msgContext.Timestamp));
                                                    timeout = (msgTimeout < timeout) ? msgTimeout : timeout;
                                                }
                                            }
                                        }
                                        break;

                                    case MqttMsgState.WaitForPubrec:

                                        // QoS 2, waiting for PUBREC of a PUBLISH message sent
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            acknowledge = false;
                                            lock (this.internalQueue)
                                            {
                                                if (this.internalQueue.Count > 0)
                                                    msgReceived = (MqttMsgBase)this.internalQueue.Peek();
                                            }

                                            // it is a PUBREC message
                                            if ((msgReceived != null) && (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBREC_TYPE))
                                            {
                                                // PUBREC message for the current PUBLISH message, send PUBREL, wait for PUBCOMP
                                                if (((MqttMsgPubrec)msgReceived).MessageId == ((MqttMsgPublish)msgInflight).MessageId)
                                                {
                                                    lock (this.internalQueue)
                                                    {
                                                        // received message processed
                                                        this.internalQueue.Dequeue();
                                                        acknowledge = true;
                                                    }

                                                    MqttMsgPubrel pubrel = new MqttMsgPubrel();
                                                    pubrel.MessageId = ((MqttMsgPublish)msgInflight).MessageId;

                                                    msgContext.State = MqttMsgState.WaitForPubcomp;
                                                    msgContext.Timestamp = Environment.TickCount;
                                                    msgContext.Attempt = 1;

                                                    this.Send(pubrel.GetBytes());

                                                    // update timeout
                                                    int msgTimeout = (this.settings.DelayOnRetry - (Environment.TickCount - msgContext.Timestamp));
                                                    timeout = (msgTimeout < timeout) ? msgTimeout : timeout;

                                                    // re-enqueue message
                                                    this.inflightQueue.Enqueue(msgContext);
                                                }
                                            }

                                            // current message not acknowledged
                                            if (!acknowledge)
                                            {
                                                // check timeout for receiving PUBREC since PUBLISH was sent
                                                if ((Environment.TickCount - msgContext.Timestamp) >= this.settings.DelayOnRetry)
                                                {
                                                    // max retry not reached, resend
                                                    if (msgContext.Attempt <= this.settings.AttemptsOnRetry)
                                                    {
                                                        msgContext.State = MqttMsgState.QueuedQos2;

                                                        // re-enqueue message
                                                        this.inflightQueue.Enqueue(msgContext);

                                                        // update timeout (0 -> reanalyze queue immediately)
                                                        timeout = 0;
                                                    }
                                                }
                                                else
                                                {
                                                    // re-enqueue message
                                                    this.inflightQueue.Enqueue(msgContext);

                                                    // update timeout
                                                    int msgTimeout = (this.settings.DelayOnRetry - (Environment.TickCount - msgContext.Timestamp));
                                                    timeout = (msgTimeout < timeout) ? msgTimeout : timeout;
                                                }
                                            }
                                        }
                                        break;

                                    case MqttMsgState.WaitForPubrel:

                                        // QoS 2, waiting for PUBREL of a PUBREC message sent
                                        if (msgContext.Flow == MqttMsgFlow.ToAcknowledge)
                                        {
                                            lock (this.internalQueue)
                                            {
                                                if (this.internalQueue.Count > 0)
                                                    msgReceived = (MqttMsgBase)this.internalQueue.Peek();
                                            }

                                            // it is a PUBREL message
                                            if ((msgReceived != null) && (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBREL_TYPE))
                                            {
                                                // PUBREL message for the current message, send PUBCOMP
                                                if (((MqttMsgPubrel)msgReceived).MessageId == ((MqttMsgPublish)msgInflight).MessageId)
                                                {
                                                    lock (this.internalQueue)
                                                    {
                                                        // received message processed
                                                        this.internalQueue.Dequeue();
                                                    }

                                                    MqttMsgPubcomp pubcomp = new MqttMsgPubcomp();
                                                    pubcomp.MessageId = ((MqttMsgPublish)msgInflight).MessageId;

                                                    this.Send(pubcomp.GetBytes());

                                                    // notify published message from broker and acknowledged
                                                    this.OnMqttMsgReceived(msgInflight);
                                                }
                                                else
                                                {
                                                    // re-enqueue message
                                                    this.inflightQueue.Enqueue(msgContext);
                                                }
                                            }
                                            else
                                            {
                                                // re-enqueue message
                                                this.inflightQueue.Enqueue(msgContext);
                                            }
                                        }
                                        break;

                                    case MqttMsgState.WaitForPubcomp:

                                        // QoS 2, waiting for PUBCOMP of a PUBREL message sent
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            acknowledge = false;
                                            lock (this.internalQueue)
                                            {
                                                if (this.internalQueue.Count > 0)
                                                    msgReceived = (MqttMsgBase)this.internalQueue.Peek();
                                            }

                                            // it is a PUBCOMP message
                                            if ((msgReceived != null) && (msgReceived.Type == MqttMsgBase.MQTT_MSG_PUBCOMP_TYPE))
                                            {
                                                // PUBCOMP message for the current message
                                                if (((MqttMsgPubcomp)msgReceived).MessageId == ((MqttMsgPublish)msgInflight).MessageId)
                                                {
                                                    lock (this.internalQueue)
                                                    {
                                                        // received message processed
                                                        this.internalQueue.Dequeue();
                                                        acknowledge = true;
                                                    }

                                                    // notify received acknowledge from broker of a published message
                                                    this.OnMqttMsgReceived(msgReceived);
                                                }
                                            }

                                            // current message not acknowledged
                                            if (!acknowledge)
                                            {
                                                // check timeout for receiving PUBCOMP since PUBREL was sent
                                                if ((Environment.TickCount - msgContext.Timestamp) >= this.settings.DelayOnRetry)
                                                {
                                                    // max retry not reached, resend
                                                    if (msgContext.Attempt < this.settings.AttemptsOnRetry)
                                                    {
                                                        msgContext.State = MqttMsgState.SendPubrel;

                                                        // re-enqueue message
                                                        this.inflightQueue.Enqueue(msgContext);

                                                        // update timeout (0 -> reanalyze queue immediately)
                                                        timeout = 0;
                                                    }
                                                }
                                                else
                                                {
                                                    // re-enqueue message
                                                    this.inflightQueue.Enqueue(msgContext);

                                                    // update timeout
                                                    int msgTimeout = (this.settings.DelayOnRetry - (Environment.TickCount - msgContext.Timestamp));
                                                    timeout = (msgTimeout < timeout) ? msgTimeout : timeout;
                                                }
                                            }
                                        }
                                        break;

                                    case MqttMsgState.SendPubrec:

                                        // TODO : impossible ? --> QueuedQos2 ToAcknowledge
                                        break;

                                    case MqttMsgState.SendPubrel:

                                        // QoS 2, PUBREL message to send to broker, state change to wait PUBCOMP
                                        if (msgContext.Flow == MqttMsgFlow.ToPublish)
                                        {
                                            MqttMsgPubrel pubrel = new MqttMsgPubrel();
                                            pubrel.MessageId = ((MqttMsgPublish)msgInflight).MessageId;

                                            msgContext.State = MqttMsgState.WaitForPubcomp;
                                            msgContext.Timestamp = Environment.TickCount;
                                            msgContext.Attempt++;
                                            // retry ? set dup flag
                                            if (msgContext.Attempt > 1)
                                                pubrel.DupFlag = true;

                                            this.Send(pubrel.GetBytes());

                                            // update timeout
                                            int msgTimeout = (this.settings.DelayOnRetry - (Environment.TickCount - msgContext.Timestamp));
                                            timeout = (msgTimeout < timeout) ? msgTimeout : timeout;

                                            // re-enqueue message
                                            this.inflightQueue.Enqueue(msgContext);
                                        }
                                        break;

                                    case MqttMsgState.SendPubcomp:
                                        // TODO : impossible ?
                                        break;
                                    case MqttMsgState.SendPuback:
                                        // TODO : impossible ? --> QueuedQos1 ToAcknowledge
                                        break;
                                    default:
                                        break;
                                }
                            }

                            // if calculated timeout is MaxValue, it means that must be Infinite (-1)
                            if (timeout == Int32.MaxValue)
                                timeout = Timeout.Infinite;
                        }
                    }
                }
            }
            catch (MqttCommunicationException)
            {
                this.Close();

#if BROKER
                this.OnMqttMsgDisconnected();
#endif
            }
        }

        /// <summary>
        /// Generate the next message identifier
        /// </summary>
        /// <returns>Message identifier</returns>
        private ushort GetMessageId()
        {
            if (this.messageIdCounter == 0)
                this.messageIdCounter++;
            else
                this.messageIdCounter = ((this.messageIdCounter % UInt16.MaxValue) != 0) ? (ushort)(this.messageIdCounter + 1) : (ushort)0;
            return this.messageIdCounter;
        }

        /// <summary>
        /// Finder class for PUBLISH message inside a queue
        /// </summary>
        internal class MqttMsgContextFinder
        {
            // PUBLISH message id
            internal ushort MessageId { get; set; }

            /// <summary>
            /// Constructor
            /// </summary>
            /// <param name="messageId">Message Id</param>
            internal MqttMsgContextFinder(ushort messageId)
            {
                this.MessageId = messageId;
            }

            internal bool Find(object item)
            {
                MqttMsgContext msgCtx = (MqttMsgContext)item;
                return ((msgCtx.Message.Type == MqttMsgBase.MQTT_MSG_PUBLISH_TYPE) &&
                        (((MqttMsgPublish)msgCtx.Message).MessageId == this.MessageId));

            }
        }
    }
}
