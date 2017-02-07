using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace MqttBroker
{
    /// <summary>
    /// Manager for topics and subscribers
    /// </summary>
    public class MqttSubscriberManager
    {
        // topic wildcards '+' and '#'
        private const string PLUS_WILDCARD = "+";
        private const string SHARP_WILDCARD = "#";

        // replace for wildcards '+' and '#' for using regular expression on topic match
        private const string PLUS_WILDCARD_REPLACE = @"[^/]+";
        private const string SHARP_WILDCARD_REPLACE = @".*";
        
        // subscribers list for each topic
        private Dictionary<string, List<MqttSubscription>> subscribers;

        /// <summary>
        /// Constructor
        /// </summary>
        public MqttSubscriberManager()
        {
            this.subscribers = new Dictionary<string, List<MqttSubscription>>();
        }

        /// <summary>
        /// Add a subscriber for a topic
        /// </summary>
        /// <param name="topic">Topic for subscription</param>
        /// <param name="qosLevel">QoS level for the topic subscription</param>
        /// <param name="client">Client to subscribe</param>
        public void Subscribe(string topic, byte qosLevel, MqttClientContext client)
        {
            string topicReplaced = topic.Replace(PLUS_WILDCARD, PLUS_WILDCARD_REPLACE).Replace(SHARP_WILDCARD, SHARP_WILDCARD_REPLACE);

            lock (this.subscribers)
            {
                // if the topic doesn't exist
                if (!this.subscribers.ContainsKey(topicReplaced))
                {
                    // create a new empty subscription list for the topic
                    List<MqttSubscription> list = new List<MqttSubscription>();
                    this.subscribers.Add(topicReplaced, list);
                }

                // query for check client already subscribed
                var query = from s in this.subscribers[topicReplaced]
                            where s.Client.ClientId == client.ClientId
                            select s;

                // if the client isn't already subscribed to the topic 
                if (query.Count() == 0)
                {
                    MqttSubscription subscription = new MqttSubscription()
                    {
                        Topic = topicReplaced,
                        QosLevel = qosLevel,
                        Client = client
                    };
                    // add subscription to the list for the topic
                    this.subscribers[topicReplaced].Add(subscription);
                }
            }
        }

        /// <summary>
        /// Remove a subscriber for a topic
        /// </summary>
        /// <param name="topic">Topic for unsubscription</param>
        /// <param name="client">Client to unsubscribe</param>
        public void Unsubscribe(string topic, MqttClientContext client)
        {
            string topicReplaced = topic.Replace(PLUS_WILDCARD, PLUS_WILDCARD_REPLACE).Replace(SHARP_WILDCARD, SHARP_WILDCARD_REPLACE);

            lock (this.subscribers)
            {
                // if the topic exists
                if (this.subscribers.ContainsKey(topicReplaced))
                {
                    // query for check client subscribed
                    var query = from s in this.subscribers[topicReplaced]
                                where s.Client.ClientId == client.ClientId
                                select s;

                    // if the client is subscribed for the topic
                    if (query.Count() > 0)
                    {
                        MqttSubscription subscription = query.First();

                        // remove subscription from the list for the topic
                        this.subscribers[topicReplaced].Remove(subscription);

                        // remove topic if there aren't subscribers
                        if (this.subscribers[topicReplaced].Count == 0)
                            this.subscribers.Remove(topicReplaced);
                    }
                }
            }
        }

        /// <summary>
        /// Remove a subscriber for all topics
        /// </summary>
        /// <param name="client">Client to unsubscribe</param>
        public void Unsubscribe(MqttClientContext client)
        {
            lock (this.subscribers)
            {
                List<string> topicToRemove = new List<string>();

                foreach (string topic in this.subscribers.Keys)
                {
                    // query for check client subscribed
                    var query = from s in this.subscribers[topic]
                                where s.Client.ClientId == client.ClientId
                                select s;

                    // if the client is subscribed for the topic
                    if (query.Count() > 0)
                    {
                        MqttSubscription subscription = query.First();

                        // remove subscription from the list for the topic
                        this.subscribers[topic].Remove(subscription);

                        // add topic to remove list if there aren't subscribers
                        if (this.subscribers[topic].Count == 0)
                            topicToRemove.Add(topic);
                    }
                }

                // remove topic without subscribers
                // loop needed to avoid exception on modify collection inside previous loop
                foreach (string topic in topicToRemove)
                    this.subscribers.Remove(topic);
            }
        }

        /// <summary>
        /// Get subscription list for a specified topic
        /// </summary>
        /// <param name="topic">Topic to get subscription list</param>
        /// <param name="qosLevel">QoS level requested</param>
        /// <returns>Subscription list</returns>
        public List<MqttSubscription> GetSubscriptions(string topic, byte qosLevel)
        {
            var query = from ss in this.subscribers
                        where (new Regex(ss.Key)).IsMatch(topic)    // check for topics based also on wildcard with regex
                        from s in this.subscribers[ss.Key]
                        where s.QosLevel == qosLevel                // check for subscriber only with a specified QoS level granted
                        select s;

            return query.ToList();
        }
    }

    /// <summary>
    /// MQTT subscription
    /// </summary>
    public class MqttSubscription
    {
        /// <summary>
        /// Topic of subscription
        /// </summary>
        public string Topic { get; set; }

        /// <summary>
        /// QoS level granted for the subscription
        /// </summary>
        public byte QosLevel { get; set; }

        /// <summary>
        /// Client related to the subscription
        /// </summary>
        public MqttClientContext Client { get; set; }
    }
}
