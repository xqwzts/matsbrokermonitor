We want three things:
1. A full list of queues and topics on the broker, with their queue sizes.
   1. Preferably the age of the "head message", i.e. the first message on the queue.
2. Ability to introspect the messages on those queues/topics.
3. Move a message from a DLQ back to the original Queue.

The second and third point can be done utilizing standard JMS features: QueueBrowser to introspect the queues, and Consumer (with selector) and Producer to move from DLQ to original Queue.

However, for monitoring the queue sizes, there is no standardized method.

ActiveMQ can be monitored several ways, detailed here: https://activemq.apache.org/how-can-i-monitor-activemq 

These are:
* **JMX**: https://activemq.apache.org/jmx
* **Jolokia**: JMX over HTTP, startup log shows: ```INFO | ActiveMQ Jolokia REST API available at http://127.0.0.1:8161/api/jolokia/```
  * **HawtIO**, visualization application which uses Jolokia, and have plugin for ActiveMQ
* The **Web Console** - but this isn't very relevant for our usage.
* **Advisory Messages**: Event based messages emitted by ActiveMQ over specific Topics, some which are default enabled. Includes queue/topic created, expired message on Queue/Topic etc. Also MessageDelivered (default not enabled). **Interesting one is MessageDLQd**, which is emitted when a message is DLQd on queue/topic: https://activemq.apache.org/advisory-message
* **StatisticsPlugin**: A plugin that must be enabled, which gives ability to send a message to the broker on specific destinations, and which replies with stats to "replyTo" topic specified. https://activemq.apache.org/statisticsplugin
* **API/Plugin directly on broker**: If we have "JVM access", there are plenty of options. https://activemq.apache.org/developing-plugins

From this list, the StatisticsPlugin seems the most non-intrusive, utilizing the same message queue fabric that Mats itself runs on. Possibly also employing the MessageDLQd advisory message, to quickly realize that a message has been DLQd.

However, it isn't ideal: Firstly, it isn't default - you must configure the broker to have it enabled. Secondly, the "API" provided via this messaging interface isn't optimal wrt. our needs: We would really want a single message containing an exhaustive list with queue/topic names, and their queue size. With the statistics plugin, there is no specific list - but we can send a _wildcard pattern_ query, which results in multiple messages being sent back, one for each queue/topic. The problem then, aside from heavy overhead, is to know when the result is finished - you don't know how many messages to expect. We'll therefore have to use some kind of "probabilistic" logic, where we periodically query for "gimme all", and then just consume whatever we get back - dynamically piecing together our understanding of the situation. If a queue hasn't gotten updates in a while, it is probably gone.

**Note: The API/Plugin route isn't ruled out**: Since we will need the user/developer to configure ActiveMQ to enable the StatisticsBroker plugin anyway, AND since we _highly_ recommend that the "specific DLQ per Queue" policy is enabled thus requiring config anyway, it wouldn't make much difference to instead of configuring the StatisticsBroker, rather install a special jar and enable its contained plugin. We could then get a fully bespoke message-based API tailored to our needs - possibly also standardized between brokers. 



