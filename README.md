# MatsBrokerMonitor

Monitoring of a message broker for Mats-relevant Queues, Topics and Dead Letter Queues. The project provides both an API
and implementation to gather such information, and an embeddable HTML GUI to introspect and act on messages.

When using Mats - or really any messaging based infrastructure - you gain a massive advantage by having many of the
errors in the total system "crop up" on the message broker. However, to catch these errors, you need to monitor it.

You want to monitor these three elements:

1. Queues not growing excessively: There should ideally always be zero messages in all queues.
2. Messages are actually consumed: It doesn't matter if there is only a few messages on a queue, if these are never
   consumed. This obviously happens if a queue does not have any consumers, but also if the consumers which are
   connected have become stuck.
3. Dead Letter Queues: "Poison" message which cannot be consumed even after a set of retries (i.e. the processing fails
   every time), will be put on a DLQ. All unprocessable messages must be investigated.

_(An interesting point is that the first and second bullet can often effectively be monitored by a single test: If the
message at the head of the queue is old, it EITHER means that no-one is consuming messages, OR that the queue has become
long enough that the messages gets old while waiting to be consumed.)_

In addition, for the DLQs, you'd want two more features:

1. Introspecting the messages on the DLQ, to be able to diagnose what has gone wrong, both by looking at the message
   itself, and also to find its traceId and correlate that with your distributed logging system to see what lead up to
   the problem.
2. Have a _reissue_ feature. Often a message _was_ poison, but you have now fixed the underlying problem, e.g. some
   unavailable external resource that has now become available again, or a data consistency problem that was asserted
   and caught and thus DLQ'ed, but which was then manually fixed.

_(Just since it is important enough to be mentioned right away: You should always configure your message broker to use
an_ Individual Dead Letter Queue _strategy. This is strangely not the default for any of the common brokers, instead
using a single DLQ which all unprocessable messages for any queue end up on. But all brokers have configurable ways to
get a separate DLQ for every Queue, using the common DLQ name pattern `"DLQ.{originalQueueName}"`. You really want this
if you employ Mats, at least for the Mats destinations!)_

The first set of requirements, i.e. which queues and DLQs exist on the broker, how many messages are on the queues, and
the age of the first message, is not a part of the JMS standard, or any other Message System standard for that matter,
so we need some broker-specific tools for this. ActiveMQ has several options: JMX, Jolokia, _Advisory Messages_ and the
_Statistics Plugin_ that can be enabled and then afterwards queried using the messaging system itself. These functions
are defined in the `MatsBrokerMonitor'` interface. The implementation for ActiveMQ, `ActiveMqMatsBrokerMonitor`, relies
on the installation of the Statistics Plugin, and queries this over JMS.

_(Compared to Mats, which works with both Apache Artemis and Apache ActiveMQ as message broker, there is currently no
such MatsBrokerMonitor implementation for Artemis. This will hopefully soon come.)_

The second set of requirements can be fulfilled by the JMS API itself, using the `QueueBrowser` feature of JMS to
inspect a queue, and JMS consume and produce to move a message from a DLQ back onto the incoming queue for the relevant
Mats stage. This functionality thus works across any broker that implements the JMS API. And is defined in the
`MatsBrokerBrowseAndActions`, and the JMS implementation is `JmsMatsBrokerBrowseAndActions`.