# MatsBrokerMonitor

*WORK IN PROGRESS!*

Monitors a message broker for Mats-relevant queues, topics and DLQs.

When using Mats - or really any messaging based infrastructure - you gain a massive advantage by having many of the errors in the total system "crop up" on the message broker. However, to catch these errors, you need to monitor it.

You want to monitor these three elements:
1. Queues not growing excessively: There should ideally always be zero messages in all queues.
2. Messages are actually consumed: It doesn't matter if there is only two messages on queue, if these are never consumed. This obviously happens if a queue does not have any consumers, but also if the consumers which are connected have become stuck.
3. Dead Letter Queues: "Poison" message which cannot be consumed even after a set of retries, will be put on a DLQ. All unprocessable messages must obviously be caught and investigated.

_(An interesting point is that the first and second bullet can often effectively be monitored by a single test: If the message at the head of the queue is old, it EITHER means that no-one is consuming messages, OR that the queue has become long enough that the messages gets old while waiting to be consumed.)_

In addition, for the DLQs, you'd want two more features:
1. Introspecting the messages on the DLQ, to be able to diagnose what has gone wrong, both by looking at the message itself, and also to find its traceId and correlate that with your distributed logging system to see what lead up to the problem.
2. Have a "retry again" feature. Often a message _was_ poison, but you have now fixed the underlying problem, e.g. some unavailable external resource that has now become available again, or a data consistency problem that was asserted and caught and thus DLQ'ed, but which was then manually fixed.

_(Just since it is important enough to be mentioned right away: You should always configure your message broker to use an_ Individual Dead Letter Queue _strategy. This is strangely not the default for any of the common brokers, instead using a single DLQ which all unprocessable messages for any queue end up on. But all brokers have configurable ways to get a separate DLQ for every Queue, using the common DLQ name pattern ```"DLQ.{originalQueueName}"```. You really want this if you employ Mats, at least for the Mats destinations!)_

The introspection of which queues exist on the broker and in particular how many messages are on the queues is not a part of the JMS standard, or any other Message System standard for that matter, so we need some broker-specific tools for this. For example ActiveMQ has several options: JMS, Jolokia, "Advisory Messages" and a "Statistics Plugin" that can be enabled and then afterwards queried using the messaging system itself.