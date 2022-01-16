The parts of monitoring the Mats fabric broker that can be achieved using standard JMS API.

* Browse queues and topics, i.e. looking at the messages on the queue, in particular the DLQs
  * With this, also introspect the Mats messages, that is, if employing the MatsTrace envelope.
* Delete individual messages: Just consume them, using a consumer with a message filter.
* Purge a queue: Consume all messages, dumping them
* Move messages, typically from a DLQ to the original queue: Consume (with a message filter), and put them onto the destination queue, within a transaction.
* Move all messages: .. same