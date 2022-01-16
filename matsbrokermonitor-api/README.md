# The monitoring interface


**NOTICE!
Several of the brokers have Prometheus interfaces. These might actually be possible to "hijack".**

Here's for Artemis - but that uses the Promethus JMX exporter, which kinda defeats the point: https://stackoverflow.com/questions/37162532/how-to-monitor-activemq-artemis

Artemis-example config for Prometheus JMX exporter: https://github.com/prometheus/jmx_exporter/blob/4eb95cd389ad26b43f48d54f281c928818f04d20/example_configs/artemis-2.yml


## ActiveMQ is (being) implemented.
Module 'brokerinterface-activemq'. Requires that the StatisticsBrokerPlugin is installed. Queries over JMS.


## Other brokers:

### Artemis
classes ActiveMQServerControl and QueueControl.

These can be queried over the JMX, JMX-over-Jolokia HTTP, **and JMX-over-JMS** using "CoreMessages".

Seems like it can only fetch a single property for each message, so there will be LOTS of back and forths?!

Link here, search for "Using Management Message API":
https://activemq.apache.org/components/artemis/documentation/latest/management.html

Relevant StackOverflow question, rehashing what the docs says - but also a complete code example:
https://stackoverflow.com/questions/49225634/how-to-get-queue-size-depth-in-artemis

In QueueControl, there is both ```messageCount()```,
```getFirstMessageTimestamp()```, ```getFirstMessageAge()``` and
```getDeliveringCount()```, which seems to be "in flight".


### RabbitMQ

Seems like HTTP API is way to go.

#### Head message timestamp
Head-message-time is actually present (in seconds):
https://stackoverflow.com/questions/47768901/how-to-convert-rabbit-head-message-timestamp-to-java-localdatetime
.. which states that it is possible to get this using the HTTP API.

Rabbitmqctl: https://www.rabbitmq.com/rabbitmqctl.8.html#head_message_timestamp  <- head_message_timestamp.

Mentioned here, again pointing to plugin:
https://github.com/deadtrickster/prometheus_rabbitmq_exporter/issues/15

The RabbitMQ Message Timestamp Plugin, which adds timestamp to all messages:
https://github.com/rabbitmq/rabbitmq-message-timestamp

The HTTP API:
https://rawcdn.githack.com/rabbitmq/rabbitmq-management/v3.7.0/priv/www/api/index.html

NOTICE: _"Any field which can be returned by a command of the form rabbitmqctl list_something will also be returned in the equivalent part of the HTTP API, so all those keys are not documented here."_

The relevant rabbitmqctl command is "list_queues", so there we go.

#### Queue size and possibly in flight:
HTTP API:
https://www.rabbitmq.com/monitoring.html#queue-metrics
