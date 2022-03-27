package io.mats3.matsbrokermonitor.activemq;

import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerier.ActiveMqBrokerStatsEvent;
import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerier.BrokerStatsDto;
import io.mats3.matsbrokermonitor.activemq.ActiveMqBrokerStatsQuerier.DestinationStatsDto;
import io.mats3.test.broker.MatsTestBroker;

/**
 * @author Endre Stølsvik 2022-03-27 11:38 - http://stolsvik.com/, endre@stolsvik.com
 */
public class TestStatsQuerier {
    private static final Logger log = LoggerFactory.getLogger(TestStatsQuerier.class);

    @Test
    public void testQuerier() throws JMSException, InterruptedException {
        // :: ARRANGE

        MatsTestBroker matsTestBrokerUnique = MatsTestBroker.createUniqueInVmActiveMq();
        ConnectionFactory connectionFactory = matsTestBrokerUnique.getConnectionFactory();

        // Make some "Mats.queues"
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        String matsQueue1 = "mats.Queue1.test";
        String matsQueue2 = "mats.Queue2.test";

        Queue queue1 = session.createQueue(matsQueue1);
        Queue queue2 = session.createQueue(matsQueue2);

        // Send some messages to those queues
        MessageProducer nullProducer = session.createProducer(null);

        Message message1 = session.createMessage();
        Message message2 = session.createMessage();

        nullProducer.send(queue1, message1);
        nullProducer.send(queue2, message2);

        // Should block until messages delivered to queues
        session.commit();

        // :: ACT

        // Create the StatsQuerier
        ActiveMqBrokerStatsQuerierImpl statsQuerier = ActiveMqBrokerStatsQuerierImpl.create(connectionFactory);

        String correlationId = UUID.randomUUID().toString();

        CountDownLatch latch = new CountDownLatch(1);
        Consumer<ActiveMqBrokerStatsEvent> eventListener = statsEvent -> {
            log.info("### EVENT, correlationId: [" + statsEvent.getCorrelationId()
                    + "], what we sent:[" + correlationId + "]");
            if (statsEvent.getCorrelationId().isPresent() && correlationId.equals(statsEvent.getCorrelationId()
                    .get())) {
                latch.countDown();
            }
        };
        statsQuerier.registerListener(eventListener);
        statsQuerier.start();
        statsQuerier.forceUpdate(correlationId);

        boolean await = latch.await(20, TimeUnit.SECONDS);
        Assert.assertTrue("Didn't get update from StatsQuerier.", await);

        log.info("Got event from StatsQuerier");
        statsQuerier.close();

        // :: ASSERT

        Optional<BrokerStatsDto> brokerStatsO = statsQuerier.getCurrentBrokerStatsDto();
        Assert.assertTrue("BrokerStats not present", brokerStatsO.isPresent());
        ConcurrentNavigableMap<String, DestinationStatsDto> destinationStats = statsQuerier
                .getCurrentDestinationStatsDtos();
        Assert.assertFalse("DestinationStats not present", destinationStats.isEmpty());

        BrokerStatsDto brokerStatsDto = brokerStatsO.get();
        log.info("BrokerStats: \n" + brokerStatsDto.toJson());

        // Assert that broker-stats-specific elements are present
        Assert.assertNotEquals("", brokerStatsDto.dataDirectory);
        Assert.assertNotEquals("", brokerStatsDto.vm);
        // Stats-common:
        Assert.assertNotEquals("", brokerStatsDto.brokerName);

        log.info("DestinationStats: ");
        for (Entry<String, DestinationStatsDto> destinationEntry : destinationStats.entrySet()) {
            DestinationStatsDto destinationStatsDto = destinationEntry.getValue();
            log.info("Destination [" + destinationEntry.getKey() + "]:\n" + destinationStatsDto.toJson());
            // Assert that destination-stats-specific elements are present
            Assert.assertNotEquals("", destinationStatsDto.destinationName);
            // Stats-common:
            Assert.assertNotEquals("", destinationStatsDto.brokerName);
        }

        // Assert that we have the two "Mats destinations" that we created
        assertMatsDestination(destinationStats.get(queue1.toString()));
        assertMatsDestination(destinationStats.get(queue2.toString()));
    }

    private void assertMatsDestination(DestinationStatsDto destStatsQueue1) {
        // .. there should be 1 message in wait
        Assert.assertEquals(1, destStatsQueue1.size);
        // .. there should be first message timestamp for it
        Assert.assertTrue(destStatsQueue1.firstMessageTimestamp.isPresent());
        // .. this timestamp should be earlier than now, and close to now.
        long difference = System.currentTimeMillis() - destStatsQueue1.firstMessageTimestamp.get().toEpochMilli();
        Assert.assertTrue("should be after or at now", difference >= 0);
        Assert.assertTrue("should be less than 5 sec ago", difference < 5000);
    }
}
