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
 * Tests basics of StatsQuerier.
 * 
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

        // :: ACT 0

        // Create the StatsQuerier
        ActiveMqBrokerStatsQuerierImpl statsQuerier = ActiveMqBrokerStatsQuerierImpl.create(connectionFactory);

        CountDownLatch[] latchX = new CountDownLatch[] { new CountDownLatch(1) };
        String[] correlationIdX = new String[1];
        boolean[] fullUpdateX = new boolean[1];
        boolean[] sameNode = new boolean[1];
        Consumer<ActiveMqBrokerStatsEvent> eventListener = statsEvent -> {
            log.info("### EVENT, correlationId: [" + statsEvent.getCorrelationId() + "].");
            correlationIdX[0] = statsEvent.getCorrelationId().orElse(null);
            fullUpdateX[0] = statsEvent.isFullUpdate();
            sameNode[0] = statsEvent.isStatsEventOriginatedOnThisNode();

            // Ping the latch
            latchX[0].countDown();
        };
        statsQuerier.registerListener(eventListener);
        statsQuerier.start();

        // :: ASSERT 0

        // We should get an update ASAP, and this should be a full, since none has ever been sent.
        boolean await = latchX[0].await(20, TimeUnit.SECONDS);
        Assert.assertTrue("Didn't get update from StatsQuerier.", await);
        log.info("###! StatsQuerier evidently got its first update.");

        Assert.assertNull(correlationIdX[0]);
        Assert.assertTrue(fullUpdateX[0]);
        Assert.assertTrue(sameNode[0]);

        // :: ACT 1

        // Force update, non-full
        latchX[0] = new CountDownLatch(1);
        String correlationId = UUID.randomUUID().toString();
        statsQuerier.forceUpdate(correlationId, false);

        // :: ASSERT 1

        await = latchX[0].await(20, TimeUnit.SECONDS);
        Assert.assertTrue("Didn't get update from StatsQuerier.", await);

        Assert.assertEquals(correlationId, correlationIdX[0]);
        Assert.assertFalse(fullUpdateX[0]);
        Assert.assertTrue(sameNode[0]);

        // :: ACT 2

        // Force update, full
        latchX[0] = new CountDownLatch(1);
        correlationId = UUID.randomUUID().toString();
        statsQuerier.forceUpdate(correlationId, true);

        // :: ASSERT 2

        await = latchX[0].await(20, TimeUnit.SECONDS);
        Assert.assertTrue("Didn't get update from StatsQuerier.", await);

        Assert.assertEquals(correlationId, correlationIdX[0]);
        Assert.assertTrue(fullUpdateX[0]);
        Assert.assertTrue(sameNode[0]);

        log.info("Got event from StatsQuerier");
        statsQuerier.close();

        // :: ASSERT contents.

        // Assert that the StatsQuerier has the broker-stats and destination-stats
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
