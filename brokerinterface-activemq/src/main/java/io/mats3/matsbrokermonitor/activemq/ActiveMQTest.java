package io.mats3.matsbrokermonitor.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import java.util.Enumeration;

/**
 * @author Endre Stølsvik 2021-12-19 00:32 - http://stolsvik.com/, endre@stolsvik.com
 */
public class ActiveMQTest {

    public static void main(String[] args) throws JMSException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        statisticsBroker(session);

        statisticsDestinations(session);
    }

    @SuppressWarnings("unchecked")
    private static void statisticsBroker(Session session) throws JMSException {
        Queue replyTo = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(replyTo);

        Topic request = session.createTopic("ActiveMQ.Statistics.Broker");
        MessageProducer producer = session.createProducer(request);
        Message msg = session.createMessage();
        msg.setJMSReplyTo(replyTo);
        producer.send(msg);

        MapMessage reply = (MapMessage) consumer.receive();

        System.out.println("--- BROKER");
        for (Enumeration<String> e = (Enumeration<String>) reply.getMapNames(); e.hasMoreElements();) {
            String name = e.nextElement().toString();
            System.out.println(name + "=" + reply.getObject(name));
        }
        producer.close();
        consumer.close();
    }

    @SuppressWarnings("unchecked")
    private static void statisticsDestinations(Session session) throws JMSException {
        Queue replyTo = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(replyTo);

        String queueName = "ActiveMQ.Statistics.Destination.>";

        Queue queryQueue = session.createQueue(queueName);
        MessageProducer producer = session.createProducer(queryQueue);
        Message msg = session.createMessage();
        msg.setJMSReplyTo(replyTo);
        producer.send(msg);
        producer.close();

        Topic queryTopic = session.createTopic(queueName);
        producer = session.createProducer(queryTopic);
        msg = session.createMessage();
        msg.setJMSReplyTo(replyTo);
        producer.send(msg);
        producer.close();

        while (true) {
            MapMessage reply = (MapMessage) consumer.receive();
            System.out.println("--- DESTINATION");
            for (Enumeration<String> e = (Enumeration<String>) reply.getMapNames(); e.hasMoreElements(); ) {
                String name = e.nextElement().toString();
                System.out.println(name + "=" + reply.getObject(name));
            }
        }
    }

}
