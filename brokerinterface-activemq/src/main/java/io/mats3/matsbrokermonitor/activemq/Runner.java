package io.mats3.matsbrokermonitor.activemq;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;

/**
 * @author Endre Stølsvik 2021-12-25 15:36 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Runner {
    private static Logger log = LoggerFactory.getLogger(Runner.class);

    public static void main(String[] args) throws InterruptedException {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory();

        MatsBrokerMonitor matsBrokerInterface = ActiveMqMatsBrokerMonitor.create(connectionFactory);

        matsBrokerInterface.registerListener(destinationUpdateEvent -> {
            log.info("Got update! " + destinationUpdateEvent);
            destinationUpdateEvent.getNewOrUpdatedDestinations().forEach((fqName, matsBrokerDestination) -> log.info(
                    ".. new/updated: [" + fqName + "] = [" + matsBrokerDestination + "]"));
        });
        matsBrokerInterface.start();

        Thread.sleep(30 * 1000);
        log.info("FORCING UPDATE!");
        matsBrokerInterface.forceUpdate();
        Thread.sleep(5 * 60 * 1000);

        log.info("Exiting, closing MatsBrokerInterface");
        matsBrokerInterface.close();
    }
}
