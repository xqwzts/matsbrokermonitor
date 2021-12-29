package io.mats3.matsbrokermonitor.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Endre Stølsvik 2021-12-25 15:36 - http://stolsvik.com/, endre@stolsvik.com
 */
public class Runner {
    private static Logger log = LoggerFactory.getLogger(Runner.class);

    public static void main(String[] args) throws InterruptedException {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        ActiveMqBrokerStatsQuerierImpl querier = ActiveMqBrokerStatsQuerierImpl.create(connectionFactory);
        ActiveMqMatsBrokerMonitor activeMqMatsBrokerInterface = new ActiveMqMatsBrokerMonitor(querier, "endre:");

        activeMqMatsBrokerInterface.registerListener(destinationUpdateEvent -> {
            log.info("Got update! "+destinationUpdateEvent);
        });
        activeMqMatsBrokerInterface.start();

        Thread.sleep(7 * 1000);
        querier.close();
        log.info("Exiting");
    }
}
