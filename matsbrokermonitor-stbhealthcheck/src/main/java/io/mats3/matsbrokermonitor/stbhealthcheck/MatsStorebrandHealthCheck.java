package io.mats3.matsbrokermonitor.stbhealthcheck;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.storebrand.healthcheck.Axis;
import com.storebrand.healthcheck.CheckSpecification.CheckResult;
import com.storebrand.healthcheck.HealthCheckMetadata;
import com.storebrand.healthcheck.HealthCheckMetadata.HealthCheckMetadataBuilder;
import com.storebrand.healthcheck.HealthCheckRegistry;
import com.storebrand.healthcheck.HealthCheckRegistry.RegisteredHealthCheck;
import com.storebrand.healthcheck.Responsible;

import io.mats3.MatsEndpoint;
import io.mats3.MatsFactory;
import io.mats3.MatsStage;
import io.mats3.MatsStage.StageConfig;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.UpdateEvent;
import io.mats3.matsbrokermonitor.broadcastreceiver.MatsBrokerMonitorBroadcastReceiver;

/**
 * Health checks for a MatsFactory using the <a href="https://github.com/storebrand/healthcheck">Storebrand
 * HealthCheck</a> system, depending on MatsBrokerMonitor's <code>'matsbrokermonitor-broadcastreceiver'</code> plugin
 * being installed on the MatsFactory.
 * <p>
 * When you have a {@link HealthCheckRegistry} instance, you can provide that to the static method
 * {@link #registerMatsHealthCheck(HealthCheckRegistry, MatsFactory, CharSequence...)} to get a decent health check for
 * Queues (old messages) and DLQs (messages present).
 * 
 * @author Endre Stølsvik 2024-05-06 18:32 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsStorebrandHealthCheck {

    private static final Logger log = LoggerFactory.getLogger(MatsStorebrandHealthCheck.class);

    private static final double MIDDLE_AGED_MESSAGES_MINUTES = 2;

    private static final double OLD_MESSAGES_MINUTES = 10;
    private static final double VERY_OLD_MESSAGES_MINUTES = 30;

    private static final double SLOW_OLD_MESSAGES_MINUTES = 20;
    private static final double SLOW_VERY_OLD_MESSAGES_MINUTES = 60;

    private static final double UPDATE_INTERVAL_TOO_LONG_MINUTES = 15;

    /**
     * Installs a HealthCheck on the provided {@link HealthCheckRegistry} for the provided {@link MatsFactory}, checking
     * the health of the MatsFactory's Queues (old messages) and DLQs (messages present).
     *
     * @param healthCheckRegistry
     *            The HealthCheckRegistry to register the health check with.
     * @param matsFactory
     *            The MatsFactory to check health for.
     * @param responsible
     *            Responsible parties for the health check, if not provided, defaults to "Developers".
     */
    public static void registerMatsHealthCheck(HealthCheckRegistry healthCheckRegistry, MatsFactory matsFactory,
            CharSequence... responsible) {

        String name = "Mats3 - HealthChecks for MatsFactory '" + matsFactory.getFactoryConfig().getName() + "'";
        List<RegisteredHealthCheck> registeredHealthChecks = healthCheckRegistry.getRegisteredHealthChecks();
        for (RegisteredHealthCheck registeredHealthCheck : registeredHealthChecks) {
            if (name.equals(registeredHealthCheck.getMetadata().name)) {
                log.error("You're trying to register the same HealthCheck twice for MatsFactory [" + matsFactory + "]."
                        + " Ignoring this second time.");
                return;
            }
        }

        if (responsible.length == 0) {
            responsible = new String[] { Responsible.DEVELOPERS.toString() };
        }
        final CharSequence[] responsibleF = responsible;
        HealthCheckMetadataBuilder meta = HealthCheckMetadata.builder();
        meta.name(name);
        meta.description("Checks health of Mats3 Endpoint's Queues and DLQs");
        meta.sync(true);
        healthCheckRegistry.registerHealthCheck(meta.build(), checkSpec -> {
            var atomicBroadcast = new AtomicReference<MatsBrokerMonitorBroadcastReceiver>();
            var lastUpdated = new AtomicLong(System.currentTimeMillis());
            var updateEverGotten = new AtomicBoolean();
            Consumer<UpdateEvent> updateListener = updateEvent -> {
                updateEverGotten.set(true);
                lastUpdated.set(updateEvent.getStatisticsUpdateMillis());
            };

            // :: Asserting that we have the broadcast receiver plugin installed (and hooked), and receiving updates.
            checkSpec.check(responsibleF,
                    Axis.of(Axis.MANUAL_INTERVENTION_REQUIRED, Axis.DEGRADED_PARTIAL, Axis.CRITICAL_WAKE_PEOPLE_UP),
                    checkContext -> {
                        var broadcastReceiver = atomicBroadcast.get();
                        if (broadcastReceiver == null) {
                            var list = matsFactory.getFactoryConfig().getPlugins(
                                    MatsBrokerMonitorBroadcastReceiver.class);
                            broadcastReceiver = list.isEmpty() ? null : list.get(0);
                            if (broadcastReceiver != null) {
                                boolean wasSet = atomicBroadcast.compareAndSet(null, broadcastReceiver);
                                if (wasSet) {
                                    broadcastReceiver.registerListener(updateListener);
                                }
                            }
                        }
                        if (broadcastReceiver == null) {
                            return checkContext.fault("MISSING: '" + MatsBrokerMonitorBroadcastReceiver.class
                                    .getSimpleName() + "' plugin not installed!")
                                    .turnOffAxes(Axis.DEGRADED_MINOR, Axis.DEGRADED_PARTIAL,
                                            Axis.CRITICAL_WAKE_PEOPLE_UP); // "Manual intervention" is enough.
                        }
                        checkContext.text("MBMBroadcastReceiver is installed on the MatsFactory!");
                        long lastUpdate = lastUpdated.get();
                        if (!updateEverGotten.get()) {
                            return checkContext.ok("We have never, in "
                                    + durationFormat(Duration.ofMillis(System.currentTimeMillis() - lastUpdate))
                                    + ", gotten an update yet. Assuming dev or startup.");
                        }
                        long millisSinceUpdate = System.currentTimeMillis() - lastUpdate;

                        if (millisSinceUpdate > (UPDATE_INTERVAL_TOO_LONG_MINUTES * 60 * 1000)) {
                            return checkContext.fault("Over " + UPDATE_INTERVAL_TOO_LONG_MINUTES + " minutes"
                                    + " since last update! Was " + durationFormat(Duration.ofMillis(millisSinceUpdate))
                                    + " ago.").turnOffAxes(Axis.DEGRADED_PARTIAL, Axis.CRITICAL_WAKE_PEOPLE_UP);
                        }

                        return checkContext.ok("We're receiving updates, last update was "
                                + durationFormat(Duration.ofMillis(millisSinceUpdate)) + " ago.");
                    });

            // :: DLQs
            checkSpec.staticText(" "); // Space to get an empty line.
            checkSpec.check(responsibleF,
                    Axis.of(Axis.MANUAL_INTERVENTION_REQUIRED, Axis.DEGRADED_PARTIAL, Axis.CRITICAL_WAKE_PEOPLE_UP),
                    checkContext -> {

                        long totalDlqs = 0;
                        int numBadStages = 0;
                        List<MatsEndpoint<?, ?>> endpoints = matsFactory.getEndpoints();
                        for (MatsEndpoint<?, ?> endpoint : endpoints) {
                            List<? extends MatsStage<?, ?, ?>> stages = endpoint.getStages();
                            for (MatsStage<?, ?, ?> stage : stages) {
                                StageConfig<?, ?, ?> stageConfig = stage.getStageConfig();
                                String stageId = stageConfig.getStageId();
                                MatsBrokerDestination dlqDest = stageConfig.getAttribute(
                                        StageDestinationType.DEAD_LETTER_QUEUE.getStageAttribute());
                                MatsBrokerDestination npiaDlqDest = stageConfig.getAttribute(
                                        StageDestinationType.DEAD_LETTER_QUEUE_NON_PERSISTENT_INTERACTIVE
                                                .getStageAttribute());

                                long dlqs = dlqDest != null
                                        ? dlqDest.getNumberOfQueuedMessages()
                                        : 0;
                                long npiaDlqs = npiaDlqDest != null
                                        ? npiaDlqDest.getNumberOfQueuedMessages()
                                        : 0;
                                long sumDlqs = dlqs + npiaDlqs;

                                // ?: Have we printed
                                if (sumDlqs > 0) {
                                    if (numBadStages == 0) {
                                        checkContext.text("== Stages with DLQed messages:");
                                    }
                                    long age = dlqDest != null
                                            ? dlqDest.getHeadMessageAgeMillis().orElse(0)
                                            : 0;
                                    long ageNpia = npiaDlqDest != null
                                            ? npiaDlqDest.getHeadMessageAgeMillis().orElse(0)
                                            : 0;
                                    long maxAge = Math.max(age, ageNpia);

                                    checkContext.text(" - " + stageId + ": " + sumDlqs
                                            + (sumDlqs > 1 ? " msgs" : " msg") + "  (" + durationFormat(Duration
                                                    .ofMillis(maxAge)) + ")");

                                    numBadStages++;
                                }
                                totalDlqs += sumDlqs;
                            }
                        }

                        // ?: Are we clean?
                        if (numBadStages == 0) {
                            // -> Yes, clean - no DLQs.
                            return checkContext.ok("== DLQs: All good, no stages with DLQed messages!");
                        }
                        // -> We have DLQs - let's warn, with differing level of urgency.
                        CheckResult fault = checkContext.fault("There are " + numBadStages + " stage"
                                + (numBadStages > 1 ? "s" : "") + " with DLQed messages, total: " + totalDlqs);

                        if (totalDlqs > 1000) {
                            checkContext.text("There are >1000 DLQed messages total: CRITICAL!");
                        }
                        else if (totalDlqs > 100) {
                            checkContext.text("There are >100 DLQed messages total: DEGRADED_PARTIAL!");
                            fault.turnOffAxes(Axis.CRITICAL_WAKE_PEOPLE_UP);
                        }
                        else {
                            checkContext.text("There are <100 DLQed messages total: DEGRADED_MINOR.");
                            fault.turnOffAxes(Axis.DEGRADED_PARTIAL, Axis.CRITICAL_WAKE_PEOPLE_UP);
                        }
                        checkContext.text("You must go the MatsBrokerMonitor to inspect, reissue or delete these!");
                        return fault;
                    });

            // :: Old messages:
            checkSpec.staticText(" "); // Space to get an empty line.
            checkSpec.check(responsibleF,
                    Axis.of(Axis.DEGRADED_PARTIAL, Axis.CRITICAL_WAKE_PEOPLE_UP),
                    checkContext -> {

                        boolean veryOldPresent = false;
                        long oldest = 0;
                        int numBadStages = 0;
                        boolean headerPrinted = false;
                        List<MatsEndpoint<?, ?>> endpoints = matsFactory.getEndpoints();
                        for (MatsEndpoint<?, ?> endpoint : endpoints) {
                            boolean slowEndpoint = endpoint.getEndpointConfig().getEndpointId().contains(".slow.");

                            List<? extends MatsStage<?, ?, ?>> stages = endpoint.getStages();
                            for (MatsStage<?, ?, ?> stage : stages) {
                                StageConfig<?, ?, ?> stageConfig = stage.getStageConfig();
                                String stageId = stageConfig.getStageId();
                                MatsBrokerDestination dlqDest = stageConfig.getAttribute(
                                        StageDestinationType.STANDARD.getStageAttribute());
                                MatsBrokerDestination npiaDlqDest = stageConfig.getAttribute(
                                        StageDestinationType.NON_PERSISTENT_INTERACTIVE.getStageAttribute());

                                long age = dlqDest != null
                                        ? dlqDest.getHeadMessageAgeMillis().orElse(0)
                                        : 0;
                                long ageNpia = npiaDlqDest != null
                                        ? npiaDlqDest.getHeadMessageAgeMillis().orElse(0)
                                        : 0;
                                long maxAge = Math.max(age, ageNpia);

                                double ageMinutes = maxAge / (60 * 1000d);

                                double oldLimit = slowEndpoint ? SLOW_OLD_MESSAGES_MINUTES
                                        : OLD_MESSAGES_MINUTES;
                                double veryOldLimit = slowEndpoint ? SLOW_VERY_OLD_MESSAGES_MINUTES
                                        : VERY_OLD_MESSAGES_MINUTES;

                                if (ageMinutes > MIDDLE_AGED_MESSAGES_MINUTES) {
                                    if (!headerPrinted) {
                                        checkContext.text("");
                                        checkContext.text("== Stages with old head message:");
                                        headerPrinted = true;
                                    }
                                    long msgs = dlqDest != null
                                            ? dlqDest.getNumberOfQueuedMessages()
                                            : 0;
                                    long npiaMsgs = npiaDlqDest != null
                                            ? npiaDlqDest.getNumberOfQueuedMessages()
                                            : 0;
                                    long sumMsgs = msgs + npiaMsgs;

                                    String maxAgeFormat = durationFormat(Duration.ofMillis(maxAge))
                                            + "  (" + sumMsgs + (sumMsgs > 1 ? " msgs)" : " msg)");

                                    if (ageMinutes > veryOldLimit) {
                                        // -> Very old.
                                        veryOldPresent = true;
                                        numBadStages++;
                                        checkContext.text(" - VERY OLD! " + (slowEndpoint ? "(slow) " : "")
                                                + stageId + ": " + maxAgeFormat);
                                    }
                                    else if (ageMinutes > oldLimit) {
                                        // -> Old
                                        numBadStages++;
                                        checkContext.text(" - OLD! " + (slowEndpoint ? "(slow) " : "")
                                                + stageId + ": " + maxAgeFormat);
                                    }
                                    else {
                                        // -> Ok - but potentially going bad
                                        // This is just for info, not yet counted as bad.
                                        checkContext.text(" - Close! " + (slowEndpoint ? "(slow) " : "")
                                                + stageId + ": " + maxAgeFormat);
                                    }

                                }

                                // Keep tally of oldest
                                oldest = Math.max(oldest, maxAge);
                            }
                        }

                        // ?: Are we clean?
                        if (numBadStages == 0) {
                            // -> Yes, clean - no DLQs.
                            return checkContext.ok("== Old: All good, no stages with too old messages!");
                        }
                        // E-> No, we have old messages

                        CheckResult fault = checkContext.fault("There are " + numBadStages + " stage"
                                + (numBadStages > 1 ? "s" : "") + " with messages above age limit,"
                                + " max age: " + durationFormat(Duration.ofMillis(oldest)));
                        if (!veryOldPresent) {
                            checkContext.text("There are OLD messages: DEGRADED_PARTIAL!");
                            fault.turnOffAxes(Axis.CRITICAL_WAKE_PEOPLE_UP);
                        }
                        else {
                            checkContext.text("There are VERY OLD messages: CRITICAL!");
                        }
                        checkContext.text("You must find out why the consumers are slow, and/or why queues build up!");

                        return fault;
                    });

        });
    }

    /**
     * Formats the given duration to a human readable string.
     */
    private static String durationFormat(Duration runningTime) {
        Duration left = runningTime;

        long days = left.toDays();
        left = left.minusDays(days);

        long hours = left.toHours();
        left = left.minusHours(hours);

        long minutes = left.toMinutes();
        left = left.minusMinutes(minutes);

        long seconds = left.getSeconds();

        if (days != 0) {
            return String.format("%sd %sh %sm %ss", days, hours, minutes, seconds);
        }
        else if (hours != 0) {
            return String.format("%sh %sm %ss", hours, minutes, seconds);
        }
        else if (minutes != 0) {
            return String.format("%sm %ss", minutes, seconds);
        }
        else {
            return String.format("%ss", seconds);
        }
    }

}
