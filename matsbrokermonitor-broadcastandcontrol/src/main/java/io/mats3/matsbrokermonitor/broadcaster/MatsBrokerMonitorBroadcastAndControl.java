package io.mats3.matsbrokermonitor.broadcaster;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mats3.MatsEndpoint;
import io.mats3.MatsFactory;
import io.mats3.MatsFactory.MatsPlugin;
import io.mats3.MatsInitiator;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerInfo;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerInfoDto;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestinationDto;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.UpdateEvent;

/**
 * Rather simple utility which {@link MatsBrokerMonitor#registerListener(Consumer) subscribes} to {@link UpdateEvent}s
 * from the {@link MatsBrokerMonitor}, publishing them to the Mats fabric on the topic
 * {@link #BROADCAST_UPDATE_EVENT_TOPIC_ENDPOINT_ID}.
 * <p>
 * <b>Note that life cycling of the provided MatsBrokerMonitor is handled by this class:</b> When the instance of this
 * class is {@link #start(MatsFactory) started} by the MatsFactory, it will invoke {@link MatsBrokerMonitor#start()
 * start} on the MatsBrokerMonitor, and when this instance is {@link #close() closed} by the MatsFactory, it will
 * {@link MatsBrokerMonitor#close() close} the MatsBrokerMonitor.
 *
 * 
 * @author Endre Stølsvik 2022-11-12 10:33 - http://stolsvik.com/, endre@stolsvik.com
 */
public class MatsBrokerMonitorBroadcastAndControl implements Closeable, MatsPlugin {
    private static final Logger log = LoggerFactory.getLogger(MatsBrokerMonitorBroadcastAndControl.class);

    /**
     * The EndpointId to which the update event is published.
     * <p>
     * Value is <code>"mats.MatsBrokerMonitorBroadcastUpdate"</code>.
     */
    public static final String BROADCAST_UPDATE_EVENT_TOPIC_ENDPOINT_ID = "mats.MatsBrokerMonitor.broadcastUpdate";

    /**
     * The EndpointId to which one may request operations, currently forceUpdate.
     * <p>
     * Value is <code>"mats.MatsBrokerMonitorControl"</code>.
     */
    public static final String MATSBROKERMONITOR_CONTROL = "mats.MatsBrokerMonitor.control";

    /**
     * Copied from MatsInterceptable.MatsLoggingInterceptor
     * <p>
     * We don't want all these updates to be logged on all the JVMs carrying MatsFactories, as it will be annoying noise
     * without much merit.
     */
    private static final String SUPPRESS_LOGGING_TRACE_PROPERTY_KEY = "mats.SuppressLogging";

    private final MatsBrokerMonitor _matsBrokerMonitor;

    /**
     * Creates a {@link MatsBrokerMonitorBroadcastAndControl} instance, installs it as a {@link MatsPlugin} on the
     * provided {@link MatsFactory}, and returns the instance - <b>Note that lifecycling of the provided
     * {@link MatsBrokerMonitor} is handled by this class:</b> When this instance is {@link #start(MatsFactory) started}
     * by the MatsFactory, it will invoke {@link MatsBrokerMonitor#start() start} on the MatsBrokerMonitor, and when
     * this instance is {@link #close() closed} by the MatsFactory, it will {@link MatsBrokerMonitor#close() close} the
     * MatsBrokerMonitor.
     * 
     * @param matsBrokerMonitor
     *            the {@link MatsBrokerMonitor} to listen to - note that life-cycle of this instance is managed by this
     *            class, "forwarded" from the {@link MatsFactory}.
     * @param matsFactory
     *            the {@link MatsFactory} to install the plugin on.
     * @return the created {@link MatsBrokerMonitorBroadcastAndControl} instance.
     */
    public static MatsBrokerMonitorBroadcastAndControl install(MatsBrokerMonitor matsBrokerMonitor,
            MatsFactory matsFactory) {
        MatsBrokerMonitorBroadcastAndControl broadcastAndControl = new MatsBrokerMonitorBroadcastAndControl(
                matsBrokerMonitor, matsFactory);
        matsFactory.getFactoryConfig().installPlugin(broadcastAndControl);
        return broadcastAndControl;
    }

    private Consumer<UpdateEvent> _eventUpdateListener;

    private MatsEndpoint<?, ?> _commandEndpoint;

    // ----- IMPLEMENTATION: MatsPlugin MatsBrokerMonitorBroadcastAndControl -----

    @Override
    public void start(MatsFactory matsFactory) {
        // Start the MatsBrokerMonitor, as per contract in JavaDoc.
        _matsBrokerMonitor.start();

        // :: Create the MatsBrokerMonitor UpdateEvent listener
        MatsInitiator matsInitiator = matsFactory.getDefaultInitiator();
        _eventUpdateListener = updateEvent -> {
            if (updateEvent.isUpdateEventOriginatedOnThisNode()) {
                log.info("Got UpdateEvent from MBM originating from this MBM/host (fullUpdate:[" + updateEvent
                        .isFullUpdate() + "]) - broadcasting to [" + BROADCAST_UPDATE_EVENT_TOPIC_ENDPOINT_ID + "].");

                BroadcastUpdateEventDto dto = BroadcastUpdateEventDto.of(updateEvent);

                // Send the UpdateEvent to the topic endpoint
                matsInitiator.initiateUnchecked(init -> init
                        .traceId("MatsBrokerMonitorBroadcastUpdate:"
                                + Long.toString(Math.abs(ThreadLocalRandom.current().nextLong()), 36))
                        .from("MatsBrokerMonitorBroadcaster")
                        .setTraceProperty(SUPPRESS_LOGGING_TRACE_PROPERTY_KEY, Boolean.TRUE)
                        .to(BROADCAST_UPDATE_EVENT_TOPIC_ENDPOINT_ID)
                        .publish(dto));
            }
            else {
                log.debug("Got UpdateEvent from MBM which is NOT originating from this MBM/host! Will NOT broadcast.");
            }
        };
        // .. register it
        _matsBrokerMonitor.registerListener(_eventUpdateListener);

        // :: Create the Command listener
        // On the off chance (i.e. only in the TestServer!) that another MatsBrokerMonitor command listener has already
        // been installed on this MatsFactory, we do not need to install one more.
        if (matsFactory.getEndpoint(MATSBROKERMONITOR_CONTROL).isEmpty()) {
            _commandEndpoint = matsFactory.terminator(MATSBROKERMONITOR_CONTROL, void.class,
                    MatsBrokerMonitorCommandDto.class, (ctx, state, command) -> {
                        // :: Is this FORCE_UPDATE or FORCE_UPDATE_FULL command?
                        boolean fullUpdate = MatsBrokerMonitorCommandDto.FORCE_UPDATE_FULL.equals(command.getMethod());
                        if (fullUpdate || MatsBrokerMonitorCommandDto.FORCE_UPDATE.equals(command.getMethod())) {
                            _matsBrokerMonitor.forceUpdate(command.getCorrelationId(), fullUpdate);
                        }
                    });
            log.info("Added Endpoint for commands [" + MATSBROKERMONITOR_CONTROL + "].");
        }
        else {
            _commandEndpoint = null;
            log.info("Already an Endpoint for commands installed [" + MATSBROKERMONITOR_CONTROL
                    + "] on this MatsFactory, can't install one more, ignoring.");
        }

    }

    @Override
    public void preStop() {
        // Remove the command endpoint, just to be clean.
        if (_commandEndpoint != null) {
            _commandEndpoint.remove(15_000);
            _commandEndpoint = null;
        }
        // Remove us as listener from MatsBrokerMonitor
        _matsBrokerMonitor.removeListener(_eventUpdateListener);
        // .. and close the MatsBrokerMonitor, as per contract in JavaDoc.
        _matsBrokerMonitor.close();
    }

    // TODO: Remove ASAP, once users have removed their call. Latest 2025.
    @Override
    public void close() {
        log.error("DO NOT INVOKE THIS METHOD!", new Exception("This method is deprecated -"
                + " the MatsFactory will handle it, by invoking preStop() and stop()."));
    }

    private MatsBrokerMonitorBroadcastAndControl(MatsBrokerMonitor matsBrokerMonitor, MatsFactory matsFactory) {
        _matsBrokerMonitor = matsBrokerMonitor;
    }

    /**
     * NOTE: THIS IS COPIED OVER TO MatsBrokerMonitorBroadcastReceiver
     * <p>
     * UpdateEvent DTO from MatsBrokerMonitor, which is sent to SubscriptionTerminator ("topic") EndpointId
     * {@link #BROADCAST_UPDATE_EVENT_TOPIC_ENDPOINT_ID}.
     */
    private static class BroadcastUpdateEventDto implements UpdateEvent {
        private long suts;
        private String cid; // nullable
        private boolean fu;
        private BrokerInfoDto bi; // nullable
        private List<MatsBrokerDestinationDto> ds;

        public BroadcastUpdateEventDto() {
            /* need no-args constructor for deserializing with Jackson */
        }

        public static BroadcastUpdateEventDto of(UpdateEvent updateEvent) {
            NavigableMap<String, MatsBrokerDestination> eventDestinations = updateEvent.getEventDestinations();

            ArrayList<MatsBrokerDestinationDto> destinationDtos = new ArrayList<>(eventDestinations.size());
            for (MatsBrokerDestination dest : eventDestinations.values()) {
                destinationDtos.add(MatsBrokerDestinationDto.of(dest));
            }

            BrokerInfoDto brokerInfoDto = null;
            if (updateEvent.getBrokerInfo().isPresent()) {
                brokerInfoDto = BrokerInfoDto.of(updateEvent.getBrokerInfo().get());
            }

            return new BroadcastUpdateEventDto(updateEvent.isFullUpdate(),
                    updateEvent.getStatisticsUpdateMillis(),
                    updateEvent.getCorrelationId().orElse(null),
                    brokerInfoDto,
                    destinationDtos);
        }

        public BroadcastUpdateEventDto(boolean fullUpdate, long statisticsUpdateMillis,
                String correlationId, BrokerInfoDto brokerInfo,
                List<MatsBrokerDestinationDto> destinations) {
            this.suts = statisticsUpdateMillis;
            this.fu = fullUpdate;
            this.cid = correlationId;
            this.bi = brokerInfo;
            this.ds = destinations;
        }

        public boolean isFullUpdate() {
            return fu;
        }

        @Override
        public boolean isUpdateEventOriginatedOnThisNode() {
            return false;
        }

        @Override
        public Optional<BrokerInfo> getBrokerInfo() {
            return Optional.ofNullable(bi);
        }

        @Override
        public long getStatisticsUpdateMillis() {
            return suts;
        }

        public Optional<String> getCorrelationId() {
            return Optional.ofNullable(cid);
        }

        @Override
        public NavigableMap<String, MatsBrokerDestination> getEventDestinations() {
            TreeMap<String, MatsBrokerDestination> ret = new TreeMap<>();
            for (MatsBrokerDestination dest : ds) {
                ret.put(dest.getFqDestinationName(), dest);
            }
            return ret;
        }
    }

    /**
     * NOTE: THIS IS COPIED OVER TO MatsBrokerMonitorBroadcastReceiver
     * <p>
     * "Command object", which can be sent to Endpoint {@link #MATSBROKERMONITOR_CONTROL}.
     */
    private static class MatsBrokerMonitorCommandDto {
        /**
         * Can be {@link #FORCE_UPDATE} or {@link #FORCE_UPDATE_FULL}
         */
        private String m;

        private String cid;

        public static String FORCE_UPDATE = "forceUpdate";
        public static String FORCE_UPDATE_FULL = "forceUpdateFull";

        public static MatsBrokerMonitorCommandDto forceUpdate(String correlationId, boolean full) {
            return new MatsBrokerMonitorCommandDto(correlationId, full ? FORCE_UPDATE_FULL : FORCE_UPDATE);
        }

        private MatsBrokerMonitorCommandDto() {
            /* need no-args constructor for deserializing with Jackson */
        }

        private MatsBrokerMonitorCommandDto(String correlationId, String method) {
            this.m = method;
            this.cid = correlationId;
        }

        public String getMethod() {
            return m;
        }

        public String getCorrelationId() {
            return cid;
        }
    }
}
