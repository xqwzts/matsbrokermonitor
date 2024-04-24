package io.mats3.matsbrokermonitor.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination.StageDestinationType;

/**
 * Consumes the info from {@link MatsBrokerMonitor} (all its {@link MatsBrokerDestination MatsBrokerDestination}
 * instances), and stacks it up in a Mats fabric-relevant representation: {@link MatsEndpointGroupBrokerRepresentation
 * Endpoint Groups}, consisting of {@link MatsEndpointBrokerRepresentation Endpoints}, consisting of
 * {@link MatsStageBrokerRepresentation Stages}.
 *
 * @author Endre Stølsvik 2022-01-07 00:36, 2024-04-08 21:43 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsFabricAggregatedRepresentation {

    /**
     * @param matsDestinations
     *            the output from {@link MatsBrokerMonitor.BrokerSnapshot#getMatsDestinations()}.values().
     * @return a Mats-relevant representation.
     */
    static MatsFabricAggregatedRepresentation stack(Collection<MatsBrokerDestination> matsDestinations) {
        return MatsFabricAggregatorImpl.stack_interal(matsDestinations);
    }

    default long getTotalNumberOfQueuedMessages(StageDestinationType StageDestinationType) {
        long total = 0;
        for (MatsEndpointBrokerRepresentation endpoint : getEndpoints().values()) {
            total += endpoint.getTotalNumberOfQueuedMessages(StageDestinationType);
        }
        return total;
    }

    /**
     * @return the max of
     *         {@link MatsEndpointBrokerRepresentation#getOldestStageHeadMessageAgeMillis(StageDestinationType)
     *         endpoint.getOldestMessageAgeMillis(queueType)} for {@link #getEndpoints() all endpoints} of the Mats
     *         fabric, if no Endpoint has a message, {@link OptionalLong#empty()} is returned.
     */
    default OptionalLong getOldestStageHeadMessageAgeMillis(StageDestinationType StageDestinationType) {
        return getEndpoints().values().stream()
                .map(e -> e.getOldestStageHeadMessageAgeMillis(StageDestinationType))
                .filter(OptionalLong::isPresent)
                .map(OptionalLong::getAsLong)
                .max(Comparator.naturalOrder())
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty());
    }

    /**
     * @return the max of {@link MatsEndpointBrokerRepresentation#getMaxStageNumberOfMessages(StageDestinationType)
     *         endpoint.getMaxStageNumberOfMessages(queueType)} for all Endpoints in the Mats fabric.
     */
    default long getMaxStageNumberOfMessages(StageDestinationType StageDestinationType) {
        return getEndpoints().values().stream()
                .map(e -> e.getMaxStageNumberOfMessages(StageDestinationType))
                .max(Comparator.naturalOrder())
                .orElse(0L);
    }

    /**
     * @return the default global DLQ, if there is such a thing in the connected broker. This should ideally not be in
     *         use, as the broker should be configured to use some kind of "individual DLQ policy" whereby each queue
     *         gets its own DLQ (brokers do support this).
     */
    Optional<MatsBrokerDestination> getBrokerDefaultGlobalDlq();

    /**
     * All Endpoints, no grouping.
     *
     * @return Map[String:EndpointId, MatsEndpointBrokerRepresentation]
     */
    Map<String, MatsEndpointBrokerRepresentation> getEndpoints();

    /**
     * All Endpoints, grouped by "Endpoint Group", where a "group" is all Endpoints sharing the first part of the
     * EndpointId.
     *
     * @return Map[String:EndpointGroupName, MatsEndpointGroupBrokerRepresentation]
     */
    Map<String, MatsEndpointGroupBrokerRepresentation> getEndpointGroups();

    /**
     * Return the {@link MatsStageBrokerRepresentation} for the specified destinationName, or {@link Optional#empty()}
     * if no such Stage was found in the stack. Each Stage can have multiple Queues connected to it, most obviously the
     * "standard" and "DLQ" queues, but indeed all specified by {@link StageDestinationType}. If you have a
     * destinationName, you can find the Stage for it by using this method - and then you can get a reference to all the
     * other queues connected to that stage.
     * <p>
     * Note: This method only searches for the Stage in the stacked representation (as gotten by
     * {@link #stack(Collection)}) by comparing against the {@link MatsBrokerDestination#getDestinationName()} from the
     * MatsBrokerDestination instances present in the stages represented in the stack. This means that if you have a
     * relevant destinationName for a particular Mats Stage (say e.g. the DLQ like "DLQ.SomeService.someMethod.stage1"
     * for a Stage), but that DLQ was not present in the broker when the stack was created, this method will return
     * Optional.empty().
     * 
     * @param destinationName
     *            the DestinationName to find the Stage for, e.g. "mats.SomeService.someMethod.stage1", or
     *            "DLQ.mats.matssys.NPIA.SomeService.someMethod.stage1".
     * @return the {@link MatsStageBrokerRepresentation} for the specified destinationName, or {@link Optional#empty()}
     *         if no such Stage was found in the stack.
     */
    default Optional<MatsStageBrokerRepresentation> findStageForDestinationName(String destinationName) {
        // Go through all Endpoints, and then all Stages of each Endpoint, and then all Destinations of each Stage.
        for (MatsEndpointBrokerRepresentation endpoint : getEndpoints().values()) {
            for (MatsStageBrokerRepresentation stage : endpoint.getStages().values()) {
                for (MatsBrokerDestination destination : stage.getAllDestinations()) {
                    if (destination.getDestinationName().equals(destinationName)) {
                        return Optional.of(stage);
                    }
                }
            }
        }
        return Optional.empty();
    }

    /**
     * @return the {@link MatsBrokerDestination}s which are left over after the stacking, i.e. those which are not part
     *         of any Endpoint.
     */
    List<MatsBrokerDestination> getRemainingDestinations();

    /**
     * @return all {@link MatsBrokerDestination}s, including those which are not part of any Endpoint (i.e. including
     *         non-Mats3 relevant destinations).
     */
    List<MatsBrokerDestination> getAllDestinations();

    /**
     * Representation of a Mats "EndpointGroup", a collection of Endpoint, grouped by the first part of the endpoint
     * name, i.e. <code>"EndpointGroup.[SubServiceName.]methodName"</code>. This should ideally have a 1:1 relation with
     * the actual (micro) services in the system.
     */
    interface MatsEndpointGroupBrokerRepresentation {
        String getEndpointGroup();

        Map<String, MatsEndpointBrokerRepresentation> getEndpoints();

        default long getTotalNumberOfQueuedMessages(StageDestinationType StageDestinationType) {
            long total = 0;
            for (MatsEndpointBrokerRepresentation endpoint : getEndpoints().values()) {
                total += endpoint.getTotalNumberOfQueuedMessages(StageDestinationType);
            }
            return total;
        }

        /**
         * @return the max of {@link MatsEndpointBrokerRepresentation#getMaxStageNumberOfMessages(StageDestinationType)
         *         endpoint.getMaxStageNumberOfMessages()} {@link #getEndpoints() all endpoints} of the EndpointGroup,
         *         if no Endpoint has a message, <code>0</code> is returned.
         */
        default long getMaxStageNumberOfMessages(StageDestinationType StageDestinationType) {
            return getEndpoints().values().stream()
                    .map(e -> e.getMaxStageNumberOfMessages(StageDestinationType))
                    .max(Comparator.naturalOrder())
                    .orElse(0L);
        }

        /**
         * @return the max of
         *         {@link MatsEndpointBrokerRepresentation#getOldestStageHeadMessageAgeMillis(StageDestinationType)
         *         endpoint.getStageOldestHeadMessageAgeMillis()} for {@link #getEndpoints() all endpoints} of the
         *         EndpointGroup, if no Endpoints has a message, {@link OptionalLong#empty()} is returned.
         */
        default OptionalLong getOldestStageHeadMessageAgeMillis(StageDestinationType StageDestinationType) {
            return getEndpoints().values().stream()
                    .map(e -> e.getOldestStageHeadMessageAgeMillis(StageDestinationType))
                    .filter(OptionalLong::isPresent)
                    .map(OptionalLong::getAsLong)
                    .max(Comparator.naturalOrder())
                    .map(OptionalLong::of)
                    .orElse(OptionalLong.empty());
        }

        default List<MatsBrokerDestination> getAllDestinations() {
            return getEndpoints().values().stream().map(MatsEndpointBrokerRepresentation::getAllDestinations)
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        }
    }

    /**
     * Representation of a Mats Endpoint (which contains all stages) as seen from the "Mats Fabric", i.e. as seen from
     * the Broker.
     */
    interface MatsEndpointBrokerRepresentation {
        String getEndpointId();

        Map<Integer, MatsStageBrokerRepresentation> getStages();

        default long getTotalNumberOfQueuedMessages(StageDestinationType StageDestinationType) {
            long total = 0;
            for (MatsStageBrokerRepresentation stage : getStages().values()) {
                total += stage.getDestination(StageDestinationType)
                        .map(MatsBrokerDestination::getNumberOfQueuedMessages)
                        .orElse(0L);
            }
            return total;
        }

        /**
         * @return the max of {@link MatsStageBrokerRepresentation#getHeadMessageAgeMillis(StageDestinationType)
         *         stage.getHeadMessageAgeMillis()} for {@link #getStages() all Stages} of the Endpoint, if no Stages
         *         has a message, {@link OptionalLong#empty()} is returned.
         */
        default OptionalLong getOldestStageHeadMessageAgeMillis(StageDestinationType StageDestinationType) {
            return getStages().values().stream()
                    .map(s -> s.getHeadMessageAgeMillis(StageDestinationType))
                    .filter(OptionalLong::isPresent)
                    .map(OptionalLong::getAsLong)
                    .max(Comparator.naturalOrder())
                    .map(OptionalLong::of)
                    .orElse(OptionalLong.empty());
        }

        default long getMaxStageNumberOfMessages(StageDestinationType StageDestinationType) {
            return getStages().values().stream()
                    .map(s -> s.getNumberOfMessages(StageDestinationType))
                    .max(Comparator.naturalOrder())
                    .orElse(0L);
        }

        default List<MatsBrokerDestination> getAllDestinations() {
            return getStages().values().stream().map(MatsStageBrokerRepresentation::getAllDestinations)
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
        }
    }

    /**
     * Representation of a Mats Stage, including the Endpoint's initial stage ("stage 0"), with all the relevant
     * {@link StageDestinationType}s for the stage, as seen from the "Mats Fabric", i.e. as seen from the Broker.
     */
    interface MatsStageBrokerRepresentation {
        /**
         * @return the index of the stage, where 0 is initial (endpoint entry).
         */
        int getStageIndex();

        /**
         * @return the stageId of the stage - where the 0th (initial) is identical to the EndpointId, while the
         *         subsequent have a postfix ".stageX".
         */
        String getStageId();

        /**
         * Returns the {@link MatsBrokerDestination} from the specified {@link StageDestinationType} of the Stage. Since
         * it very well is possible that a stage does not have a destination for a specific {@link StageDestinationType}
         * (even the {@link StageDestinationType#STANDARD STANDARD} queue, e.g. if there is a DLQ, but now the endpoint
         * is gone), this method returns an {@link Optional}.
         * 
         * @return the {@link MatsBrokerDestination} from the specified {@link StageDestinationType} of the Stage.
         */
        Optional<MatsBrokerDestination> getDestination(StageDestinationType StageDestinationType);

        /**
         * @return all Stage-relevant {@link MatsBrokerDestination}s.
         */
        List<MatsBrokerDestination> getAllDestinations();

        /**
         * @return {@link MatsBrokerDestination#getHeadMessageAgeMillis()} if
         *         {@link #getDestination(StageDestinationType)} is present.
         */
        default OptionalLong getHeadMessageAgeMillis(StageDestinationType StageDestinationType) {
            return getDestination(StageDestinationType)
                    .map(MatsBrokerDestination::getHeadMessageAgeMillis)
                    .orElse(OptionalLong.empty());
        }

        default long getNumberOfMessages(StageDestinationType StageDestinationType) {
            return getDestination(StageDestinationType)
                    .map(MatsBrokerDestination::getNumberOfQueuedMessages)
                    .orElse(0L);
        }
    }

    /**
     * Not for you - use {@link MatsFabricAggregatedRepresentation#stack(Collection)} instead.
     *
     * @author Endre Stølsvik 2022-01-09 00:23 - http://stolsvik.com/, endre@stolsvik.com
     */
    final class MatsFabricAggregatorImpl {
        private static final Logger log = LoggerFactory.getLogger(MatsFabricAggregatorImpl.class);

        private MatsFabricAggregatorImpl() {
            /* hide constructor */
        }

        private static final Pattern STAGE_PATTERN = Pattern.compile("(.*)\\.stage(\\d+)");

        /**
         * <b>YOU ARE NOT TO USE THIS CLASS! Use {@link MatsFabricAggregatedRepresentation#stack(Collection)}</b>
         *
         * @see MatsFabricAggregatedRepresentation#stack(Collection)
         */
        private static MatsFabricAggregatedRepresentation stack_interal(
                Collection<MatsBrokerDestination> matsBrokerDestinations) {
            MatsBrokerDestination globalDlq = null;
            SortedMap<String, MatsEndpointBrokerRepresentationImpl> endpointBrokerRepresentations = new TreeMap<>();
            List<MatsBrokerDestination> remainingDestinations = new ArrayList<>();
            for (MatsBrokerDestination matsBrokerDestination : matsBrokerDestinations) {
                // ?: Is this the Global DLQ?
                if (matsBrokerDestination.isBrokerDefaultGlobalDlq()) {
                    // -> Yes, Global DLQ: save, and continue.
                    globalDlq = matsBrokerDestination;
                    continue;
                }
                // ?: Is there a Mats StageId (which also represent its EndpointId)?
                if (matsBrokerDestination.getMatsStageId().isEmpty()) {
                    // -> No, no StageId, so put it in the "remaining destinations" list, and continue.
                    remainingDestinations.add(matsBrokerDestination);
                    continue;
                }

                // ----- We have a StageId, so we can stack it up into the Endpoint/Stage structure.

                // :: Find which Endpoint this queue/topic relates to,
                String stageId = matsBrokerDestination.getMatsStageId().get();
                String endpointId;
                int stageIndex;
                Matcher stageMatcher = STAGE_PATTERN.matcher(stageId);
                if (stageMatcher.matches()) {
                    // -> Stage
                    endpointId = stageMatcher.group(1);
                    stageIndex = Integer.parseInt(stageMatcher.group(2));
                }
                else {
                    // -> Missing "stageXX", so this is the "stage0" for the Endpoint, where StageId == EndpointId
                    endpointId = stageId;
                    stageIndex = 0;
                }

                // Endpoint: Create the MatsEndpointBrokerRepresentation if not already present.
                MatsEndpointBrokerRepresentationImpl matsEndpointBrokerRepresentation = endpointBrokerRepresentations
                        .computeIfAbsent(endpointId, MatsEndpointBrokerRepresentationImpl::new);

                // Stage: Create the MatsStageBrokerRepresentation if not already present.
                MatsStageBrokerRepresentationImpl matsStageBrokerRepresentation = matsEndpointBrokerRepresentation._stages
                        .computeIfAbsent(stageIndex, stIdx -> new MatsStageBrokerRepresentationImpl(stIdx, stageId));

                // Assert: The 'getStageDestinationType()' shall return a value since we have a StageId.
                if (matsBrokerDestination.getStageDestinationType().isEmpty()) {
                    throw new IllegalStateException("The MatsBrokerDestination [" + matsBrokerDestination
                            + "] did not have a StageDestinationType while it had a StageId [" + stageId + "]");
                }

                StageDestinationType stageDestinationType = matsBrokerDestination.getStageDestinationType().get();

                // :: Add a hack here to handle the case where both a queue and a topic have the same name!
                // (This is not allowed in Mats, but brokers allows it, so it can happen due to a bug: This was caught
                // when the code for reissue from a DLQ for a SubscriptionTerminator (Topic) didn't take into account
                // that the original destination was a Topic, and thus sent it to a same-named Queue instead.)
                // ?: Is there already a destination for this StageDestinationType/StageId?
                if (matsStageBrokerRepresentation._destinations.containsKey(stageDestinationType)) {
                    // -> Yes, there is already a destination for this StageDestinationType.
                    // Add it to the "remaining destinations" list, and continue.
                    remainingDestinations.add(matsBrokerDestination);
                    log.error("Found two destinations for same StageDestinationType/StageId [" + stageDestinationType
                            + "/" + stageId + "] (probably both queue and topic) - added this one to the 'remaining'"
                            + " section. THIS IS NOT ALLOWED IN MATS3, SO IT SHOULD NEVER HAPPEN!"
                            + " This: [" + matsBrokerDestination.getFqDestinationName()
                            + "], Existing: [" + matsStageBrokerRepresentation._destinations.get(stageDestinationType)
                                    .getFqDestinationName() + "]");
                    // Continue with the next MatsBrokerDestination.
                    continue;
                }

                // Add the MatsBrokerDestination to the Stage, keyed by the StageDestinationType.
                matsStageBrokerRepresentation._destinations
                        .put(stageDestinationType, matsBrokerDestination);
            }

            // :: Stack endpoints up into "EndpointGroups"
            // [EndpointGroupId, [EndpointId, EndpointRepresentation]]
            TreeMap<String, TreeMap<String, MatsEndpointBrokerRepresentation>> services = endpointBrokerRepresentations
                    .values().stream()
                    .map(e -> (MatsEndpointBrokerRepresentation) e)
                    .collect(Collectors.groupingBy(e -> {
                        // :: Get "ServiceName", up to first dot - or entire name if no dots.
                        String endpointId = e.getEndpointId();
                        int dot = endpointId.indexOf('.');
                        return dot != -1 ? endpointId.substring(0, dot) : endpointId;
                    }, TreeMap::new, Collectors.toMap(MatsEndpointBrokerRepresentation::getEndpointId, e -> e,
                            (ep1, ep2) -> {
                                throw new IllegalStateException("Collision! [" + ep1 + "], [" + ep2 + "]");
                            }, TreeMap::new)));

            TreeMap<String, MatsEndpointGroupBrokerRepresentation> matsServiceBrokerRepresentations = services
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Entry::getKey,
                            entry -> new MatsEndpointGroupBrokerRepresentationImpl(entry.getKey(), entry.getValue()),
                            (sg1, sg2) -> {
                                throw new IllegalStateException("Collision! [" + sg1 + "], [" + sg2 + "]");
                            }, TreeMap::new));

            ArrayList<MatsBrokerDestination> sortedDestinations = new ArrayList<>(matsBrokerDestinations);
            sortedDestinations.sort(Comparator.comparing(MatsBrokerDestination::getFqDestinationName));

            return new MatsFabricAggregatedRepresentationImpl(globalDlq,
                    matsServiceBrokerRepresentations, endpointBrokerRepresentations,
                    sortedDestinations, remainingDestinations);
        }

        private static class MatsFabricAggregatedRepresentationImpl implements MatsFabricAggregatedRepresentation {
            private final MatsBrokerDestination _globalDlq;
            private final Map<String, ? extends MatsEndpointGroupBrokerRepresentation> _matsServiceBrokerRepresentations;
            private final Map<String, ? extends MatsEndpointBrokerRepresentation> _matsEndpointBrokerRepresentations;

            private final List<MatsBrokerDestination> _allDestinations;
            private final List<MatsBrokerDestination> _remainingDestinations;

            private MatsFabricAggregatedRepresentationImpl(MatsBrokerDestination globalDlq,
                    Map<String, ? extends MatsEndpointGroupBrokerRepresentation> matsServiceBrokerRepresentations,
                    Map<String, ? extends MatsEndpointBrokerRepresentation> matsEndpointBrokerRepresentations,
                    List<MatsBrokerDestination> allDestinations,
                    List<MatsBrokerDestination> remainingDestinations) {
                _matsServiceBrokerRepresentations = matsServiceBrokerRepresentations;
                _matsEndpointBrokerRepresentations = matsEndpointBrokerRepresentations;
                _globalDlq = globalDlq;
                _allDestinations = allDestinations;
                _remainingDestinations = remainingDestinations;
            }

            @Override
            public Optional<MatsBrokerDestination> getBrokerDefaultGlobalDlq() {
                return Optional.ofNullable(_globalDlq);
            }

            @Override
            public Map<String, MatsEndpointBrokerRepresentation> getEndpoints() {
                return Collections.unmodifiableMap(_matsEndpointBrokerRepresentations);
            }

            @Override
            public Map<String, MatsEndpointGroupBrokerRepresentation> getEndpointGroups() {
                return Collections.unmodifiableMap(_matsServiceBrokerRepresentations);
            }

            @Override
            public List<MatsBrokerDestination> getRemainingDestinations() {
                return Collections.unmodifiableList(_remainingDestinations);
            }

            @Override
            public List<MatsBrokerDestination> getAllDestinations() {
                return Collections.unmodifiableList(_allDestinations);
            }
        }

        private static class MatsEndpointGroupBrokerRepresentationImpl implements
                MatsEndpointGroupBrokerRepresentation {
            private final String _serviceName;
            private final Map<String, MatsEndpointBrokerRepresentation> _matsEndpointBrokerRepresentations;

            private MatsEndpointGroupBrokerRepresentationImpl(String serviceName,
                    Map<String, MatsEndpointBrokerRepresentation> matsEndpointBrokerRepresentations) {
                _serviceName = serviceName;
                // :: We want the "private" endpoints at the end
                // Split into two maps, then join
                LinkedHashMap<String, MatsEndpointBrokerRepresentation> nonPrivateEps = new LinkedHashMap<>();
                LinkedHashMap<String, MatsEndpointBrokerRepresentation> privateEps = new LinkedHashMap<>();
                for (MatsEndpointBrokerRepresentation epr : matsEndpointBrokerRepresentations.values()) {
                    if (epr.getEndpointId().contains(".private.")) {
                        privateEps.put(epr.getEndpointId(), epr);
                    }
                    else {
                        nonPrivateEps.put(epr.getEndpointId(), epr);
                    }
                }
                // .. tack the private onto the end of the non-private.
                nonPrivateEps.putAll(privateEps);
                _matsEndpointBrokerRepresentations = nonPrivateEps;
            }

            @Override
            public String getEndpointGroup() {
                return _serviceName;
            }

            @Override
            public Map<String, MatsEndpointBrokerRepresentation> getEndpoints() {
                return Collections.unmodifiableMap(_matsEndpointBrokerRepresentations);
            }
        }

        private static class MatsEndpointBrokerRepresentationImpl implements MatsEndpointBrokerRepresentation {

            private final String _endpointId;
            private final SortedMap<Integer, MatsStageBrokerRepresentationImpl> _stages = new TreeMap<>();

            private MatsEndpointBrokerRepresentationImpl(String endpointId) {
                _endpointId = endpointId;
            }

            @Override
            public String getEndpointId() {
                return _endpointId;
            }

            @Override
            public SortedMap<Integer, MatsStageBrokerRepresentation> getStages() {
                return Collections.unmodifiableSortedMap(_stages);
            }
        }

        private static class MatsStageBrokerRepresentationImpl implements MatsStageBrokerRepresentation {
            private final int _stageIndex;
            private final String _stageId;

            private final Map<StageDestinationType, MatsBrokerDestination> _destinations = new HashMap<>();

            private MatsStageBrokerRepresentationImpl(int stageIndex, String stageId) {
                _stageIndex = stageIndex;
                _stageId = stageId;
            }

            @Override
            public int getStageIndex() {
                return _stageIndex;
            }

            @Override
            public String getStageId() {
                return _stageId;
            }

            @Override
            public Optional<MatsBrokerDestination> getDestination(StageDestinationType StageDestinationType) {
                return Optional.ofNullable(_destinations.get(StageDestinationType));
            }

            @Override
            public List<MatsBrokerDestination> getAllDestinations() {
                return new ArrayList<>(_destinations.values());
            }
        }
    }
}
