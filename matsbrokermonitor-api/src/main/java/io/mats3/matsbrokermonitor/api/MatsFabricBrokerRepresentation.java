package io.mats3.matsbrokermonitor.api;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.BrokerSnapshot;
import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.impl.MatsFabricArranger;

/**
 * Consumes the info from {@link MatsBrokerMonitor} (all its {@link MatsBrokerDestination MatsBrokerDestination}
 * instances), and stacks it up in a Mats fabric-relevant representation: {@link MatsEndpointGroupBrokerRepresentation
 * Endpoint Groups}, consisting of {@link MatsEndpointBrokerRepresentation Endpoints}, consisting of
 * {@link MatsStageBrokerRepresentation Stages}.
 *
 * @author Endre Stølsvik 2022-01-07 00:36 - http://stolsvik.com/, endre@stolsvik.com
 */
public interface MatsFabricBrokerRepresentation extends MatsFabricAggregates {

    /**
     * @param matsDestinations
     *            the output from {@link BrokerSnapshot#getMatsDestinations()}.values().
     * @return a Mats-relevant representation.
     */
    static MatsFabricBrokerRepresentation stack(Collection<MatsBrokerDestination> matsDestinations) {
        return MatsFabricArranger.stack_interal(matsDestinations);
    }

    default long getTotalNumberOfIncomingMessages() {
        long total = 0;
        for (MatsEndpointBrokerRepresentation endpoint : getEndpoints().values()) {
            total += endpoint.getTotalNumberOfIncomingMessages();
        }
        return total;
    }

    /**
     * @return the max of {@link MatsEndpointBrokerRepresentation#getOldestIncomingMessageAgeMillis()
     *         endpoint.getOldestIncomingHeadMessageAgeMillis()} for {@link #getEndpoints() all endpoints} of the Mats
     *         fabric, if no Endpoint has a message, {@link OptionalLong#empty()} is returned.
     */
    default OptionalLong getOldestIncomingMessageAgeMillis() {
        return getEndpoints().values().stream()
                .map(MatsEndpointBrokerRepresentation::getOldestIncomingMessageAgeMillis)
                .filter(OptionalLong::isPresent)
                .map(OptionalLong::getAsLong)
                .max(Comparator.naturalOrder())
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty());
    }

    /**
     * @return the max of {@link MatsEndpointBrokerRepresentation#getMaxStageNumberOfIncomingMessages()
     *         endpoint.getMaxStageNumberOfIncomingMessages()} for all Endpoints in the Mats fabric.
     */
    default long getMaxStageNumberOfIncomingMessages() {
        return getEndpoints().values().stream()
                .map(MatsEndpointBrokerRepresentation::getMaxStageNumberOfIncomingMessages)
                .max(Comparator.naturalOrder())
                .orElse(0L);
    }

    default long getTotalNumberOfDlqMessages() {
        long total = 0;
        if (getDefaultGlobalDlq().isPresent()) {
            total += getDefaultGlobalDlq().get().getNumberOfQueuedMessages();
        }
        for (MatsEndpointBrokerRepresentation endpoint : getEndpoints().values()) {
            total += endpoint.getTotalNumberOfDlqMessages();
        }
        return total;
    }

    default OptionalLong getOldestDlqMessageAgeMillis() {
        Stream<Long> endpointDlqs = getEndpoints().values().stream()
                .map(MatsEndpointBrokerRepresentation::getOldestDlqMessageAgeMillis)
                .filter(OptionalLong::isPresent)
                .map(OptionalLong::getAsLong);
        Stream<Long> globalDlq = getDefaultGlobalDlq().stream()
                .map(MatsBrokerDestination::getHeadMessageAgeMillis)
                .filter(OptionalLong::isPresent)
                .map(OptionalLong::getAsLong);

        return Stream.concat(endpointDlqs, globalDlq)
                .max(Comparator.naturalOrder())
                .map(OptionalLong::of)
                .orElse(OptionalLong.empty());
    }

    /**
     * @return the max of {@link MatsEndpointBrokerRepresentation#getMaxStageNumberOfDlqMessages()
     *         endpoint.getMaxStageNumberOfDeadLetteredMessages()} for all Endpoints in the Mats fabric, including the
     *         {@link #getDefaultGlobalDlq()} if present. Zero is a good number, any other means that there exists a DLQ
     *         with messages which probably should be looked into.
     */
    default long getMaxStageNumberOfDlqMessages() {
        Stream<Long> globalDlqNum = getDefaultGlobalDlq()
                .map(MatsBrokerDestination::getNumberOfQueuedMessages).stream();

        Stream<Long> stagesDlqMax = getEndpoints().values().stream()
                .map(MatsEndpointBrokerRepresentation::getMaxStageNumberOfDlqMessages);

        return Stream.concat(globalDlqNum, stagesDlqMax)
                .max(Comparator.naturalOrder())
                .orElse(0L);
    }

    /**
     * @return the default global DLQ, if there is such a thing in the connected broker. This should ideally not be in
     *         use, as the broker should be configured to use some kind of "individual DLQ policy" whereby each queue
     *         gets its own DLQ (brokers do support this).
     */
    Optional<MatsBrokerDestination> getDefaultGlobalDlq();

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
     * @return the {@link MatsBrokerDestination}s which are left over after the stacking, i.e. those which are not part
     *         of any Endpoint.
     */
    List<MatsBrokerDestination> getRemainingDestinations();

    /**
     * Representation of a Mats Endpoint (which contains all stages) as seen from the "Mats Fabric", i.e. as seen from
     * the Broker.
     */
    interface MatsEndpointBrokerRepresentation extends MatsFabricAggregates {
        String getEndpointId();

        Map<Integer, MatsStageBrokerRepresentation> getStages();

        default long getTotalNumberOfIncomingMessages() {
            long total = 0;
            for (MatsStageBrokerRepresentation stage : getStages().values()) {
                total += stage.getIncomingDestination()
                        .map(MatsBrokerDestination::getNumberOfQueuedMessages)
                        .orElse(0L);
            }
            return total;
        }

        /**
         * @return the max of {@link MatsStageBrokerRepresentation#getIncomingHeadMessageAgeMillis()
         *         stage.getHeadMessageAgeMillis()} for {@link #getStages() all Stages} of the Endpoint, if no Stages
         *         has a message, {@link OptionalLong#empty()} is returned.
         */
        default OptionalLong getOldestIncomingMessageAgeMillis() {
            return getStages().values().stream()
                    .map(MatsStageBrokerRepresentation::getIncomingHeadMessageAgeMillis)
                    .filter(OptionalLong::isPresent)
                    .map(OptionalLong::getAsLong)
                    .max(Comparator.naturalOrder())
                    .map(OptionalLong::of)
                    .orElse(OptionalLong.empty());
        }

        default long getMaxStageNumberOfIncomingMessages() {
            return getStages().values().stream()
                    .map(MatsStageBrokerRepresentation::getNumberOfIncomingMessages)
                    .max(Comparator.naturalOrder())
                    .orElse(0L);
        }

        default long getTotalNumberOfDlqMessages() {
            long total = 0;
            for (MatsStageBrokerRepresentation stage : getStages().values()) {
                total += stage.getDlqDestination()
                        .map(MatsBrokerDestination::getNumberOfQueuedMessages)
                        .orElse(0L);
            }
            return total;
        }

        default OptionalLong getOldestDlqMessageAgeMillis() {
            return getStages().values().stream()
                    .map(MatsStageBrokerRepresentation::getDlqHeadMessageAgeMillis)
                    .filter(OptionalLong::isPresent)
                    .map(OptionalLong::getAsLong)
                    .max(Comparator.naturalOrder())
                    .map(OptionalLong::of)
                    .orElse(OptionalLong.empty());
        }

        /**
         * @return the max of {@link MatsStageBrokerRepresentation#getNumberOfDlqMessages()
         *         stage.getNumberOfDeadLetterMessages()} for {@link #getStages() all stages} of the Endpoint.
         */
        default long getMaxStageNumberOfDlqMessages() {
            return getStages().values().stream()
                    .map(MatsStageBrokerRepresentation::getNumberOfDlqMessages)
                    .max(Comparator.naturalOrder())
                    .orElse(0L);
        }

    }

    /**
     * Representation of a Mats "EndpointGroup", a collection of Endpoint, grouped by the first part of the endpoint
     * name, i.e. <code>"EndpointGroup.[SubServiceName.]methodName"</code>. This should ideally have a 1:1 relation with
     * the actual (micro) services in the system.
     */
    interface MatsEndpointGroupBrokerRepresentation extends MatsFabricAggregates {
        String getEndpointGroup();

        Map<String, MatsEndpointBrokerRepresentation> getEndpoints();

        default long getTotalNumberOfIncomingMessages() {
            long total = 0;
            for (MatsEndpointBrokerRepresentation endpoint : getEndpoints().values()) {
                total += endpoint.getTotalNumberOfIncomingMessages();
            }
            return total;
        }

        /**
         * @return the max of {@link MatsEndpointBrokerRepresentation#getMaxStageNumberOfIncomingMessages()
         *         endpoint.getMaxStageNumberOfIncomingMessages()} {@link #getEndpoints() all endpoints} of the
         *         EndpointGroup, if no Endpoint has a message, <code>0</code> is returned.
         */
        default long getMaxStageNumberOfIncomingMessages() {
            return getEndpoints().values().stream()
                    .map(MatsEndpointBrokerRepresentation::getMaxStageNumberOfIncomingMessages)
                    .max(Comparator.naturalOrder())
                    .orElse(0L);
        }

        /**
         * @return the max of {@link MatsEndpointBrokerRepresentation#getOldestIncomingMessageAgeMillis()
         *         endpoint.getStageOldestIncomingHeadMessageAgeMillis()} for {@link #getEndpoints() all endpoints} of
         *         the EndpointGroup, if no Endpoints has a message, {@link OptionalLong#empty()} is returned.
         */
        default OptionalLong getOldestIncomingMessageAgeMillis() {
            return getEndpoints().values().stream()
                    .map(MatsEndpointBrokerRepresentation::getOldestIncomingMessageAgeMillis)
                    .filter(OptionalLong::isPresent)
                    .map(OptionalLong::getAsLong)
                    .max(Comparator.naturalOrder())
                    .map(OptionalLong::of)
                    .orElse(OptionalLong.empty());
        }

        default long getTotalNumberOfDlqMessages() {
            long total = 0;
            for (MatsEndpointBrokerRepresentation endpoint : getEndpoints().values()) {
                total += endpoint.getTotalNumberOfDlqMessages();
            }
            return total;
        }

        /**
         * @return the max of {@link MatsEndpointBrokerRepresentation#getMaxStageNumberOfDlqMessages()
         *         endpoint.getMaxStageNumberOfDeadLetteredMessages()} for {@link #getEndpoints() all Endpoints} of the
         *         EndpointGroup.
         */
        default long getMaxStageNumberOfDlqMessages() {
            return getEndpoints().values().stream()
                    .map(MatsEndpointBrokerRepresentation::getMaxStageNumberOfDlqMessages)
                    .max(Comparator.naturalOrder())
                    .orElse(0L);
        }

        default OptionalLong getOldestDlqMessageAgeMillis() {
            return getEndpoints().values().stream()
                    .map(MatsEndpointBrokerRepresentation::getOldestDlqMessageAgeMillis)
                    .filter(OptionalLong::isPresent)
                    .map(OptionalLong::getAsLong)
                    .max(Comparator.naturalOrder())
                    .map(OptionalLong::of)
                    .orElse(OptionalLong.empty());
        }

    }

    /**
     * Representation of a Mats Stage, including the Endpoint's initial stage ("stage 0"), with possibly its DLQ, as
     * seen from the "Mats Fabric", i.e. as seen from the Broker.
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
         * @return the {@link MatsBrokerDestination} from which the stage is consuming. Because it is technically
         *         possible that the incoming destination isn't present while the DLQ is, it is Optional - but in
         *         practice, this should rarely happen.
         */
        Optional<MatsBrokerDestination> getIncomingDestination();

        /**
         * @return {@link MatsBrokerDestination#getHeadMessageAgeMillis()} if {@link #getIncomingDestination()} is
         *         present.
         */
        default OptionalLong getIncomingHeadMessageAgeMillis() {
            return getIncomingDestination()
                    .map(MatsBrokerDestination::getHeadMessageAgeMillis)
                    .orElse(OptionalLong.empty());
        }

        default long getNumberOfIncomingMessages() {
            return getIncomingDestination()
                    .map(MatsBrokerDestination::getNumberOfQueuedMessages)
                    .orElse(0L);
        }

        /**
         * @return the DLQ Destination for this stage, if it is present.
         */
        Optional<MatsBrokerDestination> getDlqDestination();

        default OptionalLong getDlqHeadMessageAgeMillis() {
            return getDlqDestination()
                    .map(MatsBrokerDestination::getHeadMessageAgeMillis)
                    .orElse(OptionalLong.empty());
        }

        /**
         * @return {@link MatsBrokerDestination#getNumberOfQueuedMessages()} for the DLQ of this stage, or zero if
         *         {@link #getDlqDestination()} is not present.
         */
        default long getNumberOfDlqMessages() {
            return getDlqDestination()
                    .map(MatsBrokerDestination::getNumberOfQueuedMessages)
                    .orElse(0L);
        }
    }
}
