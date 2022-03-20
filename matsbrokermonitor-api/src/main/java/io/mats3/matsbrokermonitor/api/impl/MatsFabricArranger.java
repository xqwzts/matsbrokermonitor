package io.mats3.matsbrokermonitor.api.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.mats3.matsbrokermonitor.api.MatsBrokerMonitor.MatsBrokerDestination;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsEndpointGroupBrokerRepresentation;
import io.mats3.matsbrokermonitor.api.MatsFabricBrokerRepresentation.MatsStageBrokerRepresentation;

/**
 * @author Endre Stølsvik 2022-01-09 00:23 - http://stolsvik.com/, endre@stolsvik.com
 */
public final class MatsFabricArranger {

    private MatsFabricArranger() {
        /* hide constructor */
    }

    private static final Pattern STAGE_PATTERN = Pattern.compile("(.*)\\.stage(\\d+)");

    public static MatsFabricBrokerRepresentation stack_interal(
            Collection<MatsBrokerDestination> matsBrokerDestinations) {
        MatsBrokerDestination globalDlq = null;
        SortedMap<String, MatsEndpointBrokerRepresentationImpl> endpointBrokerRepresentations = new TreeMap<>();
        for (MatsBrokerDestination matsBrokerDestination : matsBrokerDestinations) {
            // ?: Is this the Global DLQ?
            if (matsBrokerDestination.isGlobalDlq()) {
                // -> Yes, Global DLQ: save, and continue.
                globalDlq = matsBrokerDestination;
                continue;
            }
            // ?: Is there a Mats StageId (also EndpointId)?
            if (!matsBrokerDestination.getMatsStageId().isPresent()) {
                // -> No, no StageId, so ditch this, and continue.
                continue;
            }
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

            // Add the MatsBrokerDestination to the Stage, either it is the incoming destination or the DLQ.
            if (matsBrokerDestination.isDlq()) {
                matsStageBrokerRepresentation._dlqDestination = matsBrokerDestination;
            }
            else {
                matsStageBrokerRepresentation._incomingDestination = matsBrokerDestination;
            }
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

        TreeMap<String, MatsEndpointGroupBrokerRepresentation> matsServiceBrokerRepresentations = services.entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey,
                        entry -> new MatsEndpointGroupBrokerRepresentationImpl(entry.getKey(), entry.getValue()),
                        (sg1, sg2) -> {
                            throw new IllegalStateException("Collision! [" + sg1 + "], [" + sg2 + "]");
                        }, TreeMap::new));

        return new MatsFabricBrokerRepresentationImpl(globalDlq,
                matsServiceBrokerRepresentations,
                endpointBrokerRepresentations);
    }

    static class MatsFabricBrokerRepresentationImpl implements MatsFabricBrokerRepresentation {
        private final MatsBrokerDestination _globalDlq;
        private final Map<String, ? extends MatsEndpointGroupBrokerRepresentation> _matsServiceBrokerRepresentations;
        private final Map<String, ? extends MatsEndpointBrokerRepresentation> _matsEndpointBrokerRepresentations;

        public MatsFabricBrokerRepresentationImpl(MatsBrokerDestination globalDlq,
                Map<String, ? extends MatsEndpointGroupBrokerRepresentation> matsServiceBrokerRepresentations,
                Map<String, ? extends MatsEndpointBrokerRepresentation> matsEndpointBrokerRepresentations) {
            _matsServiceBrokerRepresentations = matsServiceBrokerRepresentations;
            _matsEndpointBrokerRepresentations = matsEndpointBrokerRepresentations;
            _globalDlq = globalDlq;
        }

        @Override
        public Optional<MatsBrokerDestination> getGlobalDlq() {
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
    }

    static class MatsEndpointBrokerRepresentationImpl implements MatsEndpointBrokerRepresentation {

        private final String _endpointId;
        private final Map<Integer, MatsStageBrokerRepresentationImpl> _stages = new TreeMap<>();

        public MatsEndpointBrokerRepresentationImpl(String endpointId) {
            _endpointId = endpointId;
        }

        @Override
        public String getEndpointId() {
            return _endpointId;
        }

        @Override
        public Map<Integer, MatsStageBrokerRepresentation> getStages() {
            return Collections.unmodifiableMap(_stages);
        }
    }

    static class MatsEndpointGroupBrokerRepresentationImpl implements MatsEndpointGroupBrokerRepresentation {
        private final String _serviceName;
        private final Map<String, MatsEndpointBrokerRepresentation> _matsEndpointBrokerRepresentations;

        public MatsEndpointGroupBrokerRepresentationImpl(String serviceName,
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

    static class MatsStageBrokerRepresentationImpl implements MatsStageBrokerRepresentation {
        private final int _stageIndex;
        private final String _stageId;

        private MatsBrokerDestination _incomingDestination;
        private MatsBrokerDestination _dlqDestination;

        public MatsStageBrokerRepresentationImpl(int stageIndex, String stageId) {
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
        public Optional<MatsBrokerDestination> getIncomingDestination() {
            return Optional.ofNullable(_incomingDestination);
        }

        @Override
        public Optional<MatsBrokerDestination> getDlqDestination() {
            return Optional.ofNullable(_dlqDestination);
        }
    }
}
