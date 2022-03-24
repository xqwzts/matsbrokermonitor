package io.mats3.matsbrokermonitor.api;

import java.util.OptionalLong;

/**
 * @author Endre Stølsvik 2022-03-23 23:12 - http://stolsvik.com/, endre@stolsvik.com
 */
public
interface MatsFabricAggregates {

    // :: INCOMING

    long getTotalNumberOfIncomingMessages();

    OptionalLong getOldestIncomingMessageAgeMillis();

    long getMaxStageNumberOfIncomingMessages();

    // :: DLQ

    long getTotalNumberOfDlqMessages();

    OptionalLong getOldestDlqMessageAgeMillis();

    long getMaxStageNumberOfDlqMessages();
}
