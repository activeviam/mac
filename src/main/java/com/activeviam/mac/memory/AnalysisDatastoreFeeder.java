/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.activeviam.mac.Loggers;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.DistributedCubeOwner;
import com.activeviam.mac.statistic.memory.visitor.impl.DistributedEpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.mac.statistic.memory.visitor.impl.RegularEpochView;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.store.query.ICursor;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.IOpenedTransaction;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/** This class is responsible for parsing memory statistics into an analysis datastore. */
public class AnalysisDatastoreFeeder {

  /** Logger. */
  private static final Logger LOGGER = Logger.getLogger(Loggers.LOADING);

  /** The statistics this feeder should feed a datastore with. */
  private final Collection<? extends IMemoryStatistic> statistics;
  /** The dump name associated with this feeder. */
  private final String dumpName;

  /** The set of datastore epochs. */
  private final Set<Long> datastoreEpochs;

  /**
   * A mapping giving for each non-distributed owner the epochs that were expressed in their
   * statistics.
   */
  private final Map<ChunkOwner, SortedSet<Long>> regularEpochsPerOwner;
  /**
   * A mapping giving for each distributed owner the epochs that were expressed in their statistics.
   */
  private final Map<ChunkOwner, Set<Long>> distributedEpochsPerOwner;

  /**
   * Constructor.
   *
   * @param statistic the memory statistic to parse
   * @param dumpName the dump name to assign to the statistic
   */
  public AnalysisDatastoreFeeder(final IMemoryStatistic statistic, final String dumpName) {
    this(Collections.singleton(statistic), dumpName);
  }

  /**
   * Constructor.
   *
   * @param statistics the memory statistics to parse
   * @param dumpName the dump name to assign to the statistics
   */
  public AnalysisDatastoreFeeder(
      final Collection<? extends IMemoryStatistic> statistics, final String dumpName) {
    this.statistics = statistics;
    this.dumpName = dumpName;

    this.datastoreEpochs = new HashSet<>();
    this.regularEpochsPerOwner = new HashMap<>();
    this.distributedEpochsPerOwner = new HashMap<>();
  }

  /**
   * Feeds the given datastore with facts corresponding to the statistics of this feeder.
   *
   * @param transaction the transaction object to add the facts to
   */
  public void feedDatastore(final IOpenedTransaction transaction) {
    feedRawChunks(transaction);

    collectEpochsFromOpenedTransaction(transaction);
    replicateChunksForMissingEpochs(transaction);
  }

  /**
   * Adds the chunks of this feeder's statistics to the transaction.
   *
   * @param transaction the transaction to add facts to
   */
  private void feedRawChunks(final IOpenedTransaction transaction) {
    statistics.forEach(
        statistic -> {
          if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Start feeding the application with " + statistic);
          }

          statistic.accept(new FeedVisitor(transaction.getMetadata(), transaction, dumpName));

          if (LOGGER.isLoggable(Level.FINE)) {
            LOGGER.fine("Application processed " + statistic);
          }
        });
  }

  /**
   * Collects the epochs per owner in the given already open transaction.
   *
   * @param transaction the transaction to collect the epochs from
   */
  private void collectEpochsFromOpenedTransaction(final IOpenedTransaction transaction) {
    final ICursor result = transaction
        .getQueryRunner()
        .forStore(DatastoreConstants.CHUNK_STORE)
        .withCondition(BaseConditions.Equal(DatastoreConstants.CHUNK__DUMP_NAME, dumpName))
        .selecting(
            DatastoreConstants.OWNER__OWNER,
            DatastoreConstants.VERSION__EPOCH_ID)
        .run();

    for (final IRecordReader reader : result) {
      final ChunkOwner owner = (ChunkOwner) reader.read(0);
      final long epoch = reader.readLong(1);
      if (owner instanceof DistributedCubeOwner) {
        distributedEpochsPerOwner.computeIfAbsent(owner, key -> new HashSet<>()).add(epoch);
      } else {
        datastoreEpochs.add(epoch);
        regularEpochsPerOwner.computeIfAbsent(owner, key -> new TreeSet<>()).add(epoch);
      }
    }
  }

  /**
   * Fills the epoch view store in the given transaction according to the collected epochs from the
   * statistic.
   *
   * @param transaction the transaction to add records to
   */
  private void replicateChunksForMissingEpochs(final IOpenedTransaction transaction) {
    replicateDatastoreEpochs(transaction);
    replicateDistributedEpochs(transaction);
  }

  private void replicateDatastoreEpochs(final IOpenedTransaction transaction) {
    final IRecordFormat epochViewRecordFormat =
        transaction
            .getMetadata()
            .getDatastoreSchemaFormat()
            .getStoreFormat(DatastoreConstants.EPOCH_VIEW_STORE)
            .getRecordFormat();

    for (final var datastoreEpochId : this.datastoreEpochs) {
      for (final ChunkOwner owner : regularEpochsPerOwner.keySet()) {
        final SortedSet<Long> epochs = regularEpochsPerOwner.get(owner);
        if (!epochs.contains(datastoreEpochId)) {

          final SortedSet<Long> priorEpochs = epochs.headSet(datastoreEpochId);
          if (!priorEpochs.isEmpty()) {
            final long baseEpochId = priorEpochs.last();
            transaction.add(
                DatastoreConstants.EPOCH_VIEW_STORE,
                generateEpochViewTuple(
                    epochViewRecordFormat,
                    owner,
                    dumpName,
                    baseEpochId,
                    new RegularEpochView(datastoreEpochId)));
          }
        } else {
          transaction.add(
              DatastoreConstants.EPOCH_VIEW_STORE,
              generateEpochViewTuple(
                  epochViewRecordFormat,
                  owner,
                  dumpName,
                  datastoreEpochId,
                  new RegularEpochView(datastoreEpochId)));
        }
      }
    }
  }

  private void replicateDistributedEpochs(final IOpenedTransaction transaction) {
    final IRecordFormat epochViewRecordFormat =
        transaction
            .getMetadata()
            .getDatastoreSchemaFormat()
            .getStoreFormat(DatastoreConstants.EPOCH_VIEW_STORE)
            .getRecordFormat();

    for (final ChunkOwner owner : distributedEpochsPerOwner.keySet()) {
      final Collection<Long> epochs = distributedEpochsPerOwner.get(owner);
      for (final Long epochId : epochs) {
        transaction.add(
            DatastoreConstants.EPOCH_VIEW_STORE,
            generateEpochViewTuple(
                epochViewRecordFormat,
                owner,
                dumpName,
                epochId,
                new DistributedEpochView(owner.getName(), epochId)));
      }
    }
  }

  /**
   * Generates a record to put into the epoch view store.
   *
   * @param recordFormat the format of the epoch view store
   * @param owner the owner field of the record
   * @param dumpName the dump name field of the record
   * @param baseEpochId the base epoch id field of the record
   * @param epochView the epoch view field of the record
   * @return the record
   */
  private static Object[] generateEpochViewTuple(
      final IRecordFormat recordFormat,
      final ChunkOwner owner,
      final String dumpName,
      final long baseEpochId,
      final EpochView epochView) {
    final Object[] tuple = new Object[recordFormat.getFieldCount()];
    tuple[recordFormat.getFieldIndex(DatastoreConstants.EPOCH_VIEW__OWNER)] = owner;
    tuple[recordFormat.getFieldIndex(DatastoreConstants.CHUNK__DUMP_NAME)] = dumpName;
    tuple[recordFormat.getFieldIndex(DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID)] = baseEpochId;
    tuple[recordFormat.getFieldIndex(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID)] = epochView;
    return tuple;
  }
}
