/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.statistic.memory.visitor.impl.DistributedEpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochVisitor;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.mac.statistic.memory.visitor.impl.RegularEpochView;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.store.query.ICursor;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.IOpenedTransaction;
import gnu.trove.set.TLongSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class is responsible for parsing memory statistics into an analysis datastore.
 */
public class AnalysisDatastoreFeeder {

  private final Collection<? extends IMemoryStatistic> statistics;
  private final String dumpName;

  private TLongSet datastoreEpochs;
  private Map<ChunkOwner, SortedSet<Long>> regularEpochsPerOwner;
  private Map<ChunkOwner, Set<Long>> distributedEpochsPerOwner;

  /**
   * Constructor.
   *
   * @param statistic the memory statistic to parse
   * @param dumpName the dump name to assign to the statistic
   */
  public AnalysisDatastoreFeeder(
      final IMemoryStatistic statistic,
      final String dumpName) {
    this(Collections.singleton(statistic), dumpName);
  }

  /**
   * Constructor.
   *
   * @param statistics the memory statistics to parse
   * @param dumpName the dump name to assign to the statistics
   */
  public AnalysisDatastoreFeeder(
      final Collection<? extends IMemoryStatistic> statistics,
      final String dumpName) {
    this.statistics = statistics;
    this.dumpName = dumpName;

    collectEpochs(statistics);
  }

  /**
   * Pre-computation pass on the given statistics to determine which epochs are expressed within the
   * statistics.
   *
   * @param statistics the statistics to explore
   */
  private void collectEpochs(final Collection<? extends IMemoryStatistic> statistics) {
    final EpochVisitor epochVisitor = new EpochVisitor();
    statistics.forEach(statistic -> statistic.accept(epochVisitor));

    this.datastoreEpochs = epochVisitor.getDatastoreEpochs();
    this.regularEpochsPerOwner = epochVisitor.getRegularEpochsPerOwner();
    this.distributedEpochsPerOwner = epochVisitor.getDistributedEpochsPerOwner();
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
   * Collects the epochs per owner in the given already open transaction.
   *
   * @param transaction the transaction to collect the epochs from
   */
  private void collectEpochsFromOpenedTransaction(final IOpenedTransaction transaction) {
    final ICursor result = transaction.getQueryRunner()
        .forStore(DatastoreConstants.EPOCH_VIEW_STORE)
        .withCondition(BaseConditions.Equal(DatastoreConstants.CHUNK__DUMP_NAME, dumpName))
        .selecting(
            DatastoreConstants.EPOCH_VIEW__OWNER,
            DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID,
            DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID)
        .run();

    for (final IRecordReader reader : result) {
      final ChunkOwner owner = (ChunkOwner) reader.read(0);
      final long epoch = reader.readLong(1);
      if (reader.read(2) instanceof DistributedEpochView) {
        distributedEpochsPerOwner.computeIfAbsent(owner, key -> new HashSet<>())
            .add(epoch);
      } else {
        datastoreEpochs.add(epoch);
        regularEpochsPerOwner.computeIfAbsent(owner, key -> new TreeSet<>())
            .add(epoch);
      }
    }
  }

  /**
   * Adds the chunks of this feeder's statistics to the transaction.
   *
   * @param transaction the transaction to add facts to
   */
  private void feedRawChunks(final IOpenedTransaction transaction) {
    statistics.forEach(statistic -> statistic.accept(
        new FeedVisitor(transaction.getMetadata(), transaction, dumpName)));
  }

  /**
   * Fills the epoch view store in the given transaction according to the collected epochs from the
   * statistic.
   *
   * @param transaction the transaction to add records to
   */
  private void replicateChunksForMissingEpochs(final IOpenedTransaction transaction) {
    final IRecordFormat epochViewRecordFormat =
        transaction.getMetadata().getDatastoreSchemaFormat()
            .getStoreFormat(DatastoreConstants.EPOCH_VIEW_STORE)
            .getRecordFormat();

    datastoreEpochs.forEach(epochId -> {
      for (final ChunkOwner owner : regularEpochsPerOwner.keySet()) {
        final SortedSet<Long> epochs = regularEpochsPerOwner.get(owner);
        if (!epochs.contains(epochId)) {

          final SortedSet<Long> priorEpochs = epochs.headSet(epochId);
          if (!priorEpochs.isEmpty()) {
            final long baseEpochId = priorEpochs.last();
            transaction.add(
                DatastoreConstants.EPOCH_VIEW_STORE,
                generateEpochViewTuple(epochViewRecordFormat, owner, dumpName, baseEpochId,
                    new RegularEpochView(epochId)));
          }
        } else {
          transaction.add(
              DatastoreConstants.EPOCH_VIEW_STORE,
              generateEpochViewTuple(epochViewRecordFormat, owner, dumpName, epochId,
                  new RegularEpochView(epochId)));
        }
      }

      return true;
    });

    for (final ChunkOwner owner : distributedEpochsPerOwner.keySet()) {
      final Collection<Long> epochs = distributedEpochsPerOwner.get(owner);
      for (final Long epochId : epochs) {
        transaction.add(
            DatastoreConstants.EPOCH_VIEW_STORE,
            generateEpochViewTuple(epochViewRecordFormat, owner, dumpName, epochId,
                new DistributedEpochView(owner.getName(), epochId)));
      }
    }
  }

  private static Object[] generateEpochViewTuple(
      IRecordFormat recordFormat,
      ChunkOwner owner, String dumpName, long baseEpochId, EpochView epochView) {
    final Object[] tuple = new Object[recordFormat.getFieldCount()];
    tuple[recordFormat.getFieldIndex(DatastoreConstants.EPOCH_VIEW__OWNER)] = owner;
    tuple[recordFormat.getFieldIndex(DatastoreConstants.CHUNK__DUMP_NAME)] = dumpName;
    tuple[recordFormat.getFieldIndex(DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID)] = baseEpochId;
    tuple[recordFormat.getFieldIndex(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID)] = epochView;
    return tuple;
  }
}
