/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochVisitor;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.google.common.collect.SortedSetMultimap;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.store.transaction.IOpenedTransaction;
import gnu.trove.set.TLongSet;
import java.util.Collection;
import java.util.Collections;
import java.util.SortedSet;

public class MemoryStatisticDatastoreFeeder {

  private Collection<? extends IMemoryStatistic> statistics;
  private String dumpName;
  private TLongSet datastoreEpochs;
  private SortedSetMultimap<ChunkOwner, Long> epochsPerOwner;

  public MemoryStatisticDatastoreFeeder(
      final IMemoryStatistic statistic,
      final String dumpName) {
    this(Collections.singleton(statistic), dumpName);
  }

  public MemoryStatisticDatastoreFeeder(
      final Collection<? extends IMemoryStatistic> statistics,
      final String dumpName) {
    this.statistics = statistics;
    this.dumpName = dumpName;

    preprocessEpochs(statistics);
  }

  private void preprocessEpochs(final Collection<? extends IMemoryStatistic> statistics) {
    final EpochVisitor epochVisitor = new EpochVisitor();
    statistics.forEach(statistic -> statistic.accept(epochVisitor));
    this.datastoreEpochs = epochVisitor.getDatastoreEpochs();
    this.epochsPerOwner = epochVisitor.getEpochsPerOwner();
  }

  public void feedDatastore(final IOpenedTransaction transaction) {
    feedRawChunks(transaction);
    replicateChunksForMissingEpochs(transaction);
  }

  private void feedRawChunks(final IOpenedTransaction transaction) {
    statistics.forEach(statistic -> statistic.accept(
        new FeedVisitor(transaction.getMetadata(), transaction, dumpName)));
  }

  private void replicateChunksForMissingEpochs(final IOpenedTransaction transaction) {
    datastoreEpochs.forEach(epochId -> {
      for (final ChunkOwner owner : epochsPerOwner.keySet()) {
        final SortedSet<Long> epochs = epochsPerOwner.get(owner);
        if (!epochs.contains(epochId)) {

          final SortedSet<Long> priorEpochs = epochs.headSet(epochId);
          if (!priorEpochs.isEmpty()) {
            final long baseEpoch = priorEpochs.last();
            transaction.add(
                DatastoreConstants.EPOCH_VIEW_STORE, owner, dumpName, baseEpoch, epochId);
          }

        } else {
          transaction.add(DatastoreConstants.EPOCH_VIEW_STORE, owner, dumpName, epochId, epochId);
        }
      }

      return true;
    });
  }
}
