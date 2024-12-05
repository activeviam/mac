/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.activeviam.database.api.conditions.BaseConditions;
import com.activeviam.database.api.query.ListQuery;
import com.activeviam.database.api.schema.FieldPath;
import com.activeviam.database.api.schema.IDataTable;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.api.transaction.IOpenedTransaction;
import com.activeviam.database.datastore.api.transaction.stats.IDatastoreSchemaTransactionInformation;
import com.activeviam.mac.Loggers;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.DistributedCubeOwner;
import com.activeviam.mac.statistic.memory.visitor.impl.DistributedEpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.EpochView;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.mac.statistic.memory.visitor.impl.RegularEpochView;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.records.api.ICursor;
import com.activeviam.tech.records.api.IRecordReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/** This class is responsible for parsing memory statistics into an analysis datastore. */
public class AnalysisDatastoreFeeder {

  private static final Logger LOGGER = Logger.getLogger(Loggers.DATASTORE_LOADING);

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

  final IDatastore datastore;

  /**
   * Constructor.
   *
   * @param dumpName the dump name to assign to the statistic
   * @param datastore
   */
  public AnalysisDatastoreFeeder(final String dumpName, IDatastore datastore) {
    this.dumpName = dumpName;
    this.datastore = datastore;

    this.datastoreEpochs = new HashSet<>();
    this.regularEpochsPerOwner = new HashMap<>();
    this.distributedEpochsPerOwner = new HashMap<>();
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
      final IDataTable recordFormat,
      final ChunkOwner owner,
      final String dumpName,
      final long baseEpochId,
      final EpochView epochView) {
    final Object[] tuple = new Object[recordFormat.getFields().size()];
    tuple[recordFormat.getFieldIndex(DatastoreConstants.EPOCH_VIEW__OWNER)] = owner;
    tuple[recordFormat.getFieldIndex(DatastoreConstants.CHUNK__DUMP_NAME)] = dumpName;
    tuple[recordFormat.getFieldIndex(DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID)] = baseEpochId;
    tuple[recordFormat.getFieldIndex(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID)] = epochView;
    return tuple;
  }

  /**
   * Loads the provided statistics into the datastore in a single transaction.
   *
   * @return the result of the transaction
   */
  public Optional<IDatastoreSchemaTransactionInformation> loadInto(
      final Stream<? extends AMemoryStatistic> stats) {
    return datastore.edit(transaction -> loadWithTransaction(transaction, stats));
  }

  /** Loads the provided statistics within an open transaction. */
  public void loadWithTransaction(
      final IOpenedTransaction transaction, final Stream<? extends AMemoryStatistic> stats) {
    stats.forEach(stat -> feedChunk(transaction, stat));
    completeTransaction(transaction);
  }

  /**
   * Adds the chunks of this feeder's statistics to the transaction.
   *
   * @param transaction the transaction to add facts to
   */
  private void feedChunk(final IOpenedTransaction transaction, final AMemoryStatistic statistic) {
    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine("Start feeding the application with " + statistic);
    }

    statistic.accept(
        new FeedVisitor(this.datastore.getCurrentSchema(), transaction, this.dumpName));

    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine("Application processed " + statistic);
    }
  }

  /** Completes the loading, computing the viewed epochs. */
  private void completeTransaction(final IOpenedTransaction transaction) {
    collectEpochsFromOpenedTransaction(transaction);
    replicateChunksForMissingEpochs(transaction);
  }

  /**
   * Collects the epochs per owner in the given already open transaction.
   *
   * @param transaction the transaction to collect the epochs from
   */
  private void collectEpochsFromOpenedTransaction(final IOpenedTransaction transaction) {
    final ListQuery query =
        this.datastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(
                    FieldPath.of(DatastoreConstants.CHUNK__DUMP_NAME), this.dumpName))
            .withTableFields(DatastoreConstants.OWNER__OWNER, DatastoreConstants.VERSION__EPOCH_ID)
            .toQuery();
    final ICursor result = transaction.getQueryRunner().listQuery(query).run();

    for (final IRecordReader reader : result) {
      final ChunkOwner owner = (ChunkOwner) reader.read(0);
      final long epoch = reader.readLong(1);
      if (owner instanceof DistributedCubeOwner) {
        this.distributedEpochsPerOwner.computeIfAbsent(owner, key -> new HashSet<>()).add(epoch);
      } else {
        this.datastoreEpochs.add(epoch);
        this.regularEpochsPerOwner.computeIfAbsent(owner, key -> new TreeSet<>()).add(epoch);
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
    final var epochViewRecordFormat =
        this.datastore.getCurrentSchema().getTable(DatastoreConstants.EPOCH_VIEW_STORE);

    for (final var datastoreEpochId : this.datastoreEpochs) {
      for (final ChunkOwner owner : this.regularEpochsPerOwner.keySet()) {
        final SortedSet<Long> epochs = this.regularEpochsPerOwner.get(owner);
        if (!epochs.contains(datastoreEpochId)) {

          final SortedSet<Long> priorEpochs = epochs.headSet(datastoreEpochId);
          if (!priorEpochs.isEmpty()) {
            final long baseEpochId = priorEpochs.last();
            transaction.add(
                DatastoreConstants.EPOCH_VIEW_STORE,
                generateEpochViewTuple(
                    epochViewRecordFormat,
                    owner,
                    this.dumpName,
                    baseEpochId,
                    new RegularEpochView(datastoreEpochId)));
          }
        } else {
          transaction.add(
              DatastoreConstants.EPOCH_VIEW_STORE,
              generateEpochViewTuple(
                  epochViewRecordFormat,
                  owner,
                  this.dumpName,
                  datastoreEpochId,
                  new RegularEpochView(datastoreEpochId)));
        }
      }
    }
  }

  private void replicateDistributedEpochs(final IOpenedTransaction transaction) {
    final var epochViewRecordFormat =
        this.datastore.getCurrentSchema().getTable(DatastoreConstants.EPOCH_VIEW_STORE);

    for (final ChunkOwner owner : this.distributedEpochsPerOwner.keySet()) {
      final Collection<Long> epochs = this.distributedEpochsPerOwner.get(owner);
      for (final Long epochId : epochs) {
        transaction.add(
            DatastoreConstants.EPOCH_VIEW_STORE,
            generateEpochViewTuple(
                epochViewRecordFormat,
                owner,
                this.dumpName,
                epochId,
                new DistributedEpochView(owner.getName(), epochId)));
      }
    }
  }
}
