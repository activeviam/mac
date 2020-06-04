/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.entities.StoreOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;
import java.time.Instant;

/**
 * Implementation of the {@link com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor}
 * class for Vectors.
 *
 * @author ActiveViam
 */
public class VectorStatisticVisitor extends ADatastoreFeedVisitor<Void> {

  /** The record format of the store that stores the chunks. */
  protected final IRecordFormat chunkRecordFormat;

  /** The export date, found on the first statistics we read. */
  protected final Instant current;

  /** The partition id of the visited statistic. */
  protected final int partitionId;

  /**
   * Constructor.
   *
   * @param storageMetadata metadata of the application datastore
   * @param transaction ongoing transaction
   * @param dumpName name of the ongoing import
   * @param current current time
   * @param store store being visited
   * @param partitionId partition id of the parent if the chunkSet
   */
  public VectorStatisticVisitor(
      final IDatastoreSchemaMetadata storageMetadata,
      final IOpenedTransaction transaction,
      final String dumpName,
      final Instant current,
      final String store,
      final int partitionId) {
    super(transaction, storageMetadata, dumpName);
    this.current = current;
    this.store = store;
    this.partitionId = partitionId;

    this.chunkRecordFormat =
        this.storageMetadata
            .getStoreMetadata(DatastoreConstants.CHUNK_STORE)
            .getStoreFormat()
            .getRecordFormat();
  }

  /**
   * Tests if a statistic represents a Vector.
   *
   * @param statistic instance to test
   * @return true for a Vector, that this Visitor must handle
   */
  static boolean isVector(final IMemoryStatistic statistic) {
    return statistic.getName().equals(MemoryStatisticConstants.STAT_NAME_VECTOR_BLOCK)
        || (statistic
                .getAttributes()
                .containsKey(MemoryStatisticConstants.ATTR_NAME_VECTOR_OFFHEAP_PRINT)
            && isVectorName(statistic.getName()));
  }

  /**
   * Tests if the name of the statistic is matching the list of known entities representing vectors.
   *
   * @param statisticName name of the visited statistic
   * @return true for a stat on a vector
   */
  static boolean isVectorName(final String statisticName) {
    return statisticName.equals(MemoryStatisticConstants.STAT_NAME_CHUNK_ENTRY)
        || statisticName.endsWith("BlockVector");
  }

  /**
   * Process the Vector element and all its children.
   *
   * @param statistic root statistic
   */
  public void process(final IMemoryStatistic statistic) {
    statistic.accept(this);
  }

  @Override
  public Void visit(DefaultMemoryStatistic memoryStatistic) {
    // TODO we could store the information on that vector in another dedicated store.
    FeedVisitor.visitChildren(this, memoryStatistic);
    return null;
  }

  @Override
  public Void visit(final ChunkStatistic chunkStatistic) {
    if (chunkStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_VECTOR_BLOCK)) {
      visitVectorBlock(chunkStatistic);
    } else {
      throw new IllegalStateException(
          "Unexpected statistics for chunks. Got "
              + chunkStatistic
              + "from "
              + StatisticTreePrinter.getTreeAsString(chunkStatistic));
    }
    return null;
  }

  // NOT RELEVANT

  @Override
  public Void visit(final ChunkSetStatistic statistic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visit(ReferenceStatistic referenceStatistic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visit(IndexStatistic indexStatistic) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Void visit(DictionaryStatistic dictionaryStatistic) {
    throw new UnsupportedOperationException();
  }

  /**
   * Visits a chunk containing only vectors.
   *
   * <p>This is the actual block containing all the data stored in various vectors, not the objects
   * representing vectors.
   *
   * @param statistic statistics about a block of vector items.
   */
  protected void visitVectorBlock(final ChunkStatistic statistic) {
    assert statistic.getChildren().isEmpty() : "Vector statistics with children";

    final IRecordFormat format = this.chunkRecordFormat;
    final Object[] tuple = FeedVisitor.buildChunkTupleFrom(format, statistic);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__PARENT_TYPE, ParentType.VECTOR_BLOCK);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__PARENT_ID, "None");

    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__OWNER, new StoreOwner(this.store));
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__COMPONENT, ParentType.VECTOR_BLOCK);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__PARTITION_ID, this.partitionId);

    // Debug
    if (MemoryAnalysisDatastoreDescription.ADD_DEBUG_TREE) {
      tuple[format.getFieldIndex(DatastoreConstants.CHUNK__DEBUG_TREE)] =
          StatisticTreePrinter.getTreeAsString(statistic);
    }

    FeedVisitor.add(statistic, this.transaction, DatastoreConstants.CHUNK_STORE, tuple);

    visitChildren(statistic);
  }
}
