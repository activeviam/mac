/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.database.api.schema.IDataTable;
import com.activeviam.database.api.schema.IDatabaseSchema;
import com.activeviam.database.datastore.api.transaction.IOpenedTransaction;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.UsedByVersion;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.ChunkSetStatistic;
import com.activeviam.tech.observability.internal.memory.ChunkStatistic;
import com.activeviam.tech.observability.internal.memory.DefaultMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.DictionaryStatistic;
import com.activeviam.tech.observability.internal.memory.IndexStatistic;
import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;
import com.activeviam.tech.observability.internal.memory.ReferenceStatistic;
import java.time.Instant;
import java.util.Collection;

/**
 * Implementation of the {@link
 * com.activeviam.tech.observability.internal.memory.IMemoryStatisticVisitor} class for Vectors.
 *
 * @author ActiveViam
 */
public class VectorStatisticVisitor extends AFeedVisitor<Void> {

  /** The record format of the store that stores the chunks. */
  protected final IDataTable chunkRecordFormat;

  /** The export date, found on the first statistics we read. */
  protected final Instant current;

  /** The partition id of the visited statistic. */
  protected final int partitionId;

  private final UsedByVersion usedByVersion;

  /** The epoch id we are currently reading statistics for. */
  protected Long epochId;

  /** The fields corresponding to the vector block statistic. */
  protected Collection<String> fields;

  /**
   * Constructor.
   *
   * @param storageMetadata metadata of the application datastore
   * @param transaction ongoing transaction
   * @param dumpName name of the ongoing import
   * @param current current time
   * @param owner owner being visited
   * @param fields the fields related to the current statistic
   * @param partitionId partition id of the parent if the chunkSet
   * @param epochId the epoch id of the current statistic
   * @param usedByVersion the used by version flag for the current statistic
   */
  public VectorStatisticVisitor(
      final IDatabaseSchema storageMetadata,
      final IOpenedTransaction transaction,
      final String dumpName,
      final Instant current,
      final ChunkOwner owner,
      final Collection<String> fields,
      final int partitionId,
      final Long epochId,
      final UsedByVersion usedByVersion) {
    super(transaction, storageMetadata, dumpName);
    this.current = current;
    this.fields = fields;
    this.owner = owner;
    this.partitionId = partitionId;
    this.epochId = epochId;
    this.usedByVersion = usedByVersion;

    this.chunkRecordFormat = this.storageMetadata.getTable(DatastoreConstants.CHUNK_STORE);
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
  public void process(final AMemoryStatistic statistic) {
    statistic.accept(this);
  }

  @Override
  public Void visit(DefaultMemoryStatistic memoryStatistic) {
    // TODO we could store the information on that vector in another dedicated store.
    FeedVisitor.visitChildren(this, memoryStatistic);
    return null;
  }

  @Override
  public Void visit(AMemoryStatistic memoryStatistic) {
    visitChildren(memoryStatistic);
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

    final var format = this.chunkRecordFormat;
    final Object[] tuple = FeedVisitor.buildChunkTupleFrom(format, statistic);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, ParentType.VECTOR_BLOCK);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__PARENT_ID, "None");
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.OWNER__OWNER, this.owner);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.OWNER__COMPONENT, ParentType.VECTOR_BLOCK);

    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__EPOCH_ID, this.epochId);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__USED_BY_VERSION, this.usedByVersion);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__PARTITION_ID, this.partitionId);

    FeedVisitor.setTupleElement(
        tuple,
        format,
        DatastoreConstants.CHUNK__VECTOR_BLOCK_LENGTH,
        statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_LENGTH).asLong());
    FeedVisitor.setTupleElement(
        tuple,
        format,
        DatastoreConstants.CHUNK__VECTOR_BLOCK_REF_COUNT,
        statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_BLOCK_REFERENCE_COUNT).asLong());

    // Debug
    if (MemoryAnalysisDatastoreDescriptionConfig.ADD_DEBUG_TREE) {
      tuple[format.getFieldIndex(DatastoreConstants.CHUNK__DEBUG_TREE)] =
          StatisticTreePrinter.getTreeAsString(statistic);
    }
    // Set the chunk data to be added to the Chunk store
    FeedVisitor.writeChunkTupleForFields(
        statistic, this.transaction, this.fields, this.chunkRecordFormat, tuple);

    visitChildren(statistic);
  }
}
