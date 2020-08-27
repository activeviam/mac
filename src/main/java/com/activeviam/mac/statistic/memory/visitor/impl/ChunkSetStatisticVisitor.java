/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.Workaround;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.StoreOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.qfs.fwk.services.InternalServiceException;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.impl.ChunkSet;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;
import java.time.Instant;
import java.util.Collections;

/**
 * Implementation of the {@link com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor}
 * class for {@link ChunkSetStatistic}.
 *
 * @author ActiveViam
 */
public class ChunkSetStatisticVisitor extends ADatastoreFeedVisitor<Void> {

  /** The record format of the store that stores the chunks. */
  protected final IRecordFormat chunkRecordFormat;

  /** The export date, found on the first statistics we read. */
  protected final Instant current;

  private final ParentType rootComponent;
  private final ParentType directParentType;
  private final String directParentId;
  /** The partition id of the visited statistic. */
  protected final int partitionId;

  /** ID of the current {@link ChunkSet}. */
  protected Long chunkSetId = null;

  private Integer chunkSize;
  private Integer freeRows;
  private Integer nonWrittenRows;

  /**
   * Constructor.
   *
   * @param storageMetadata metadata of the application datastore
   * @param transaction ongoing transaction
   * @param dumpName name of the ongoing import
   * @param current current time
   * @param store store being visited
   * @param rootComponent highest component holding the ChunkSet
   * @param parentType structure type of the parent of the Chunkset
   * @param parentId id of the parent of the ChunkSet
   * @param partitionId partition id of the parent if the chunkSet
   * @param indexId index id of the Chunkset
   * @param referenceId reference id of the chunkset
   */
  public ChunkSetStatisticVisitor(
      final IDatastoreSchemaMetadata storageMetadata,
      final IOpenedTransaction transaction,
      final String dumpName,
      final Instant current,
      final String store,
      final ParentType rootComponent,
      final ParentType parentType,
      final String parentId,
      final int partitionId,
      final Long indexId,
      final Long referenceId) {
    super(transaction, storageMetadata, dumpName);
    this.current = current;
    this.store = store;
    this.rootComponent = rootComponent;
    this.directParentType = parentType;
    this.directParentId = parentId;
    this.partitionId = partitionId;
    this.indexId = indexId;
    this.referenceId = referenceId;

    this.chunkRecordFormat =
        this.storageMetadata
            .getStoreMetadata(DatastoreConstants.CHUNK_STORE)
            .getStoreFormat()
            .getRecordFormat();
  }

  @Override
  public Void visit(final DefaultMemoryStatistic memoryStatistic) {
    if (memoryStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_ROW_MAPPING)) {

      FeedVisitor.visitChildren(this, memoryStatistic);

    } else if (VectorStatisticVisitor.isVector(memoryStatistic)) {
      @Workaround(jira = "PIVOT-4127", solution = "Support for 5.8.4- versions")
      final IMemoryStatistic vectorStat = memoryStatistic;
      visitVectorBlock(vectorStat);
    } else if (memoryStatistic
        .getName()
        .equals(MemoryStatisticConstants.STAT_NAME_CHUNK_OF_CHUNKSET)) {

      boolean isFieldSpecified =
          memoryStatistic.getAttributes().containsKey(MemoryStatisticConstants.ATTR_NAME_FIELD);
      if (isFieldSpecified) {
        final IStatisticAttribute fieldAttribute =
            memoryStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
        this.fields.push(Collections.singleton(fieldAttribute.asText()));
      }

      FeedVisitor.visitChildren(this, memoryStatistic);

      if (isFieldSpecified) {
        this.fields.pop();
      }
    } else if (memoryStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_CHUNK_ENTRY)) {

      // Remove this stat for a subchunk, particularly for vector chunks
      final Integer previousSize = this.chunkSize;
      final Integer previousFree = this.freeRows;
      final Integer previousNonWritten = this.nonWrittenRows;
      this.chunkSize = null;
      this.freeRows = null;
      this.nonWrittenRows = null;

      FeedVisitor.visitChildren(this, memoryStatistic);

      this.chunkSize = previousSize;
      this.freeRows = previousFree;
      this.nonWrittenRows = previousNonWritten;
    } else {
      handleUnknownDefaultStatistic(memoryStatistic);
    }

    return null;
  }

  @Override
  public Void visit(final ChunkSetStatistic statistic) {
    this.chunkSize = statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_LENGTH).asInt();
    this.freeRows = statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FREED_ROWS).asInt();
    this.nonWrittenRows =
        statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_NOT_WRITTEN_ROWS).asInt();
    this.chunkSetId =
        statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_CHUNKSET_ID).asLong();

    FeedVisitor.visitChildren(this, statistic);

    // Reset
    this.chunkSetId = null;
    this.chunkSize = null;
    this.freeRows = null;
    this.nonWrittenRows = null;

    return null;
  }

  @Override
  public Void visit(final ChunkStatistic chunkStatistic) {
    if (VectorStatisticVisitor.isVector(chunkStatistic)) {
      visitVectorBlock(chunkStatistic);
    } else {
      final boolean isFieldSpecified =
          chunkStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_CHUNK_OF_CHUNKSET);
      if (isFieldSpecified) {
        final IStatisticAttribute fieldAttribute =
            chunkStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
        this.fields.add(Collections.singleton(fieldAttribute.asText()));
      }

      final ChunkOwner owner = new StoreOwner(this.store);

      final IRecordFormat ownerFormat = AFeedVisitor.getOwnerFormat(this.storageMetadata);
      final Object[] ownerTuple =
          FeedVisitor.buildOwnerTupleFrom(ownerFormat, chunkStatistic, owner, this.dumpName);
      FeedVisitor.add(
          chunkStatistic, transaction, DatastoreConstants.CHUNK_TO_OWNER_STORE, ownerTuple);

      final IRecordFormat componentFormat = AFeedVisitor.getComponentFormat(this.storageMetadata);
      final Object[] componentTuple =
          FeedVisitor.buildComponentTupleFrom(
              componentFormat, chunkStatistic, this.rootComponent, this.dumpName);
      FeedVisitor.add(
          chunkStatistic, transaction, DatastoreConstants.CHUNK_TO_COMPONENT_STORE, componentTuple);

      final IRecordFormat format = this.chunkRecordFormat;
      final Object[] tuple = FeedVisitor.buildChunkTupleFrom(format, chunkStatistic);

      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, this.directParentType);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__PARENT_ID, this.directParentId);

      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);

      FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__OWNER, owner);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__COMPONENT, this.rootComponent);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__PARTITION_ID, this.partitionId);
      if (this.referenceId != null) {
        FeedVisitor.setTupleElement(
            tuple, format, DatastoreConstants.CHUNK__PARENT_REF_ID, this.referenceId);
      }
      if (this.indexId != null) {
        FeedVisitor.setTupleElement(
            tuple, format, DatastoreConstants.CHUNK__PARENT_INDEX_ID, this.indexId);
      }
      if (this.dictionaryId != null) {
        FeedVisitor.setTupleElement(
            tuple, format, DatastoreConstants.CHUNK__PARENT_DICO_ID, this.dictionaryId);
      }
      if (!this.fields.isEmpty()) {
        writeFieldRecordsForChunk(chunkStatistic);

        // todo vlg clear this if obsolete
        FeedVisitor.setTupleElement(
            tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_FIELD_NAME,
            this.fields.peek().iterator().next());
      }
      if (this.store != null) {
        FeedVisitor.setTupleElement(
            tuple, chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_STORE_NAME, this.store);
      }

      // Complete chunk info regarding size and usage if not defined by a parent
      if (this.chunkSize != null) {
        FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__SIZE, this.chunkSize);
      }
      if (this.freeRows != null) {
        FeedVisitor.setTupleElement(
            tuple, format, DatastoreConstants.CHUNK__FREE_ROWS, this.freeRows);
      }
      if (this.nonWrittenRows != null) {
        FeedVisitor.setTupleElement(
            tuple, format, DatastoreConstants.CHUNK__NON_WRITTEN_ROWS, this.nonWrittenRows);
      }

      // Debug
      if (MemoryAnalysisDatastoreDescription.ADD_DEBUG_TREE) {
        tuple[format.getFieldIndex(DatastoreConstants.CHUNK__DEBUG_TREE)] =
            StatisticTreePrinter.getTreeAsString(chunkStatistic);
      }
      // Set the chunk data to be added to the Chunk store
      FeedVisitor.add(chunkStatistic, this.transaction, DatastoreConstants.CHUNK_STORE, tuple);

      visitChildren(chunkStatistic);

      if (isFieldSpecified) {
        this.fields.pop();
      }
    }
    return null;
  }

  // region NOT RELEVANT
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

  // endregion

  private void visitVectorBlock(final IMemoryStatistic memoryStatistic) {
    final VectorStatisticVisitor subVisitor =
        new VectorStatisticVisitor(
            this.storageMetadata,
            this.transaction,
            this.dumpName,
            this.current,
            this.store,
            this.fields,
            this.partitionId);
    subVisitor.process(memoryStatistic);
  }

  @Workaround(
      jira = "PIVOT-4041",
      solution =
          "Some attributes were missing, causing a bad classification of the chunk."
              + "We cannot properly re-classify as mandatory attributes are still missing.")
  private void handleUnknownDefaultStatistic(final DefaultMemoryStatistic statistic) {
    if (statistic
        .getAttribute(MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS)
        .asText()
        .contains("com.qfs.chunk.direct.impl.Direct")) {
      throw new InternalServiceException(
          "A default statistic is representing a chunk. This is a known issue when export "
              + "statistics using ActivePivot 5.8.x in JDK11."
              + " This was solved in 5.8.4.\nStatistic: "
              + statistic);
    } else {
      throw new RuntimeException("unexpected statistic " + statistic);
    }
  }
}
