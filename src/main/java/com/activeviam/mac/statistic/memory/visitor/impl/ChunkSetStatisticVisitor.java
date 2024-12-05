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
import com.activeviam.mac.Workaround;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.UsedByVersion;
import com.activeviam.tech.chunks.internal.chunkset.impl.ChunkSet;
import com.activeviam.tech.core.api.exceptions.service.InternalServiceException;
import com.activeviam.tech.observability.api.memory.IStatisticAttribute;
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
import java.util.Collections;

/**
 * Implementation of the {@link
 * com.activeviam.tech.observability.internal.memory.IMemoryStatisticVisitor} class for {@link
 * ChunkSetStatistic}.
 *
 * @author ActiveViam
 */
public class ChunkSetStatisticVisitor extends ADatastoreFeedVisitor<Void> {

  /** The record format of the store that stores the chunks. */
  protected final IDataTable chunkRecordFormat;

  /** The export date, found on the first statistics we read. */
  protected final Instant current;

  /** Type of the root structure. */
  protected final ParentType rootComponent;

  /** Type of the direct parent structure. */
  protected final ParentType directParentType;

  /** id of the direct parent structure. */
  protected final String directParentId;

  /** Aggregate provider being currently visited. */
  protected final Long providerId;

  /** The partition id of the visited statistic. */
  protected final int partitionId;

  /** Whether or not to ignore the field attributes of the visited statistics. */
  protected final boolean ignoreFieldSpecifications;

  /** The epoch id we are currently reading statistics for. */
  protected Long epochId;

  /**
   * Whether or not the currently visited statistics were flagged as used by the current version.
   */
  protected UsedByVersion usedByVersion;

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
   * @param owner owner being visited
   * @param rootComponent highest component holding the ChunkSet
   * @param parentType structure type of the parent of the Chunkset
   * @param parentId id of the parent of the ChunkSet
   * @param partitionId partition id of the parent of the ChunkSet
   * @param indexId index id of the Chunkset
   * @param referenceId reference id of the chunkset
   * @param providerId id of the parent provider
   * @param epochId the epoch id of the chunkset
   * @param usedByVersion the used by version flag for the Chunkset
   * @param ignoreFieldSpecifications whether or not to attribute the visited chunkset's chunks to
   *     the encountered fields
   */
  public ChunkSetStatisticVisitor(
      final IDatabaseSchema storageMetadata,
      final IOpenedTransaction transaction,
      final String dumpName,
      final Instant current,
      final ChunkOwner owner,
      final ParentType rootComponent,
      final ParentType parentType,
      final String parentId,
      final int partitionId,
      final Long indexId,
      final Long referenceId,
      final Long providerId,
      final Long epochId,
      final UsedByVersion usedByVersion,
      final boolean ignoreFieldSpecifications) {
    super(transaction, storageMetadata, dumpName);
    this.current = current;
    this.owner = owner;
    this.rootComponent = rootComponent;
    this.directParentType = parentType;
    this.directParentId = parentId;
    this.partitionId = partitionId;
    this.indexId = indexId;
    this.referenceId = referenceId;
    this.providerId = providerId;
    this.epochId = epochId;
    this.usedByVersion = usedByVersion;
    this.ignoreFieldSpecifications = ignoreFieldSpecifications;

    this.chunkRecordFormat = this.storageMetadata.getTable(DatastoreConstants.CHUNK_STORE);
  }

  @Override
  public Void visit(final DefaultMemoryStatistic memoryStatistic) {
    if (memoryStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_ROW_MAPPING)) {

      FeedVisitor.visitChildren(this, memoryStatistic);

    } else if (VectorStatisticVisitor.isVector(memoryStatistic)) {
      @Workaround(jira = "PIVOT-4127", solution = "Support for 5.8.4- versions")
      final AMemoryStatistic vectorStat = memoryStatistic;
      visitVectorBlock(vectorStat);
    } else if (memoryStatistic
        .getName()
        .equals(MemoryStatisticConstants.STAT_NAME_CHUNK_OF_CHUNKSET)) {

      boolean isFieldSpecified =
          memoryStatistic.getAttributes().containsKey(MemoryStatisticConstants.ATTR_NAME_FIELD);
      final Collection<String> oldFields = this.fields;
      if (isFieldSpecified && !this.ignoreFieldSpecifications) {
        final IStatisticAttribute fieldAttribute =
            memoryStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
        this.fields = Collections.singleton(fieldAttribute.asText());
      }

      FeedVisitor.visitChildren(this, memoryStatistic);

      if (isFieldSpecified) {
        this.fields = oldFields;
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
    } else if (memoryStatistic.getName().contains("VectorHistory")
        || MemoryStatisticConstants.STAT_NAME_VECTOR.equals(memoryStatistic.getName())) {
      FeedVisitor.visitChildren(this, memoryStatistic);
    } else {
      handleUnknownDefaultStatistic(memoryStatistic);
    }

    return null;
  }

  @Override
  public Void visit(AMemoryStatistic memoryStatistic) {
    visitChildren(memoryStatistic);
    return null;
  }

  @Override
  public Void visit(final ChunkSetStatistic statistic) {
    this.chunkSize = statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_LENGTH).asInt();
    this.freeRows = statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FREED_ROWS).asInt();
    final IStatisticAttribute nonWrittenRowsAttribute =
        statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_NOT_WRITTEN_ROWS);
    this.nonWrittenRows = nonWrittenRowsAttribute != null ? nonWrittenRowsAttribute.asInt() : 0;
    this.chunkSetId =
        statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_CHUNKSET_ID).asLong();

    final UsedByVersion previousUsedByVersion = this.usedByVersion;
    final IStatisticAttribute usedByVersionAttribute =
        statistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_USED_BY_VERSION);
    if (usedByVersionAttribute != null) {
      this.usedByVersion =
          usedByVersionAttribute.asBoolean() ? UsedByVersion.TRUE : UsedByVersion.FALSE;
    }

    FeedVisitor.visitChildren(this, statistic);

    // Reset
    this.chunkSetId = null;
    this.chunkSize = null;
    this.freeRows = null;
    this.nonWrittenRows = null;
    this.usedByVersion = previousUsedByVersion;

    return null;
  }

  @Override
  public Void visit(final ChunkStatistic chunkStatistic) {
    if (VectorStatisticVisitor.isVector(chunkStatistic)) {
      visitVectorBlock(chunkStatistic);
    } else {
      final boolean isFieldSpecified =
          chunkStatistic.getName().equals(MemoryStatisticConstants.STAT_NAME_CHUNK_OF_CHUNKSET);
      Collection<String> oldFields = null;

      if (isFieldSpecified && !this.ignoreFieldSpecifications) {
        final IStatisticAttribute fieldAttribute =
            chunkStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
        oldFields = this.fields;
        this.fields = Collections.singleton(fieldAttribute.asText());
      }

      final var format = this.chunkRecordFormat;
      final Object[] tuple = FeedVisitor.buildChunkTupleFrom(format, chunkStatistic);

      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, this.directParentType);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__PARENT_ID, this.directParentId);
      FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.OWNER__OWNER, this.owner);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.OWNER__COMPONENT, this.rootComponent);

      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.VERSION__EPOCH_ID, this.epochId);
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__USED_BY_VERSION, this.usedByVersion);

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
      final Long dictionaryId = this.dictionaryAttributes.getDictionaryId();
      if (dictionaryId != null) {
        FeedVisitor.setTupleElement(
            tuple, format, DatastoreConstants.CHUNK__PARENT_DICO_ID, dictionaryId);
      }
      if (this.providerId != null) {
        FeedVisitor.setTupleElement(
            tuple, format, DatastoreConstants.CHUNK__PROVIDER_ID, this.providerId);
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
      if (MemoryAnalysisDatastoreDescriptionConfig.ADD_DEBUG_TREE) {
        tuple[format.getFieldIndex(DatastoreConstants.CHUNK__DEBUG_TREE)] =
            StatisticTreePrinter.getTreeAsString(chunkStatistic);
      }
      // Set the chunk data to be added to the Chunk store
      FeedVisitor.writeChunkTupleForFields(
          chunkStatistic, this.transaction, this.fields, format, tuple);

      visitChildren(chunkStatistic);

      if (isFieldSpecified) {
        this.fields = oldFields;
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

  private void visitVectorBlock(final AMemoryStatistic memoryStatistic) {
    final VectorStatisticVisitor subVisitor =
        new VectorStatisticVisitor(
            this.storageMetadata,
            this.transaction,
            this.dumpName,
            this.current,
            this.owner,
            this.fields,
            this.partitionId,
            this.epochId,
            this.usedByVersion);
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
