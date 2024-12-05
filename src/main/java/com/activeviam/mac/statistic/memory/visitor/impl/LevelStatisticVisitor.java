/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.database.api.schema.IDatabaseSchema;
import com.activeviam.database.datastore.api.transaction.IOpenedTransaction;
import com.activeviam.mac.Loggers;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.ChunkSetStatistic;
import com.activeviam.tech.observability.internal.memory.ChunkStatistic;
import com.activeviam.tech.observability.internal.memory.DefaultMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.DictionaryStatistic;
import com.activeviam.tech.observability.internal.memory.IMemoryStatisticVisitor;
import com.activeviam.tech.observability.internal.memory.IndexStatistic;
import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;
import com.activeviam.tech.observability.internal.memory.ReferenceStatistic;
import java.util.logging.Logger;

/**
 * {@link IMemoryStatisticVisitor} implementation for visiting {@link
 * MemoryStatisticConstants#STAT_NAME_LEVEL} named statistics.
 *
 * @author ActiveViam
 */
public class LevelStatisticVisitor extends AFeedVisitorWithDictionary<Void> {

  private static final Logger LOGGER = Logger.getLogger(Loggers.ACTIVEPIVOT_LOADING);

  private final PivotFeederVisitor parent;
  private final IOpenedTransaction transaction;
  private final Long epochId;

  /** The number of members of the visited level. */
  protected Integer memberCount;

  private ParentType directParentType;
  private String directParentId;

  /**
   * Constructor.
   *
   * @param parent Parent pivot statistic visitor
   * @param transaction current transaction
   * @param storageMetadata datastore metadata schema
   * @param dumpName name of the import being processed
   * @param epochId the epoch id of the current statistic
   */
  public LevelStatisticVisitor(
      final PivotFeederVisitor parent,
      final IOpenedTransaction transaction,
      final IDatabaseSchema storageMetadata,
      final String dumpName,
      final Long epochId) {
    super(transaction, storageMetadata, dumpName);
    this.parent = parent;
    this.owner = parent.owner;
    this.transaction = transaction;
    this.epochId = epochId;

    this.directParentType = ParentType.LEVEL;
    this.directParentId =
        parent.owner.getName()
            + "/"
            + parent.dimension
            + "/"
            + parent.hierarchy
            + "/"
            + parent.level;
  }

  /**
   * Initialize the visit of the children {@link AMemoryStatistic}.
   *
   * @param root parent of the children to ve visited
   */
  public void analyze(final AMemoryStatistic root) {
    visitChildren(root);
  }

  private void processLevelMember(final IMemoryStatistic stat) {
    this.memberCount =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_LEVEL_MEMBER_COUNT).asInt();
    assert stat.getChildren().isEmpty();
  }

  @Override
  public Void visit(final ChunkStatistic stat) {

    recordLevelForStructure(this.directParentType, this.directParentId);

    final var format = getChunkFormat(this.storageMetadata);
    final Object[] tuple = FeedVisitor.buildChunkTupleFrom(format, stat);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__EPOCH_ID, this.epochId);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.OWNER__OWNER, this.owner);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.OWNER__FIELD, this.parent.directParentId);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.OWNER__COMPONENT, ParentType.LEVEL);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, this.directParentType);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__PARENT_ID, this.directParentId);

    FeedVisitor.setTupleElement(
        tuple,
        format,
        DatastoreConstants.CHUNK__PARTITION_ID,
        MemoryAnalysisDatastoreDescriptionConfig.NO_PARTITION);

    final Long dictionaryId = this.dictionaryAttributes.getDictionaryId();
    if (dictionaryId != null) {
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__PARENT_DICO_ID, dictionaryId);
    }
    FeedVisitor.writeChunkTupleForFields(stat, this.transaction, null, format, tuple);

    visitChildren(stat);

    return null;
  }

  @Override
  public Void visit(DictionaryStatistic stat) {
    final var previousDictionaryAttributes = processDictionaryStatistic(stat, this.epochId);
    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;

    this.directParentType = ParentType.DICTIONARY;
    this.directParentId = String.valueOf(this.dictionaryAttributes.getDictionaryId());

    // We are processing a hierarchy/level
    visitChildren(stat);

    this.dictionaryAttributes = previousDictionaryAttributes;
    this.directParentType = previousParentType;
    this.directParentId = previousParentId;

    return null;
  }

  @Override
  public Void visit(final ChunkSetStatistic stat) {
    visitChildren(stat);
    return null;
  }

  @Override
  public Void visit(ReferenceStatistic stat) {
    throw new RuntimeException("Cannot find a store reference under a level. Got: " + stat);
  }

  @Override
  public Void visit(IndexStatistic stat) {
    throw new RuntimeException("Cannot find a store index under a level. Got: " + stat);
  }

  @Override
  public Void visit(final DefaultMemoryStatistic stat) {
    if (MemoryStatisticConstants.STAT_NAME_LEVEL_MEMBERS.equals(stat.getName())) {
      processLevelMember(stat);
    } else {
      visitChildren(stat);
    }
    return null;
  }

  @Override
  public Void visit(AMemoryStatistic memoryStatistic) {
    visitChildren(memoryStatistic);
    return null;
  }

  private void recordLevelForStructure(final ParentType type, final String id) {
    final var format =
        FeedVisitor.getRecordFormat(this.storageMetadata, DatastoreConstants.CHUNK_TO_LEVEL_STORE);
    final Object[] tuple = new Object[format.getFields().size()];
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK_TO_LEVEL__MANAGER_ID, this.parent.manager);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK_TO_LEVEL__PIVOT_ID, this.owner.getName());
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK_TO_LEVEL__DIMENSION, this.parent.dimension);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK_TO_LEVEL__HIERARCHY, this.parent.hierarchy);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK_TO_LEVEL__LEVEL, this.parent.level);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK_TO_LEVEL__PARENT_TYPE, type);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK_TO_LEVEL__PARENT_ID, id);

    this.transaction.add(DatastoreConstants.CHUNK_TO_LEVEL_STORE, tuple);
  }
}
