/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import static com.qfs.monitoring.statistic.memory.MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS;
import static com.qfs.monitoring.statistic.memory.MemoryStatisticConstants.ATTR_NAME_DICTIONARY_ID;

import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.store.structure.impl.StructureDictionaryManager;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;

/**
 * {@link IMemoryStatisticVisitor} implementation for visiting {@link
 * MemoryStatisticConstants#STAT_NAME_LEVEL} named statistics.
 *
 * @author ActiveViam
 */
public class LevelStatisticVisitor extends AFeedVisitor<Void> {

  private final PivotFeederVisitor parent;
  private final IOpenedTransaction transaction;

  private ParentType directParentType;
  private String directParentId;

  private final Long epochId;

  /** The number of members of the visited level. */
  protected Integer memberCount;

  protected Long dictionaryId;
  protected String dictionaryClass;
  protected Integer dictionarySize;
  protected Integer dictionaryOrder;

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
      final IDatastoreSchemaMetadata storageMetadata,
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
   * Initialize the visit of the children {@link IMemoryStatistic}.
   *
   * @param root parent of the children to ve visited
   */
  public void analyze(final IMemoryStatistic root) {
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

    final IRecordFormat format = getChunkFormat(this.storageMetadata);
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
        MemoryAnalysisDatastoreDescription.NO_PARTITION);

    if (this.dictionaryId != null) {
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__PARENT_DICO_ID, this.dictionaryId);
    }
    FeedVisitor.writeChunkTupleForFields(stat, transaction, null, format, tuple);

    visitChildren(stat);

    return null;
  }

  @Override
  public Void visit(DictionaryStatistic stat) {
    final IRecordFormat format = getDictionaryFormat(this.storageMetadata);
    final Long previousDictionaryId = this.dictionaryId;
    final Integer previousSize = this.dictionarySize;
    final Integer previousOrder = this.dictionaryOrder;
    final String previousDictionaryClass = this.dictionaryClass;

    if (!stat.getName().equals(MemoryStatisticConstants.STAT_NAME_DICTIONARY_UNDERLYING)) {
      final IStatisticAttribute dictionaryIdAttribute = stat.getAttribute(ATTR_NAME_DICTIONARY_ID);
      if (dictionaryIdAttribute != null) {
        this.dictionaryId = dictionaryIdAttribute.asLong();
      }

      final var classAttribute = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_CLASS);
      if (classAttribute != null) {
        this.dictionaryClass = classAttribute.asText();
      } else if (previousDictionaryClass == null) {
        this.dictionaryClass = stat.getAttribute(ATTR_NAME_CREATOR_CLASS).asText();
      }

      final var sizeAttribute = stat.getAttribute(DatastoreConstants.DICTIONARY_SIZE);
      if (sizeAttribute != null) {
        this.dictionarySize = sizeAttribute.asInt();
      }

      final var orderAttribute = stat.getAttribute(DatastoreConstants.DICTIONARY_ORDER);
      if (orderAttribute != null) {
        this.dictionaryOrder = orderAttribute.asInt();
      }

      if (!dictionaryClass.equals(StructureDictionaryManager.class.getName())) {
        final Object[] tuple = FeedVisitor.buildDictionaryTupleFrom(
            format, dictionaryId, dictionaryClass, dictionarySize, dictionaryOrder);
        FeedVisitor.setTupleElement(
            tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
        FeedVisitor.setTupleElement(
            tuple, format, DatastoreConstants.VERSION__EPOCH_ID, this.epochId);

        FeedVisitor.add(stat, this.transaction, DatastoreConstants.DICTIONARY_STORE, tuple);
      }
    }

    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentType = ParentType.DICTIONARY;
    this.directParentId = String.valueOf(this.dictionaryId);

    // We are processing a hierarchy/level
    visitChildren(stat);

    this.dictionaryId = previousDictionaryId;
    this.dictionarySize = previousSize;
    this.dictionaryOrder = previousOrder;
    this.dictionaryClass = previousDictionaryClass;
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

  private void recordLevelForStructure(final ParentType type, final String id) {
    final IRecordFormat format =
        FeedVisitor.getRecordFormat(this.storageMetadata, DatastoreConstants.CHUNK_TO_LEVEL_STORE);
    final Object[] tuple = new Object[format.getFieldCount()];
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
