/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.CubeOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.PivotMemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.query.ICompiledGetByKey;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.record.IRecordReader;
import com.qfs.store.transaction.IOpenedTransaction;

/**
 * {@link IMemoryStatisticVisitor} implementation for visiting {@link
 * PivotMemoryStatisticConstants.STAT_NAME_LEVEL} named statistics.
 *
 * @author ActiveViam
 */
public class LevelStatisticVisitor extends AFeedVisitor<Void> {

  private final PivotFeederVisitor parent;
  private final IOpenedTransaction transaction;

  private final ICompiledGetByKey chunkIdCQ;

  private ParentType directParentType;
  private String directParentId;

  Integer memberCount;
  Long dictionaryId;

  /**
   * Constuctor.
   *
   * @param parent Parent pivot statistic visitor
   * @param transaction current transaction
   * @param storageMetadata datastore metadata schema
   * @param dumpName name of the import being processsed
   */
  public LevelStatisticVisitor(
      final PivotFeederVisitor parent,
      final IOpenedTransaction transaction,
      final IDatastoreSchemaMetadata storageMetadata,
      final String dumpName) {
    super(transaction, storageMetadata, dumpName);
    this.parent = parent;
    this.transaction = transaction;

    this.directParentType = ParentType.LEVEL;
    this.directParentId =
        parent.pivot + "/" + parent.dimension + "/" + parent.hierarchy + "/" + parent.level;

    this.chunkIdCQ =
        this.transaction
            .getQueryRunner()
            .createGetByKeyQuery(
                DatastoreConstants.CHUNK_STORE,
                DatastoreConstants.CHUNK_ID,
                DatastoreConstants.CHUNK__DUMP_NAME);
  }

  /**
   * Initialize the visit of the children {@link IMemoryStatistic}.
   *
   * @param root parent of the children to ve visited
   */
  public void analyse(final IMemoryStatistic root) {
    visitChildren(root);
  }

  private void processLevelMember(final IMemoryStatistic stat) {
    this.memberCount =
        stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_LEVEL_MEMBER_COUNT).asInt();
    assert stat.getChildren().isEmpty();
  }

  @Override
  public Void visit(final ChunkStatistic stat) {

    recordLevelForStructure(this.directParentType, this.directParentId);

    final ChunkOwner owner = new CubeOwner(this.parent.pivot);

    final IRecordFormat ownerFormat = AFeedVisitor.getOwnerFormat(this.storageMetadata);
    final Object[] ownerTuple =
        FeedVisitor.buildOwnerTupleFrom(ownerFormat, stat, owner, this.dumpName, ParentType.LEVEL);
    FeedVisitor.add(stat, transaction, DatastoreConstants.OWNER_STORE, ownerTuple);

    final IRecordFormat format = getChunkFormat(this.storageMetadata);
    final Object[] tuple = FeedVisitor.buildChunkTupleFrom(format, stat);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, this.directParentType);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__PARENT_ID, this.directParentId);

    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__OWNER, owner);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__COMPONENT, ParentType.LEVEL);
    FeedVisitor.setTupleElement(
        tuple,
        format,
        DatastoreConstants.CHUNK__PARTITION_ID,
        MemoryAnalysisDatastoreDescription.NO_PARTITION);

    if (this.dictionaryId != null) {
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__PARENT_DICO_ID, this.dictionaryId);
    }

    final IRecordReader r =
        this.chunkIdCQ.runInTransaction(new Object[] {stat.getChunkId(), this.dumpName}, false);
    if (r != null) {
      // There is already an entry that has likely been set by the DatastoreFeederVisitor. We do not
      // need to keep on
      return null; // Abort
    }

    this.transaction.add(DatastoreConstants.CHUNK_STORE, tuple);

    visitChildren(stat);

    return null;
  }

  @Override
  public Void visit(DictionaryStatistic stat) {
    if (this.dictionaryId != null) {
      throw new RuntimeException("Already visited a dictionary: " + this.dictionaryId);
    }

    final IRecordFormat format = getDictionaryFormat(this.storageMetadata);
    final Object[] tuple = FeedVisitor.buildDictionaryTupleFrom(format, stat);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.APPLICATION__DUMP_NAME, this.dumpName);

    this.dictionaryId = (Long) tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)];
    this.transaction.add(DatastoreConstants.DICTIONARY_STORE, tuple);

    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentType = ParentType.DICTIONARY;
    this.directParentId = String.valueOf(this.dictionaryId);

    // We are processing a hierarchy/level
    visitChildren(stat);
    // Do not nullify dictionaryId. It is done after visiting the whole level

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
    switch (stat.getName()) {
      case PivotMemoryStatisticConstants.STAT_NAME_LEVEL_MEMBERS:
        processLevelMember(stat);
        break;
      default:
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
        tuple, format, DatastoreConstants.CHUNK_TO_LEVEL__PIVOT_ID, this.parent.pivot);
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
