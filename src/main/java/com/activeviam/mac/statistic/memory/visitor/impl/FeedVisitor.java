/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import static com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS;

import com.activeviam.database.api.schema.IDataTable;
import com.activeviam.database.api.schema.IDatabaseSchema;
import com.activeviam.database.datastore.api.transaction.IOpenedTransaction;
import com.activeviam.mac.Loggers;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
import com.activeviam.tech.observability.api.memory.IStatisticAttribute;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.ChunkSetStatistic;
import com.activeviam.tech.observability.internal.memory.ChunkStatistic;
import com.activeviam.tech.observability.internal.memory.DefaultMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.DictionaryStatistic;
import com.activeviam.tech.observability.internal.memory.IMemoryStatisticVisitor;
import com.activeviam.tech.observability.internal.memory.IndexStatistic;
import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;
import com.activeviam.tech.observability.internal.memory.ReferenceStatistic;
import com.activeviam.tech.records.api.IRecordFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Visitor for chunks.
 *
 * @author ActiveViam
 */
public class FeedVisitor implements IMemoryStatisticVisitor<Void> {

  private static final Logger LOGGER = Logger.getLogger(Loggers.LOADING);

  private final IDatabaseSchema storageMetadata;
  private final IOpenedTransaction transaction;
  private final String dumpName;
  private static final AtomicLong chunkIdGenerator = new AtomicLong(0L);

  /**
   * Constructor.
   *
   * @param storageMetadata metadata of the Datastore schema
   * @param tm ongoing transaction
   * @param dumpName name of the import being currently executed
   */
  public FeedVisitor(
      final IDatabaseSchema storageMetadata, final IOpenedTransaction tm, final String dumpName) {
    this.storageMetadata = storageMetadata;
    this.transaction = tm;
    this.dumpName = dumpName;
  }

  /**
   * Adds a tuple to the current transaction.
   *
   * @param statistic statistic
   * @param transaction ongoing transaction
   * @param store store being appended
   * @param tuple tuple-ized data to add to the store
   */
  protected static void add(
      IMemoryStatistic statistic, IOpenedTransaction transaction, String store, Object... tuple) {
    transaction.add(store, tuple);
  }

  static Object[] buildChunkTupleFrom(final IDataTable format, final ChunkStatistic stat) {
    final Object[] tuple = new Object[format.getFields().size()];
    tuple[format.getFieldIndex(DatastoreConstants.CHUNK_ID)] = getChunkId(stat);
    tuple[format.getFieldIndex(DatastoreConstants.CHUNK__CLASS)] =
        stat.getAttribute(ATTR_NAME_CREATOR_CLASS).asText();
    tuple[format.getFieldIndex(DatastoreConstants.CHUNK__OFF_HEAP_SIZE)] = stat.getShallowOffHeap();
    tuple[format.getFieldIndex(DatastoreConstants.CHUNK__ON_HEAP_SIZE)] = stat.getShallowOnHeap();

    final IStatisticAttribute sizeAttribute =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_LENGTH);
    final int chunkSize = sizeAttribute == null ? 0 : sizeAttribute.asInt();
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__SIZE, chunkSize);

    // TODO(ope) we may want to do the same for chunk whenever possible
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__NON_WRITTEN_ROWS, 0);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__FREE_ROWS, 0);

    return tuple;
  }

  /**
   * Extracts the chunk ID from the given {@link ChunkStatistic}.
   *
   * @param stat the chunk statistic
   * @return the chunk ID
   */
  public static long getChunkId(final ChunkStatistic stat) {
    if (stat.getAttribute("chunkId") == null) {
      LOGGER.warning("Chunk ID is null for chunk: " + stat.getName() + ", generating value.");
      return chunkIdGenerator.decrementAndGet();
    }
    return stat.getChunkId();
  }

  /**
   * Writes a record into the {@link DatastoreConstants#CHUNK_STORE} for each given field using the
   * given tuple as a base.
   *
   * <p>This method can modify the {@link DatastoreConstants#OWNER__FIELD} column of the given
   * tuple.
   *
   * @param statistic the statistic associated with the chunk
   * @param transaction the ongoing transaction
   * @param fields the fields associated with the chunk statistic
   * @param format the format of the tuple
   * @param tuple the base tuple to write records with
   */
  static void writeChunkTupleForFields(
      final ChunkStatistic statistic,
      final IOpenedTransaction transaction,
      final Collection<String> fields,
      final IDataTable format,
      final Object... tuple) {
    if (fields == null || fields.isEmpty()) {
      FeedVisitor.add(statistic, transaction, DatastoreConstants.CHUNK_STORE, tuple);
    } else {
      fields.forEach(
          field -> {
            FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.OWNER__FIELD, field);
            FeedVisitor.add(statistic, transaction, DatastoreConstants.CHUNK_STORE, tuple);
          });
    }
  }

  static Object[] buildDictionaryTupleFrom(
      final IDataTable format,
      final long dictionaryId,
      final String dictionaryClass,
      final int dictionarySize,
      final int dictionaryOrder) {
    final Object[] tuple = new Object[format.getFields().size()];
    tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)] = dictionaryId;
    tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_CLASS)] = dictionaryClass;
    tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_SIZE)] = dictionarySize;
    tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ORDER)] = dictionaryOrder;
    return tuple;
  }

  static Object[] buildVersionTupleFrom(
      final IDataTable format,
      final IMemoryStatistic statistic,
      final String dumpName,
      final long epochId,
      final String branch) {
    final Object[] tuple = new Object[format.getFields().size()];
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__DUMP_NAME, dumpName);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__EPOCH_ID, epochId);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__BRANCH_NAME, branch);
    return tuple;
  }

  /**
   * Visits all the children of the given {@link AMemoryStatistic}.
   *
   * @param visitor the visitor to use to visit the children
   * @param statistic The statistics whose children to visit
   */
  protected static void visitChildren(
      final IMemoryStatisticVisitor<?> visitor, final AMemoryStatistic statistic) {
    if (statistic.getChildren() != null) {
      for (final AMemoryStatistic child : statistic.getChildren()) {
        child.accept(visitor);
      }
    }
  }

  static void includeApplicationInfoIfAny(
      final IOpenedTransaction tm,
      final Instant date,
      final String dumpName,
      final IMemoryStatistic stat) {
    final IStatisticAttribute usedHeap =
        stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_HEAP_MEMORY);
    final IStatisticAttribute maxHeap =
        stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_HEAP_MEMORY);
    final IStatisticAttribute usedOffHeap =
        stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_DIRECT_MEMORY);
    final IStatisticAttribute maxOffHeap =
        stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_DIRECT_MEMORY);

    if (usedHeap != null && maxHeap != null && usedOffHeap != null && maxOffHeap != null) {
      tm.add(
          DatastoreConstants.APPLICATION_STORE,
          dumpName,
          date,
          usedHeap.asLong(),
          maxHeap.asLong(),
          usedOffHeap.asLong(),
          maxOffHeap.asLong());
    } else {
      throw new RuntimeException(
          "Missing memory properties in " + stat.getName() + ": " + stat.getAttributes());
    }
  }

  static IDataTable getRecordFormat(final IDatabaseSchema storageMetadata, final String storeName) {
    return storageMetadata.getTable(storeName);
  }

  /**
   * Adds a value to a given field of a tuple.
   *
   * @param tuple tuple to be filled
   * @param format {@link IRecordFormat} the tuple must match
   * @param field field we're inserting data on
   * @param value data to insert
   */
  protected static void setTupleElement(
      Object[] tuple, IDataTable format, String field, final Object value) {
    if (value == null) {
      throw new RuntimeException("Expected a non-null value for field " + field);
    }
    tuple[format.getFieldIndex(field)] = value;
  }

  /**
   * Checks the format of a tuple correspond to the given input.
   *
   * @param tuple tuple to be checked
   * @param format format the tuple must match
   */
  protected static void checkTuple(Object[] tuple, IRecordFormat format) {
    for (int i = 0; i < tuple.length; i++) {
      if (tuple[i] == null) {
        String field = format.getFieldName(i);
        throw new RuntimeException(
            "Unexpected null value for field " + field + " in tuple: " + Arrays.toString(tuple));
      }
    }
  }

  @Override
  public Void visit(DefaultMemoryStatistic stat) {
    switch (stat.getName()) {
      case MemoryStatisticConstants.STAT_NAME_DATASTORE:
      case MemoryStatisticConstants.STAT_NAME_MULTIVERSION_STORE:
      case MemoryStatisticConstants.STAT_NAME_STORE:
        final DatastoreFeederVisitor visitor =
            new DatastoreFeederVisitor(this.storageMetadata, this.transaction, this.dumpName);
        visitor.startFrom(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_MANAGER:
      case MemoryStatisticConstants.STAT_NAME_MULTIVERSION_PIVOT:
      case MemoryStatisticConstants.STAT_NAME_PIVOT:
        final PivotFeederVisitor feed =
            new PivotFeederVisitor(this.storageMetadata, this.transaction, this.dumpName);
        feed.startFrom(stat);
        break;
      default:
        visitChildren(this, stat);
    }

    return null;
  }

  @Override
  public Void visit(AMemoryStatistic memoryStatistic) {
    System.err.println(
        "Unexpected type of statistics : "
            + memoryStatistic
            + " which is a "
            + memoryStatistic.getClass().getSimpleName());
    visitChildren(this, memoryStatistic);
    return null;
  }

  @Override
  public Void visit(ChunkSetStatistic chunkSetStatistic) {
    return null;
  }

  @Override
  public Void visit(ChunkStatistic chunkStatistic) {
    return null;
  }

  @Override
  public Void visit(ReferenceStatistic referenceStatistic) {
    return null;
  }

  @Override
  public Void visit(IndexStatistic indexStatistic) {
    return null;
  }

  @Override
  public Void visit(DictionaryStatistic dictionaryStatistic) {
    return null;
  }
}
