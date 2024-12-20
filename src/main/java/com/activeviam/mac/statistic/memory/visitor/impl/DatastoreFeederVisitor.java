/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import static com.activeviam.mac.statistic.memory.visitor.impl.DebugVisitor.DEBUG;

import com.activeviam.database.api.schema.IDataTable;
import com.activeviam.database.api.schema.IDatabaseSchema;
import com.activeviam.database.datastore.api.transaction.IOpenedTransaction;
import com.activeviam.database.datastore.private_.IStore;
import com.activeviam.mac.entities.StoreOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.UsedByVersion;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * This visitor is not reusable.
 *
 * <p>This visits memory statistics and commits them into the given datastore. The datastore must
 * have the schema defined by {@link MemoryAnalysisDatastoreDescriptionConfig}.
 *
 * @author ActiveViam
 */
public class DatastoreFeederVisitor extends ADatastoreFeedVisitor<Void> {

  /** The record format of the store that stores the chunks. */
  protected final IDataTable chunkRecordFormat;

  /**
   * A boolean that if true tells us that the currently visited component is responsible for storing
   * versioning data.
   */
  // FIXME find a cleaner way to do that. (specific stat for instance).
  protected boolean isVersionColumn = false;

  /** The export date, found on the first statistics we read. */
  protected Instant current = null;

  /** The epoch id we are currently reading statistics for. */
  protected Long epochId = null;

  /** Branch owning {@link #epochId}. */
  protected String branch = null;

  /**
   * Whether or not the currently visited statistics were flagged as used by the current version.
   */
  protected UsedByVersion usedByVersion = UsedByVersion.UNKNOWN;

  /** Type of the root component visited. */
  protected ParentType rootComponent;

  /** Types of the direct parent component owning the chunk. */
  protected ParentType directParentType;

  /** Id of the direct parent owning the chunk. */
  protected String directParentId;

  /** The partition id of the visited statistic. */
  protected Integer partitionId = null;

  /** Type of the currently visited index. */
  private IndexType indexType = null;

  /**
   * Constructor.
   *
   * @param storageMetadata structure of the metadata
   * @param transaction the open transaction to use to fill the datastore with the visited data
   * @param dumpName The name of the off-heap dump. Can be null.
   */
  public DatastoreFeederVisitor(
      final IDatabaseSchema storageMetadata,
      final IOpenedTransaction transaction,
      final String dumpName) {
    super(transaction, storageMetadata, dumpName);
    this.chunkRecordFormat = this.storageMetadata.getTable(DatastoreConstants.CHUNK_STORE);
  }

  private static Object[] buildIndexTupleFrom(final IDataTable format, final IndexStatistic stat) {
    final Object[] tuple = new Object[format.getFields().size()];
    tuple[format.getFieldIndex(DatastoreConstants.INDEX_ID)] =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_INDEX_ID).asLong();

    final String[] fieldNames =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELDS).asStringArray();
    assert fieldNames != null && fieldNames.length > 0
        : "Cannot find fields in the attributes of " + stat;
    Arrays.sort(fieldNames);
    tuple[format.getFieldIndex(DatastoreConstants.INDEX__FIELDS)] =
        new MemoryAnalysisDatastoreDescriptionConfig.StringArrayObject(fieldNames);

    tuple[format.getFieldIndex(DatastoreConstants.INDEX_CLASS)] =
        stat.getAttribute(DatastoreConstants.INDEX_CLASS).asText();

    return tuple;
  }

  /**
   * Starts navigating the tree of chunk statistics from the input entry point.
   *
   * @param stat Entry point for the traversal of the memory statistics tree
   */
  public void startFrom(final AMemoryStatistic stat) {

    if (this.current == null) {
      final IStatisticAttribute dateAtt =
          stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_DATE);

      this.current =
          Instant.ofEpochSecond(null != dateAtt ? dateAtt.asLong() : System.currentTimeMillis());

      readEpochAndBranchIfAny(stat);
      assert stat.getName().equalsIgnoreCase(MemoryStatisticConstants.STAT_NAME_MULTIVERSION_STORE)
          || this.epochId != null;

      FeedVisitor.includeApplicationInfoIfAny(this.transaction, this.current, this.dumpName, stat);

      try {
        stat.accept(this);
      } catch (final Exception e) {
        if (Boolean.TRUE.equals(DEBUG)) {
          final StatisticTreePrinter printer = DebugVisitor.createDebugPrinter(stat);
          printer.print();
        }
        throw e;
      }
    } else {
      throw new RuntimeException("Cannot reuse a feed instance");
    }
  }

  @Override
  public Void visit(final ChunkStatistic chunkStatistic) {

    final IStatisticAttribute usedByVersionAttribute =
        chunkStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_USED_BY_VERSION);
    if (usedByVersionAttribute != null) {
      this.usedByVersion =
          usedByVersionAttribute.asBoolean() ? UsedByVersion.TRUE : UsedByVersion.FALSE;
    }

    final Object[] tuple = FeedVisitor.buildChunkTupleFrom(this.chunkRecordFormat, chunkStatistic);
    if (this.isVersionColumn) {
      FeedVisitor.setTupleElement(
          tuple,
          this.chunkRecordFormat,
          DatastoreConstants.CHUNK__NON_WRITTEN_ROWS,
          chunkStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_NOT_WRITTEN_ROWS).asInt());
    }
    FeedVisitor.setTupleElement(
        tuple, this.chunkRecordFormat, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
    FeedVisitor.setTupleElement(
        tuple, this.chunkRecordFormat, DatastoreConstants.VERSION__EPOCH_ID, this.epochId);
    FeedVisitor.setTupleElement(
        tuple, this.chunkRecordFormat, DatastoreConstants.OWNER__OWNER, this.owner);
    FeedVisitor.setTupleElement(
        tuple, this.chunkRecordFormat, DatastoreConstants.OWNER__COMPONENT, this.rootComponent);

    FeedVisitor.setTupleElement(
        tuple,
        this.chunkRecordFormat,
        DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE,
        this.directParentType);
    FeedVisitor.setTupleElement(
        tuple, this.chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_ID, this.directParentId);
    if (this.referenceId != null) {
      FeedVisitor.setTupleElement(
          tuple, this.chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_REF_ID, this.referenceId);
    }
    if (this.indexId != null) {
      FeedVisitor.setTupleElement(
          tuple, this.chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_INDEX_ID, this.indexId);
    }
    final Long dictionaryId = this.dictionaryAttributes.getDictionaryId();
    if (dictionaryId != null) {
      FeedVisitor.setTupleElement(
          tuple, this.chunkRecordFormat, DatastoreConstants.CHUNK__PARENT_DICO_ID, dictionaryId);
    }
    tuple[this.chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__USED_BY_VERSION)] =
        this.usedByVersion;
    tuple[this.chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID)] =
        this.partitionId;
    if (MemoryAnalysisDatastoreDescriptionConfig.ADD_DEBUG_TREE) {
      tuple[this.chunkRecordFormat.getFieldIndex(DatastoreConstants.CHUNK__DEBUG_TREE)] =
          StatisticTreePrinter.getTreeAsString(chunkStatistic);
    }

    FeedVisitor.writeChunkTupleForFields(
        chunkStatistic, this.transaction, this.fields, this.chunkRecordFormat, tuple);

    visitChildren(chunkStatistic);

    return null;
  }

  @Override
  @SuppressWarnings("deprecation")
  public Void visit(final DefaultMemoryStatistic stat) {
    final Long initialEpoch = this.epochId;
    final String initialBranch = this.branch;

    if (readEpochAndBranchIfAny(stat)) {
      final var versionStoreFormat = getVersionStoreFormat(this.storageMetadata);
      final Object[] tuple =
          FeedVisitor.buildVersionTupleFrom(
              versionStoreFormat, stat, this.dumpName, this.epochId, this.branch);
      FeedVisitor.add(stat, this.transaction, DatastoreConstants.VERSION_STORE, tuple);
    }

    switch (stat.getName()) {
      case MemoryStatisticConstants.STAT_NAME_STORE:
        processStoreStat(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_STORE_PARTITION:
        processStorePartition(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_PRIMARY_INDICES:
      case MemoryStatisticConstants.STAT_NAME_UNIQUE_INDICES:
        processUniqueIndices(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_KEY_INDEX:
        processKeyIndices(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_SECONDARY_INDICES:
        processSecondaryIndices(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_RECORD_SET:
        processRecords(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN:
        processRecordVersion(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_DICTIONARY_MANAGER:
        processDictionaryManager(stat);
        break;
      default:
        recordStatAndExplore(stat);
        break;
    }

    this.epochId = initialEpoch;
    this.branch = initialBranch;

    return null;
  }

  @Override
  public Void visit(AMemoryStatistic memoryStatistic) {
    visitChildren(memoryStatistic);
    return null;
  }

  @Override
  public Void visit(final ChunkSetStatistic stat) {
    return new ChunkSetStatisticVisitor(
            this.storageMetadata,
            this.transaction,
            this.dumpName,
            this.current,
            this.owner,
            this.rootComponent,
            this.directParentType,
            this.directParentId,
            this.partitionId,
            this.indexId,
            this.referenceId,
            null,
            this.epochId,
            this.usedByVersion,
            false)
        .visit(stat);
  }

  @Override
  public Void visit(final ReferenceStatistic referenceStatistic) {
    final Object[] tuple = buildPartOfReferenceStatisticTuple(referenceStatistic);
    final var refStoreFormat = getReferenceFormat(this.storageMetadata);

    // fill out the tuple
    this.referenceId =
        referenceStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_REFERENCE_ID).asLong();

    tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_ID)] = this.referenceId;
    tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_NAME)] =
        referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_NAME).asText();
    tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_CLASS)] =
        referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_CLASS).asText();
    FeedVisitor.setTupleElement(
        tuple, refStoreFormat, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
    FeedVisitor.setTupleElement(
        tuple, refStoreFormat, DatastoreConstants.VERSION__EPOCH_ID, this.epochId);

    FeedVisitor.add(
        referenceStatistic, this.transaction, DatastoreConstants.REFERENCE_STORE, tuple);

    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentType = ParentType.REFERENCE;
    this.directParentId = String.valueOf(this.referenceId);
    this.rootComponent = ParentType.REFERENCE;
    visitChildren(referenceStatistic);
    this.directParentType = previousParentType;
    this.directParentId = previousParentId;
    this.rootComponent = null;

    // Reset
    this.referenceId = null;

    return null;
  }

  @Override
  public Void visit(final IndexStatistic stat) {
    final var format = getIndexFormat(this.storageMetadata);
    final Object[] tuple = buildIndexTupleFrom(format, stat);

    this.indexId = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_INDEX_ID).asLong();
    final boolean isKeyIndex = stat.getName().equals(MemoryStatisticConstants.STAT_NAME_KEY_INDEX);
    if (isKeyIndex) {
      this.indexType = IndexType.KEY;
    }

    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.INDEX_TYPE, this.indexType);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__EPOCH_ID, this.epochId);
    FeedVisitor.add(stat, this.transaction, DatastoreConstants.INDEX_STORE, tuple);

    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentType = ParentType.INDEX;
    this.directParentId = String.valueOf(this.indexId);
    this.rootComponent = ParentType.INDEX;

    final String[] fieldNames =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELDS).asStringArray();
    final Collection<String> oldFields = this.fields;
    assert fieldNames != null && fieldNames.length > 0
        : "Cannot find fields in the attributes of " + stat;
    this.fields = Arrays.asList(fieldNames);

    visitChildren(stat);

    this.directParentType = previousParentType;
    this.directParentId = previousParentId;
    this.rootComponent = null;
    this.fields = oldFields;

    // Reset
    this.indexId = null;
    if (isKeyIndex) {
      this.indexType = null;
    }

    return null;
  }

  @Override
  public Void visit(final DictionaryStatistic stat) {

    final IStatisticAttribute storageSpecification =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_STORAGE_SPECIFICATION);

    if (storageSpecification != null && !isSingleFieldStorage(storageSpecification.asText())) {
      return null;
    }

    final var previousDictionaryAttributes = processDictionaryStatistic(stat, this.epochId);
    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentType = ParentType.DICTIONARY;
    this.directParentId = String.valueOf(this.dictionaryAttributes.getDictionaryId());

    final Collection<String> oldFields = this.fields;
    readFieldsIfAny(stat);

    visitChildren(stat);

    // Reset
    this.directParentType = previousParentType;
    this.directParentId = previousParentId;
    this.dictionaryAttributes = previousDictionaryAttributes;
    this.fields = oldFields;

    return null;
  }

  /**
   * Checks whether or not the given storage specification concerns a single field.
   *
   * <h2>When importing data from an AP version prior to 5.10</h2>
   *
   * <p>Due to how {@code DictionaryManager} exports its dictionaries, dictionary entries that are
   * not {@code com.qfs.dic.impl.FieldStorageSpecification}s should be ignored, as they are
   * duplicates with potentially wrong field attributions.
   *
   * @param storageSpecification a string representing a storage specification
   * @return whether or not it corresponds to a single field storage specification
   */
  protected boolean isSingleFieldStorage(final String storageSpecification) {
    return storageSpecification.startsWith("F");
  }

  /**
   * Processes the statistic of a {@code DictionaryManager}.
   *
   * @param stat statistic to be processed
   */
  protected void processDictionaryManager(final AMemoryStatistic stat) {
    this.rootComponent = ParentType.DICTIONARY;
    visitChildren(stat);
    this.rootComponent = null;
  }

  /**
   * Processes the statistic of a {@link IStore}.
   *
   * @param stat statistic to be processed
   */
  protected void processStoreStat(final AMemoryStatistic stat) {
    final IStatisticAttribute nameAttr =
        Objects.requireNonNull(
            stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_STORE_NAME),
            () -> "No store name in stat " + stat);
    this.owner = new StoreOwner(nameAttr.asText());

    // Explore the store children
    visitChildren(stat);

    this.owner = null;
  }

  /**
   * Processes the statistic of a store partition.
   *
   * @param stat statistic to be processed
   */
  protected void processStorePartition(final AMemoryStatistic stat) {
    final IStatisticAttribute partitionAttr =
        Objects.requireNonNull(
            stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_PARTITION_ID),
            () -> "No partition attribute in stat" + stat);
    this.partitionId = partitionAttr.asInt();

    visitChildren(stat);

    this.partitionId = null;
  }

  private void processRecords(final AMemoryStatistic stat) {
    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;

    this.directParentType = ParentType.RECORDS;
    this.directParentId =
        String.valueOf(
            stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_RECORD_SET_ID).asLong());
    this.rootComponent = ParentType.RECORDS;
    visitChildren(stat);
    this.directParentType = previousParentType;
    this.directParentId = previousParentId;
    this.rootComponent = null;
  }

  private void processRecordVersion(final AMemoryStatistic stat) {
    this.isVersionColumn =
        MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN.equals(stat.getName());

    visitChildren(stat);
    this.isVersionColumn = false;
  }

  private void processUniqueIndices(final AMemoryStatistic stat) {
    processIndexList(stat, IndexType.UNIQUE);
  }

  private void processKeyIndices(final AMemoryStatistic stat) {
    processIndexList(stat, IndexType.KEY);
  }

  private void processSecondaryIndices(final AMemoryStatistic stat) {
    processIndexList(stat, IndexType.SECONDARY);
  }

  private void processIndexList(final AMemoryStatistic stat, final IndexType type) {
    final var oldComponent = this.rootComponent;
    final var oldParent = this.directParentType;

    this.indexType = type;
    this.rootComponent = ParentType.INDEX;
    this.directParentType = ParentType.INDEX;
    visitChildren(stat);
    this.directParentType = oldParent;
    this.rootComponent = oldComponent;
    this.indexType = null;
  }

  private void recordStatAndExplore(final DefaultMemoryStatistic stat) {
    this.isVersionColumn =
        MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN.equals(stat.getName());
    visitChildren(stat);
    this.isVersionColumn = false;
  }

  private boolean readEpochAndBranchIfAny(final IMemoryStatistic stat) {
    boolean epochOrBranchChanged = false;

    final IStatisticAttribute epochAttr =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_EPOCH);
    if (epochAttr != null) {
      this.epochId = epochAttr.asLong();
      epochOrBranchChanged = true;
    }
    final IStatisticAttribute branchAttr =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_BRANCH);
    if (branchAttr != null) {
      this.branch = branchAttr.asText();
      epochOrBranchChanged = true;
    }

    return epochOrBranchChanged;
  }

  private void readFieldsIfAny(final IMemoryStatistic stat) {
    IStatisticAttribute fieldAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
    if (fieldAttr != null) {
      this.fields = Collections.singleton(fieldAttr.asText());
    } else {
      IStatisticAttribute multipleFieldsAttr =
          stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELDS);
      if (multipleFieldsAttr != null) {
        this.fields = List.of(multipleFieldsAttr.asStringArray());
      }
    }
  }

  /**
   * Build an object array that represents a reference.
   *
   * @param referenceStatistic the {@link ReferenceStatistic} from which the array must be built.
   * @return the object array.
   */
  protected Object[] buildPartOfReferenceStatisticTuple(
      final ReferenceStatistic referenceStatistic) {
    final var refStoreFormat = getReferenceFormat(this.storageMetadata);

    final Object[] tuple = new Object[refStoreFormat.getFields().size()];
    tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_FROM_STORE)] =
        referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_FROM_STORE).asText();
    tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID)] =
        referenceStatistic
            .getAttribute(DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID)
            .asInt();
    tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_TO_STORE)] =
        referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_TO_STORE).asText();
    tuple[refStoreFormat.getFieldIndex(DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID)] =
        referenceStatistic.getAttribute(DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID).asInt();

    return tuple;
  }
}
