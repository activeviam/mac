/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.copper.HierarchyIdentifier;
import com.activeviam.copper.LevelIdentifier;
import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.Loggers;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.CubeOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.PivotMemoryStatisticConstants;
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
import com.quartetfs.biz.pivot.impl.ActivePivotManager;
import com.quartetfs.fwk.QuartetRuntimeException;
import java.time.Instant;
import java.util.Objects;
import java.util.logging.Logger;

/**
 * Implementation of {@link IMemoryStatisticVisitor} for pivot statistics.
 *
 * @author ActiveViam
 */
public class PivotFeederVisitor extends AFeedVisitor<Void> {

  /** Class logger. */
  @SuppressWarnings("unused")
  private static final Logger logger = Logger.getLogger(Loggers.ACTIVEPIVOT_LOADING);

  /** The export date, found on the first statistics we read. */
  protected Instant current = null;
  /** The epoch id we are currently reading statistics for. */
  protected Long epochId = null;
  /** The branch of the pivot we're currently reading statistics. */
  protected String branch = null;
  /** current {@link ActivePivotManager}. */
  protected String manager;
  /** currently visited Pivot. */
  protected String pivot;
  /** Aggregate provider being currently visited. */
  protected Long providerId;
  /** Partition being currently visited. */
  protected Integer partition;
  /** Dictionary being currently visited. */
  protected Long dictionaryId;
  /** ChunkSet being visited. */
  protected Long chunkSetId;
  /** dimension being currently visited. */
  protected String dimension;
  /** hierarchy being currently visited. */
  protected String hierarchy;
  /** level being currently visited. */
  protected String level;
  /** Type of the aggregate Provider being currently visited. */
  protected ProviderCpnType providerCpnType;
  /** Type of the root structure. */
  protected ParentType rootComponent;
  /** Type of the direct parent structure. */
  protected ParentType directParentType;
  /** id of the direct parent structure. */
  protected String directParentId;

  /** Tree Printer. */
  protected StatisticTreePrinter printer;

  /**
   * Constructor.
   *
   * @param storageMetadata datastore schema metadata
   * @param tm ongoing transaction
   * @param dumpName name of the current import
   */
  public PivotFeederVisitor(
      final IDatastoreSchemaMetadata storageMetadata,
      final IOpenedTransaction tm,
      final String dumpName) {
    super(tm, storageMetadata, dumpName);
  }

  /**
   * Initializes the visit of the statistics of an entire pivot.
   *
   * @param stat Entry point of the tree creation, should be a {@link
   *     PivotMemoryStatisticConstants#STAT_NAME_MULTIVERSION_PIVOT} , a {@link
   *     PivotMemoryStatisticConstants#STAT_NAME_PIVOT} or {@link
   *     PivotMemoryStatisticConstants#STAT_NAME_MANAGER} named statistic
   */
  public void startFrom(final IMemoryStatistic stat) {
    this.printer = DebugVisitor.createDebugPrinter(stat);
    if (this.current == null) {
      final IStatisticAttribute dateAtt =
          stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_DATE);

      this.current =
          Instant.ofEpochSecond(null != dateAtt ? dateAtt.asLong() : System.currentTimeMillis());

      readEpochAndBranchIfAny(stat);
      if (this.epochId == null
          && stat.getName().equals(PivotMemoryStatisticConstants.STAT_NAME_MANAGER)) {
        // Look amongst the children to find the epoch
        for (final IMemoryStatistic child : stat.getChildren()) {
          readEpochAndBranchIfAny(child);
          if (this.epochId != null) {
            break;
          }
        }
      }

      FeedVisitor.includeApplicationInfoIfAny(this.transaction, this.current, this.dumpName, stat);

      try {
        stat.accept(this);
      } catch (RuntimeException e) {
        this.printer.print();
        throw e;
      }
    } else {
      throw new RuntimeException("Cannot reuse a feed instance");
    }
  }

  @Override
  public Void visit(final DefaultMemoryStatistic stat) {
    switch (stat.getName()) {
      case PivotMemoryStatisticConstants.STAT_NAME_MANAGER:
        processManager(stat);
        break;
      case PivotMemoryStatisticConstants.STAT_NAME_MULTIVERSION_PIVOT:
        processMultiVersionPivot(stat);
        break;
      case PivotMemoryStatisticConstants.STAT_NAME_PIVOT:
        processPivot(stat);
        break;
        // Unless said otherwise we assume all providers are partial,safer than the
        // other way
      case PivotMemoryStatisticConstants.STAT_NAME_PROVIDER:
      case PivotMemoryStatisticConstants.STAT_NAME_PARTIAL_PROVIDER:
        processProvider(stat);
        break;
      case PivotMemoryStatisticConstants.STAT_NAME_FULL_PROVIDER:
        processFullProvider(stat);
        break;
      case PivotMemoryStatisticConstants.STAT_NAME_PROVIDER_PARTITION:
        processPartition(stat);
        break;
      case PivotMemoryStatisticConstants.STAT_NAME_HIERARCHY:
        processHierarchy(stat);
        break;
      case PivotMemoryStatisticConstants.STAT_NAME_LEVEL:
        processLevel(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_CHUNK_ENTRY:
        processChunkObject(stat);
        break;
      default:
        final ProviderCpnType type = detectProviderComponent(stat);
        if (type != null) {
          processProviderComponent(stat, type);
        } else {
          visitChildren(stat);
        }
    }
    return null;
  }

  @Override
  public Void visit(final ChunkSetStatistic stat) {
    this.chunkSetId = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_CHUNKSET_ID).asLong();
    visitChildren(stat);
    this.chunkSetId = null;

    return null;
  }

  @Override
  public Void visit(final ChunkStatistic stat) {

    final ChunkOwner owner = new CubeOwner(this.pivot);

    final IRecordFormat ownerFormat = AFeedVisitor.getOwnerFormat(this.storageMetadata);
    final Object[] ownerTuple =
        FeedVisitor.buildOwnerTupleFrom(ownerFormat, stat, owner, this.dumpName);
    FeedVisitor.add(stat, transaction, DatastoreConstants.CHUNK_TO_OWNER_STORE, ownerTuple);

    final IRecordFormat componentFormat = AFeedVisitor.getComponentFormat(this.storageMetadata);
    final Object[] componentTuple =
        FeedVisitor.buildComponentTupleFrom(
            componentFormat, stat, this.rootComponent, this.dumpName);
    FeedVisitor.add(stat, transaction, DatastoreConstants.CHUNK_TO_COMPONENT_STORE, componentTuple);

    final IRecordFormat format = getChunkFormat(this.storageMetadata);
    final Object[] tuple = FeedVisitor.buildChunkTupleFrom(format, stat);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.VERSION__BRANCH, this.branch);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.VERSION__EPOCH_ID, this.epochId);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, this.directParentType);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__PARENT_ID, this.directParentId);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__PROVIDER_ID, this.providerId);
    FeedVisitor.setTupleElement(
        tuple,
        format,
        DatastoreConstants.CHUNK__PROVIDER_COMPONENT_TYPE,
        this.providerCpnType.toString());

    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__OWNER, owner);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__COMPONENT, this.rootComponent);
    tuple[format.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID)] = this.partition;

    if (this.dictionaryId != null) {
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__PARENT_DICO_ID, this.dictionaryId);
    }

    this.transaction.add(DatastoreConstants.CHUNK_STORE, tuple);

    visitChildren(stat);

    return null;
  }

  @Override
  public Void visit(final DictionaryStatistic stat) {
    final ProviderCpnType cpnType = detectProviderComponent(stat);
    if (cpnType != null) {
      assert this.providerCpnType == null;
      assert this.rootComponent == null;
      this.providerCpnType = cpnType;
      this.rootComponent = getCorrespondingParentType(cpnType);

      final IRecordFormat cpnFormat = getProviderCpnFormat();
      final Object[] cpnTuple = buildProviderComponentTupleFrom(cpnFormat, stat);
      FeedVisitor.setTupleElement(
          cpnTuple, cpnFormat, DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID, this.providerId);
      FeedVisitor.setTupleElement(
          cpnTuple,
          cpnFormat,
          DatastoreConstants.PROVIDER_COMPONENT__TYPE,
          this.providerCpnType.toString());

      this.transaction.add(DatastoreConstants.PROVIDER_COMPONENT_STORE, cpnTuple);
    }

    final IRecordFormat format = getDictionaryFormat(this.storageMetadata);
    final Object[] tuple = FeedVisitor.buildDictionaryTupleFrom(format, stat);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.APPLICATION__DUMP_NAME, this.dumpName);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.VERSION__BRANCH, this.branch);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.VERSION__EPOCH_ID, this.epochId);


    this.dictionaryId = (Long) tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)];
    this.transaction.add(DatastoreConstants.DICTIONARY_STORE, tuple);

    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentType = cpnType != null ? this.rootComponent : ParentType.DICTIONARY;
    this.directParentId = String.valueOf(this.dictionaryId);

    if (this.providerId != null) {
      visitChildren(stat);
    } else if (this.level != null) {
      // We are processing a hierarchy/level
      visitChildren(stat);
      // Do not nullify dictionaryId. It is done after visiting the whole level
    } else if (this.hierarchy != null) {
      // For distributed hierarchy, it is possible to have standalone dictionaries
      visitChildren(stat);
    } else {
      throw new ActiveViamRuntimeException("Unexpected stat on dictionary: " + stat);
    }

    this.dictionaryId = null;
    this.directParentType = previousParentType;
    this.directParentId = previousParentId;

    return null;
  }

  @Override
  public Void visit(final ReferenceStatistic stat) {
    throw new UnsupportedOperationException(
        "An ActivePivot cannot contain references. Received: " + stat);
  }

  @Override
  public Void visit(IndexStatistic stat) {
    throw new UnsupportedOperationException(
        "An ActivePivot cannot contain references. Received: " + stat);
  }

  private void processManager(final IMemoryStatistic stat) {
    final IStatisticAttribute idAttr =
        Objects.requireNonNull(
            stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_MANAGER_ID),
            () -> "No manager id in " + stat);
    this.manager = idAttr.asText();

    visitChildren(stat);

    this.manager = null;
  }

  private void processMultiVersionPivot(final IMemoryStatistic stat) {
    final IStatisticAttribute managerAttr =
        stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_MANAGER_ID);
    if (managerAttr != null) {
      assert this.manager == null;
      this.manager = managerAttr.asText();
    }

    visitChildren(stat);
    if (managerAttr != null) {
      this.manager = null;
    }
  }

  private void processPivot(final IMemoryStatistic stat) {

    readEpochAndBranchIfAny(stat);

    final IStatisticAttribute idAttr =
        Objects.requireNonNull(
            stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PIVOT_ID),
            () -> "No pivot id in " + stat);
    this.pivot = idAttr.asText();
    final IStatisticAttribute managerAttr =
        stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_MANAGER_ID);
    if (managerAttr != null) {
      assert this.manager == null;
      this.manager = managerAttr.asText();
    }

    visitChildren(stat);

    this.pivot = null;
    if (managerAttr != null) {
      this.manager = null;
    }
    this.epochId = null;
    this.branch = null;
  }

  private void processFullProvider(final IMemoryStatistic stat) {
    // Ignore this provider, only consider its underlying providers
    visitChildren(stat);
  }

  private void processProvider(final IMemoryStatistic stat) {
    final IRecordFormat format = getProviderFormat(this.storageMetadata);
    final Object[] tuple = buildProviderTupleFrom(format, stat);

    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.PROVIDER__PIVOT_ID, this.pivot);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.PROVIDER__MANAGER_ID, this.manager);

    this.transaction.add(DatastoreConstants.PROVIDER_STORE, tuple);

    this.providerId = (Long) tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__PROVIDER_ID)];
    visitChildren(stat);
    this.providerId = null;
  }

  private void processPartition(final IMemoryStatistic stat) {
    final IStatisticAttribute idAttr =
        Objects.requireNonNull(
            stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_PARTITION_ID),
            () -> "No partition id in " + stat);
    assert this.partition == null;
    this.partition = idAttr.asInt();

    visitChildren(stat);

    this.partition = null;
  }

  private void processHierarchy(final IMemoryStatistic stat) {
    String hierarchyDescription =
        stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_HIERARCHY_ID).asText();
    HierarchyIdentifier hc = HierarchyIdentifier.fromDescription(hierarchyDescription);
    this.dimension = hc.dimension;
    this.hierarchy = hc.hierarchy;

    visitChildren(stat);

    this.hierarchy = null;
  }

  private void processLevel(final IMemoryStatistic stat) {
    final IRecordFormat format = getLevelFormat(this.storageMetadata);
    final Object[] tuple = buildLevelTupleFrom(format, stat);

    String levelDescription =
        stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_LEVEL_ID).asText();
    LevelIdentifier lc = LevelIdentifier.fromDescription(levelDescription);
    this.level = lc.level;

    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__MANAGER_ID, this.manager);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__PIVOT_ID, this.pivot);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__DIMENSION, this.dimension);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__HIERARCHY, this.hierarchy);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__LEVEL, this.level);

    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentId = levelDescription;
    this.directParentType = ParentType.LEVEL;
    this.rootComponent = ParentType.LEVEL;

    final LevelStatisticVisitor levelVisitor =
        new LevelStatisticVisitor(this, this.transaction, this.storageMetadata, this.dumpName,
            this.epochId, this.branch);
    levelVisitor.analyse(stat);

    this.directParentType = previousParentType;
    this.directParentId = previousParentId;
    this.rootComponent = null;

    // Maybe instead explicitly visit specific children: Dico, Members
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.LEVEL__MEMBER_COUNT, levelVisitor.memberCount);
    //		if (!this.hierarchy.equals(IMeasureHierarchy.MEASURE_HIERARCHY) &&
    // !this.level.equals(ILevelInfo.ClassificationType.ALL.name())) {
    //			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__DICTIONARY_ID,
    // levelVisitor.dictionaryId);
    //		}

    this.transaction.add(DatastoreConstants.LEVEL_STORE, tuple);

    this.dictionaryId = null;
    this.level = null;
  }

  private void processProviderComponent(final IMemoryStatistic stat, final ProviderCpnType type) {
    this.providerCpnType = Objects.requireNonNull(type, "Null provider type");
    final IRecordFormat format = getProviderCpnFormat();
    final Object[] tuple = buildProviderComponentTupleFrom(format, stat);

    FeedVisitor.setTupleElement(
        tuple,
        format,
        DatastoreConstants.PROVIDER_COMPONENT__TYPE,
        this.providerCpnType.toString());
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID, this.providerId);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);

    FeedVisitor.checkTuple(tuple, format);
    this.transaction.add(DatastoreConstants.PROVIDER_COMPONENT_STORE, tuple);

    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentType = getCorrespondingParentType(type);
    this.directParentId = this.pivot + "-" + this.providerId + "-" + type + "-" + this.partition;
    this.rootComponent = this.directParentType;

    visitChildren(stat);

    this.directParentType = previousParentType;
    this.directParentId = previousParentId;
    this.rootComponent = null;
    this.providerCpnType = null;
  }

  private void processChunkObject(final IMemoryStatistic statistic) {
    if (VectorStatisticVisitor.isVector(statistic)) {
      final VectorStatisticVisitor subVisitor =
          new VectorStatisticVisitor(
              this.storageMetadata,
              this.transaction,
              this.dumpName,
              this.current,
              this.pivot,
              null,
              this.partition,
              this.epochId,
              this.branch);
      subVisitor.process(statistic);
    } else {
      FeedVisitor.visitChildren(this, statistic);
    }
  }

  /**
   * Returns the provider component that corresponds to the given statistic or null.
   *
   * @param stat the statistic
   * @return the provider component
   */
  protected ProviderCpnType detectProviderComponent(final IMemoryStatistic stat) {
    switch (stat.getName()) {
      case PivotMemoryStatisticConstants.STAT_NAME_POINT_INDEX:
        return ProviderCpnType.POINT_INDEX;
      case PivotMemoryStatisticConstants.STAT_NAME_POINT_MAPPING:
        return ProviderCpnType.POINT_MAPPING;
      case PivotMemoryStatisticConstants.STAT_NAME_AGGREGATE_STORE:
        return ProviderCpnType.AGGREGATE_STORE;
      case PivotMemoryStatisticConstants.STAT_NAME_BITMAP_MATCHER:
        return ProviderCpnType.BITMAP_MATCHER;
      default:
        return null;
    }
  }

  /**
   * Returns the generic ParentType for a given Aggregate Provider component type.
   *
   * @param type Aggregate Provider component type
   * @return the corresponding generic {@link ParentType}
   */
  protected ParentType getCorrespondingParentType(final ProviderCpnType type) {
    switch (type) {
      case POINT_INDEX:
        return ParentType.POINT_INDEX;
      case POINT_MAPPING:
        return ParentType.POINT_MAPPING;
      case AGGREGATE_STORE:
        return ParentType.AGGREGATE_STORE;
      case BITMAP_MATCHER:
        return ParentType.BITMAP_MATCHER;
      default:
        throw new IllegalStateException("Incomplete switch case. Got: " + type);
    }
  }

  private void readEpochAndBranchIfAny(final IMemoryStatistic stat) {
    final IStatisticAttribute epochAttr =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_EPOCH);
    if (epochAttr != null) {
      final Long epoch = epochAttr.asLong();
      assert this.epochId == null || epoch.equals(this.epochId);
      this.epochId = epoch;
    }
    final IStatisticAttribute branchAttr =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_BRANCH);
    if (branchAttr != null) {
      final String branch = branchAttr.asText();
      assert this.branch == null || this.branch.equals(branch);
      this.branch = branch;
    }
  }

  private static Object[] buildProviderTupleFrom(
      final IRecordFormat format, final IMemoryStatistic stat) {
    final Object[] tuple = new Object[format.getFieldCount()];

    final IStatisticAttribute indexAttr =
        stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_INDEX);
    if (indexAttr != null) {
      tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__INDEX)] = indexAttr.asText();
    }

    tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__PROVIDER_ID)] =
        stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_ID).asLong();
    tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__TYPE)] =
        stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_TYPE)
            .asText(); // JIT, BITMAP, LEAF
    tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__CATEGORY)] = getProviderCategory(stat);

    return tuple;
  }

  private static Object[] buildProviderComponentTupleFrom(
      final IRecordFormat format, final IMemoryStatistic stat) {
    final Object[] tuple = new Object[format.getFieldCount()];

    tuple[format.getFieldIndex(DatastoreConstants.PROVIDER_COMPONENT__CLASS)] =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS).asText();

    return tuple;
  }

  private static Object[] buildLevelTupleFrom(
      final IRecordFormat format, final IMemoryStatistic stat) {
    final Object[] tuple = new Object[format.getFieldCount()];

    tuple[format.getFieldIndex(DatastoreConstants.LEVEL__ON_HEAP_SIZE)] = stat.getShallowOnHeap();
    tuple[format.getFieldIndex(DatastoreConstants.LEVEL__OFF_HEAP_SIZE)] = stat.getShallowOffHeap();

    return tuple;
  }

  private static String getProviderCategory(final IMemoryStatistic stat) {
    switch (stat.getName()) {
      case PivotMemoryStatisticConstants.STAT_NAME_FULL_PROVIDER:
        return "Full";
      case PivotMemoryStatisticConstants.STAT_NAME_PARTIAL_PROVIDER:
        return "Partial";
      case PivotMemoryStatisticConstants.STAT_NAME_PROVIDER:
        return "Unique";
      default:
        throw new IllegalArgumentException("Unsupported provider type " + stat);
    }
  }
}
