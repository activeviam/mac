/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import static com.activeviam.mac.statistic.memory.visitor.impl.DebugVisitor.DEBUG;

import com.activeviam.activepivot.core.impl.internal.impl.ActivePivotManager;
import com.activeviam.activepivot.core.intf.api.cube.metadata.HierarchyIdentifier;
import com.activeviam.activepivot.core.intf.api.cube.metadata.LevelIdentifier;
import com.activeviam.activepivot.dist.impl.api.cube.IMultiVersionDistributedActivePivot;
import com.activeviam.database.api.schema.IDataTable;
import com.activeviam.database.api.schema.IDatabaseSchema;
import com.activeviam.database.datastore.api.transaction.IOpenedTransaction;
import com.activeviam.mac.entities.CubeOwner;
import com.activeviam.mac.entities.DistributedCubeOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.UsedByVersion;
import com.activeviam.tech.core.api.exceptions.ActiveViamRuntimeException;
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
import java.time.Instant;
import java.util.Objects;

/**
 * Implementation of {@link IMemoryStatisticVisitor} for pivot statistics.
 *
 * @author ActiveViam
 */
public class PivotFeederVisitor extends AFeedVisitorWithDictionary<Void> {

  /** The export date, found on the first statistics we read. */
  protected Instant current = null;

  /** The epoch id we are currently reading statistics for. */
  protected Long epochId = null;

  /** The branch of the pivot we're currently reading statistics. */
  protected String branch = null;

  /** Current {@link ActivePivotManager}. */
  protected String manager;

  /** Aggregate provider being currently visited. */
  protected Long providerId;

  /** Partition being currently visited. */
  protected Integer partition;

  /** Dimension being currently visited. */
  protected String dimension;

  /** Hierarchy being currently visited. */
  protected String hierarchy;

  /** Level being currently visited. */
  protected String level;

  /** Type of the aggregate Provider being currently visited. */
  protected ProviderComponentType providerComponentType;

  /** Type of the root structure. */
  protected ParentType rootComponent;

  /** Type of the direct parent structure. */
  protected ParentType directParentType;

  /** Id of the direct parent structure. */
  protected String directParentId;

  /** Whether or not to ignore the field attributes of the visited statistics. */
  protected boolean ignoreFieldSpecifications = false;

  /**
   * Constructor.
   *
   * @param storageMetadata datastore schema metadata
   * @param tm ongoing transaction
   * @param dumpName name of the current import
   */
  public PivotFeederVisitor(
      final IDatabaseSchema storageMetadata, final IOpenedTransaction tm, final String dumpName) {
    super(tm, storageMetadata, dumpName);
  }

  private static boolean isPivotDistributed(final IMemoryStatistic pivotStat) {
    return pivotStat.getChildren().stream()
        .filter(stat -> stat.getName().equals(MemoryStatisticConstants.STAT_NAME_PROVIDER))
        .anyMatch(
            stat -> {
              final IStatisticAttribute providerType =
                  stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_PROVIDER_TYPE);
              return providerType != null
                  && providerType.asText().equals(IMultiVersionDistributedActivePivot.PLUGIN_KEY);
            });
  }

  private static Object[] buildProviderTupleFrom(
      final IDataTable format, final IMemoryStatistic stat) {
    final Object[] tuple = new Object[format.getFields().size()];

    final IStatisticAttribute indexAttr =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_PROVIDER_NAME);
    if (indexAttr != null) {
      tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__INDEX)] = indexAttr.asText();
    }

    tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__PROVIDER_ID)] =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_PROVIDER_ID).asLong();
    tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__TYPE)] =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_PROVIDER_TYPE)
            .asText(); // JIT, BITMAP, LEAF
    tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__CATEGORY)] = getProviderCategory(stat);

    return tuple;
  }

  private static Object[] buildLevelTupleFrom(
      final IDataTable format, final IMemoryStatistic stat) {
    final Object[] tuple = new Object[format.getFields().size()];

    tuple[format.getFieldIndex(DatastoreConstants.LEVEL__ON_HEAP_SIZE)] = stat.getShallowOnHeap();
    tuple[format.getFieldIndex(DatastoreConstants.LEVEL__OFF_HEAP_SIZE)] = stat.getShallowOffHeap();

    return tuple;
  }

  private static String getProviderCategory(final IMemoryStatistic stat) {
    switch (stat.getName()) {
      case MemoryStatisticConstants.STAT_NAME_FULL_PROVIDER:
        return "Full";
      case MemoryStatisticConstants.STAT_NAME_PARTIAL_PROVIDER:
        return "Partial";
      case MemoryStatisticConstants.STAT_NAME_PROVIDER:
        return "Unique";
      default:
        throw new IllegalArgumentException("Unsupported provider type " + stat);
    }
  }

  /**
   * Initializes the visit of the statistics of an entire pivot.
   *
   * @param stat Entry point of the tree creation, should be a {@link
   *     MemoryStatisticConstants#STAT_NAME_MULTIVERSION_PIVOT} , a {@link
   *     MemoryStatisticConstants#STAT_NAME_PIVOT} or {@link
   *     MemoryStatisticConstants#STAT_NAME_MANAGER} named statistic
   */
  public void startFrom(final AMemoryStatistic stat) {
    if (this.current != null) {
      throw new IllegalArgumentException("Cannot reuse a feed instance");
    }
    final IStatisticAttribute dateAtt = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_DATE);
    this.current =
        Instant.ofEpochSecond(null != dateAtt ? dateAtt.asLong() : System.currentTimeMillis());
    readEpochAndBranchIfAny(stat);
    if (this.epochId == null && stat.getName().equals(MemoryStatisticConstants.STAT_NAME_MANAGER)) {
      // Look amongst the children to find the epoch
      findEpoch(stat);
    }

    FeedVisitor.includeApplicationInfoIfAny(this.transaction, this.current, this.dumpName, stat);

    try {
      stat.accept(this);
    } catch (final RuntimeException e) {
      if (Boolean.TRUE.equals(DEBUG)) {
        final StatisticTreePrinter printer = DebugVisitor.createDebugPrinter(stat);
        printer.print();
      }
      throw e;
    }
  }

  private void findEpoch(AMemoryStatistic stat) {
    for (final IMemoryStatistic child : stat.getChildren()) {
      readEpochAndBranchIfAny(child);
      if (this.epochId != null) {
        break;
      }
    }
  }

  @Override
  public Void visit(final DefaultMemoryStatistic stat) {
    switch (stat.getName()) {
      case MemoryStatisticConstants.STAT_NAME_MANAGER:
        processManager(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_MULTIVERSION_PIVOT:
        processMultiVersionPivot(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_PIVOT:
        processPivot(stat);
        break;
      // Unless said otherwise we assume all providers are partial,safer than the
      // other way
      case MemoryStatisticConstants.STAT_NAME_PROVIDER:
      case MemoryStatisticConstants.STAT_NAME_PARTIAL_PROVIDER:
      case MemoryStatisticConstants.STAT_NAME_FULL_PROVIDER:
        processProvider(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_PROVIDER_PARTITION:
        processPartition(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_HIERARCHY:
        processHierarchy(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_LEVEL:
        processLevel(stat);
        break;
      case MemoryStatisticConstants.STAT_NAME_CHUNK_ENTRY:
        processChunkObject(stat);
        break;
      default:
        final ProviderComponentType type = detectProviderComponent(stat);
        if (type != null) {
          processProviderComponent(stat, type);
        } else {
          visitChildren(stat);
        }
    }
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
            this.partition,
            null,
            null,
            this.providerId,
            this.epochId,
            UsedByVersion.UNKNOWN,
            this.ignoreFieldSpecifications)
        .visit(stat);
  }

  @Override
  public Void visit(final ChunkStatistic stat) {

    final var format = getChunkFormat(this.storageMetadata);
    final Object[] tuple = FeedVisitor.buildChunkTupleFrom(format, stat);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.VERSION__EPOCH_ID, this.epochId);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.OWNER__OWNER, this.owner);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.OWNER__COMPONENT, this.rootComponent);

    final IStatisticAttribute fieldAttribute =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELD);
    if (fieldAttribute != null) {
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.OWNER__FIELD, fieldAttribute.asText());
    }

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, this.directParentType);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__PARENT_ID, this.directParentId);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.CHUNK__PROVIDER_ID, this.providerId);

    tuple[format.getFieldIndex(DatastoreConstants.CHUNK__PARTITION_ID)] = this.partition;

    final Long dictionaryId = this.dictionaryAttributes.getDictionaryId();
    if (dictionaryId != null) {
      FeedVisitor.setTupleElement(
          tuple, format, DatastoreConstants.CHUNK__PARENT_DICO_ID, dictionaryId);
    }

    this.transaction.add(DatastoreConstants.CHUNK_STORE, tuple);

    visitChildren(stat);

    return null;
  }

  @Override
  public Void visit(final DictionaryStatistic stat) {
    final ProviderComponentType cpnType = detectProviderComponent(stat);
    final boolean previousIgnoreFieldSpecifications = this.ignoreFieldSpecifications;
    if (cpnType != null) {
      assert this.rootComponent == null;
      this.rootComponent = getCorrespondingParentType(cpnType);

      if (cpnType == ProviderComponentType.BITMAP_MATCHER
          || cpnType == ProviderComponentType.POINT_INDEX
          || cpnType == ProviderComponentType.POINT_MAPPING) {
        this.ignoreFieldSpecifications = true;
      }
    }

    final var previousDictionaryAttributes = processDictionaryStatistic(stat, this.epochId);
    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentType = cpnType != null ? this.rootComponent : ParentType.DICTIONARY;
    this.directParentId = String.valueOf(this.dictionaryAttributes.getDictionaryId());

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

    this.dictionaryAttributes = previousDictionaryAttributes;
    this.directParentType = previousParentType;
    this.directParentId = previousParentId;
    this.ignoreFieldSpecifications = previousIgnoreFieldSpecifications;

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
        "An ActivePivot cannot contain indices. Received: " + stat);
  }

  private void processManager(final AMemoryStatistic stat) {
    final IStatisticAttribute idAttr =
        Objects.requireNonNull(
            stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_MANAGER_ID),
            () -> "No manager id in " + stat);
    this.manager = idAttr.asText();

    visitChildren(stat);

    this.manager = null;
  }

  private void processMultiVersionPivot(final AMemoryStatistic stat) {
    final IStatisticAttribute managerAttr =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_MANAGER_ID);
    if (managerAttr != null) {
      assert this.manager == null;
      this.manager = managerAttr.asText();
    }

    visitChildren(stat);
    if (managerAttr != null) {
      this.manager = null;
    }
  }

  private void processPivot(final AMemoryStatistic stat) {
    readEpochAndBranchIfAny(stat);

    if (readEpochAndBranchIfAny(stat)) {
      final var versionStoreFormat = getVersionStoreFormat(this.storageMetadata);
      final Object[] tuple =
          FeedVisitor.buildVersionTupleFrom(
              versionStoreFormat, stat, this.dumpName, this.epochId, this.branch);
      FeedVisitor.add(stat, this.transaction, DatastoreConstants.VERSION_STORE, tuple);
    }

    final IStatisticAttribute idAttr =
        Objects.requireNonNull(
            stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_PIVOT_ID),
            () -> "No pivot id in " + stat);
    final String pivotId = idAttr.asText();
    if (isPivotDistributed(stat)) {
      this.owner = new DistributedCubeOwner(pivotId);
    } else {
      this.owner = new CubeOwner(pivotId);
    }

    final IStatisticAttribute managerAttr =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_MANAGER_ID);
    if (managerAttr != null) {
      assert this.manager == null;
      this.manager = managerAttr.asText();
    }

    visitChildren(stat);

    this.owner = null;
    if (managerAttr != null) {
      this.manager = null;
    }
    this.epochId = null;
    this.branch = null;
  }

  private void processProvider(final AMemoryStatistic stat) {
    final var format = getProviderFormat(this.storageMetadata);
    final Object[] tuple = buildProviderTupleFrom(format, stat);

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.PROVIDER__PIVOT_ID, this.owner.getName());
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.APPLICATION__DUMP_NAME, this.dumpName);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.PROVIDER__MANAGER_ID, this.manager);

    this.transaction.add(DatastoreConstants.PROVIDER_STORE, tuple);

    this.providerId = (Long) tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__PROVIDER_ID)];
    visitChildren(stat);
    this.providerId = null;
  }

  private void processPartition(final AMemoryStatistic stat) {
    final IStatisticAttribute idAttr =
        Objects.requireNonNull(
            stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_PROVIDER_PARTITION_ID),
            () -> "No partition id in " + stat);
    assert this.partition == null;
    this.partition = idAttr.asInt();

    visitChildren(stat);

    this.partition = null;
  }

  private void processHierarchy(final AMemoryStatistic stat) {
    String hierarchyDescription =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_HIERARCHY_ID).asText();
    HierarchyIdentifier hc = HierarchyIdentifier.fromDescription(hierarchyDescription);
    this.dimension = hc.getDimensionName();
    this.hierarchy = hc.getHierarchyName();

    visitChildren(stat);

    this.hierarchy = null;
  }

  private void processLevel(final AMemoryStatistic stat) {
    final var format = getLevelFormat(this.storageMetadata);
    final Object[] tuple = buildLevelTupleFrom(format, stat);

    String levelDescription =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_LEVEL_ID).asText();
    LevelIdentifier lc = LevelIdentifier.fromDescription(levelDescription);
    this.level = lc.getLevelName();

    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__MANAGER_ID, this.manager);
    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.LEVEL__PIVOT_ID, this.owner.getName());
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__DIMENSION, this.dimension);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__HIERARCHY, this.hierarchy);
    FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__LEVEL, this.level);

    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentId = levelDescription;
    this.directParentType = ParentType.LEVEL;
    this.rootComponent = ParentType.LEVEL;

    final LevelStatisticVisitor levelVisitor =
        new LevelStatisticVisitor(
            this, this.transaction, this.storageMetadata, this.dumpName, this.epochId);
    levelVisitor.analyze(stat);

    this.directParentType = previousParentType;
    this.directParentId = previousParentId;
    this.rootComponent = null;

    FeedVisitor.setTupleElement(
        tuple, format, DatastoreConstants.LEVEL__MEMBER_COUNT, levelVisitor.memberCount);

    this.transaction.add(DatastoreConstants.LEVEL_STORE, tuple);

    this.dictionaryAttributes = DictionaryAttributes.NONE;
    this.level = null;
  }

  private void processProviderComponent(
      final AMemoryStatistic stat, final ProviderComponentType type) {
    final ParentType previousParentType = this.directParentType;
    final String previousParentId = this.directParentId;
    this.directParentType = getCorrespondingParentType(type);
    this.directParentId =
        this.owner.getName() + "-" + this.providerId + "-" + type + "-" + this.partition;
    this.rootComponent = this.directParentType;

    visitChildren(stat);

    this.directParentType = previousParentType;
    this.directParentId = previousParentId;
    this.rootComponent = null;
    this.providerComponentType = null;
  }

  private void processChunkObject(final AMemoryStatistic statistic) {
    if (VectorStatisticVisitor.isVector(statistic)) {
      final VectorStatisticVisitor subVisitor =
          new VectorStatisticVisitor(
              this.storageMetadata,
              this.transaction,
              this.dumpName,
              this.current,
              this.owner,
              null,
              this.partition,
              this.epochId,
              UsedByVersion.UNKNOWN);
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
  protected ProviderComponentType detectProviderComponent(final IMemoryStatistic stat) {
    switch (stat.getName()) {
      case MemoryStatisticConstants.STAT_NAME_POINT_INDEX:
        return ProviderComponentType.POINT_INDEX;
      case MemoryStatisticConstants.STAT_NAME_POINT_MAPPING:
        return ProviderComponentType.POINT_MAPPING;
      case MemoryStatisticConstants.STAT_NAME_AGGREGATE_STORE:
        return ProviderComponentType.AGGREGATE_STORE;
      case MemoryStatisticConstants.STAT_NAME_BITMAP_MATCHER:
        return ProviderComponentType.BITMAP_MATCHER;
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
  protected ParentType getCorrespondingParentType(final ProviderComponentType type) {
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

  private boolean readEpochAndBranchIfAny(final IMemoryStatistic stat) {
    boolean epochOrBranchChanged = false;

    final IStatisticAttribute epochAttr =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_EPOCH);
    if (epochAttr != null) {
      final Long epoch = epochAttr.asLong();
      assert this.epochId == null || epoch.equals(this.epochId);
      this.epochId = epoch;
      epochOrBranchChanged = true;
    }
    final IStatisticAttribute branchAttr =
        stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_BRANCH);
    if (branchAttr != null) {
      final String branchAttrText = branchAttr.asText();
      assert this.branch == null || this.branch.equals(branchAttrText);
      this.branch = branchAttrText;
      epochOrBranchChanged = true;
    }

    return epochOrBranchChanged;
  }
}
