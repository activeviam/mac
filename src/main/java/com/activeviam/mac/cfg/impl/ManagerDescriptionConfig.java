/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.builders.StartBuilding;
import com.activeviam.comparators.EpochViewComparator;
import com.activeviam.copper.ICopperContext;
import com.activeviam.copper.api.Copper;
import com.activeviam.copper.api.CopperMeasure;
import com.activeviam.copper.api.CopperMeasureToAggregateAbove;
import com.activeviam.copper.api.CopperStore;
import com.activeviam.desc.build.ICanBuildCubeDescription;
import com.activeviam.desc.build.ICanStartBuildingMeasures;
import com.activeviam.desc.build.IHasAtLeastOneMeasure;
import com.activeviam.desc.build.ISelectionDescriptionBuilder;
import com.activeviam.desc.build.dimensions.ICanStartBuildingDimensions;
import com.activeviam.formatter.ByteFormatter;
import com.activeviam.formatter.ClassFormatter;
import com.activeviam.formatter.PartitionIdFormatter;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.qfs.agg.impl.SingleValueFunction;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.qfs.pivot.util.impl.MdxNamingUtil;
import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.quartetfs.biz.pivot.context.impl.QueriesTimeLimit;
import com.quartetfs.biz.pivot.cube.dimension.IDimension;
import com.quartetfs.biz.pivot.cube.hierarchy.IHierarchy;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.ICalculatedMemberDescription;
import com.quartetfs.biz.pivot.definitions.ISelectionDescription;
import com.quartetfs.biz.pivot.definitions.impl.CalculatedMemberDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import com.quartetfs.fwk.format.impl.DateFormatter;
import com.quartetfs.fwk.format.impl.NumberFormatter;
import com.quartetfs.fwk.ordering.impl.ReverseOrderComparator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Manager Description Config that defines the manager description which contains the cube
 * dimensions and every CopperMeasure.
 *
 * @author ActiveViam
 */
@Configuration
public class ManagerDescriptionConfig implements IActivePivotManagerDescriptionConfig {

  /**
   * The main monitoring cube.
   *
   * <p>This Cube is based from Chunk facts
   */
  public static final String MONITORING_CUBE = "MemoryCube";

  /** Measure of the summed off-heap memory footprint. */
  public static final String DIRECT_MEMORY_SUM = "DirectMemory.SUM";

  public static final String DIRECT_MEMORY_RATIO = "DirectMemory.Ratio";

  public static final String CHUNK_COUNT = "Chunks.COUNT";

  /**
   * Measure of the summed on-heap memory footprint.
   *
   * <p>This measure only sums the on-heap memory held by chunk, and therefore do not contains the
   * entire on-heap footprint of the entire ActivePivot Application
   */
  public static final String HEAP_MEMORY_SUM = "HeapMemory.SUM";

  /** Java class of the chunk. */
  public static final String CHUNK_CLASS_LEVEL = "Class";
  /** Type of the structure owning the chunk. */
  public static final String CHUNK_TYPE_LEVEL = "Type";

  /** Name of the chunk dump level. */
  public static final String CHUNK_DUMP_NAME_LEVEL = "Import info";

  /** Type of the structure owning the chunk. */
  public static final String CHUNK_PARENT_ID_LEVEL = "ParentID";

  /** Level for the Ids of the dictionary. */
  public static final String CHUNK_DICO_ID_LEVEL = "DicoID";
  /** Level for the Ids of the references. */
  public static final String CHUNK_REF_ID_LEVEL = "ReferenceID";
  /** Level for the Ids of the indexes. */
  public static final String CHUNK_INDEX_ID_LEVEL = "IndexID";

  /** Name of the Chunk Hierarchy. */
  public static final String CHUNK_DIMENSION = "Chunks";

  /** Name of the component dimension. */
  public static final String COMPONENT_DIMENSION = "Components";
  /** Name of the component dimension. */
  public static final String OWNER_DIMENSION = "Owners";
  /** Name of the field dimension. */
  public static final String FIELD_DIMENSION = "Fields";
  /** Name of the index dimension. */
  public static final String INDEX_DIMENSION = "Indices";
  /** Name of the version dimension. */
  public static final String VERSION_DIMENSION = "Versions";
  public static final String AGGREGATE_PROVIDER_DIMENSION = "Aggregate Provider";
  public static final String PARTITION_DIMENSION = "Partitions";
  public static final String USED_BY_VERSION_DIMENSION = "Used by Version";

  /** Total on-heap memory footprint of the application. */
  public static final String USED_HEAP = "UsedHeapMemory";
  /** Total on-heap memory committed by the JVM. */
  public static final String COMMITTED_HEAP = "CommittedHeapMemory";
  /** Total off-heap memory footprint of the application. */
  public static final String USED_DIRECT = "UsedDirectMemory";
  /** Total off-heap memory committed by the JVM. */
  public static final String MAX_DIRECT = "MaxDirectMemory";

  public static final String USED_MEMORY_RATIO = "UsedMemory.Ratio";
  public static final String MAX_MEMORY_RATIO = "MaxMemory.Ratio";

  public static final String DICTIONARY_SIZE = "Dictionary Size";

  public static final String VECTOR_BLOCK_REFCOUNT = "VectorBlock.RefCount";
  public static final String VECTOR_BLOCK_SIZE = "VectorBlock.Length";

  /** Measure of the size of Chunks (in Bytes). */
  public static final String CHUNK_SIZE_SUM = "ChunkSize.SUM";

  /** Measure of the the non written rows in Chunks. */
  public static final String NON_WRITTEN_ROWS_COUNT = "NonWrittenRows.COUNT";

  public static final String NON_WRITTEN_ROWS_RATIO = "NonWrittenRows.Ratio";

  /** Measure of the deleted rows in Chunks. */
  public static final String DELETED_ROWS_COUNT = "DeletedRows.COUNT";

  public static final String DELETED_ROWS_RATIO = "DeletedRows.Ratio";

  public static final String COMITTED_ROWS = "CommittedRows";
  public static final String COMITTED_CHUNK = "ComittedChunk";
  public static final String COMMITTED_MEMORY_RATIO = "CommittedMemory.Ratio";

  /** Formatter for Numbers. */
  public static final String NUMBER_FORMATTER = NumberFormatter.TYPE + "[#,###]";
  /** Formatter for Percentages. */
  public static final String PERCENT_FORMATTER = NumberFormatter.TYPE + "[#.##%]";

  /** The name of the hierarchy of indexed fields. */
  public static final String INDEXED_FIELDS_HIERARCHY = "Indexed Fields";
  /** The name of the hierarchy of indexed fields. */
  public static final String INDEX_TYPE_HIERARCHY = "Index Type";
  /** Name of the owner hierarchy. */
  public static final String OWNER_HIERARCHY = "Owner";
  /** Name of the component hierarchy. */
  public static final String COMPONENT_HIERARCHY = "Component";
  /** Name of the field hierarchy. */
  public static final String FIELD_HIERARCHY = "Field";
  /** The name of the hierarchy of reference names. */
  public static final String REFERENCE_NAMES_HIERARCHY = "Reference Names";
  /** The name of the hierarchy of provider ids. */
  public static final String PROVIDER_ID_HIERARCHY = "ProviderId";
  /** The name of the hierarchy of provider partitions. */
  public static final String PROVIDER_PARTITION_HIERARCHY = "ProviderPartition";
  /** The name of the hierarchy of provider types. */
  public static final String PROVIDER_TYPE_HIERARCHY = "ProviderType";
  /** The name of the hierarchy of pivots. */
  public static final String PIVOT_HIERARCHY = "Pivot";
  /** The name of the hierarchy of managers. */
  public static final String MANAGER_HIERARCHY = "Manager";
  /** The name of the hierarchy of owner components. */
  public static final String CHUNK_ID_HIERARCHY = "ChunkId";
  /** The name of the hierarchy of partitions. */
  public static final String PARTITION_HIERARCHY = "Partition";
  /** Name of the branch hierarchy. */
  public static final String BRANCH_HIERARCHY = "Branch";
  /** Name of the internal epoch id hierarchy. */
  public static final String INTERNAL_EPOCH_ID_HIERARCHY = "Internal Epoch Id";
  /** Name of the epoch id hierarchy. */
  public static final String EPOCH_ID_HIERARCHY = "Epoch Id";
  public static final String DATE_HIERARCHY = "Date";

  /** The name of the folder for measures related to chunk ownership. */
  public static final String OWNERSHIP_FOLDER = "Ownership";
  /** The name of the folder for measures related to memory metrics. */
  public static final String MEMORY_FOLDER = "Memory";

  private CopperStore chunkToDicoStore;

  @Bean
  @Override
  public IActivePivotManagerDescription userManagerDescription() {
    return ActivePivotManagerBuilder.postProcess(
        StartBuilding.managerDescription()
            .withSchema("MemorySchema")
            .withSelection(memorySelection())
            .withCube(memoryCube())
            .build(), userSchemaDescription());
  }

  @Override
  public IDatastoreSchemaDescription userSchemaDescription() {
    return new MemoryAnalysisDatastoreDescription();
  }

  /**
   * Prefixes a field by another string.
   *
   * @param prefix string to prepend
   * @param field field to be prefixed
   * @return the prefixed string
   */
  protected static String prefixField(String prefix, String field) {
    return prefix + field.substring(0, 1).toUpperCase() + field.substring(1);
  }

  private ISelectionDescription memorySelection() {
    return StartBuilding.selection(this.userSchemaDescription())
        .fromBaseStore(DatastoreConstants.CHUNK_STORE)
        .withAllReachableFields(
            allReachableFields -> {
              allReachableFields.remove(DatastoreConstants.CHUNK__CLASS);
              final Map<String, String> result =
                  ISelectionDescriptionBuilder.FieldsCollisionHandler.CLOSEST.handle(
                      allReachableFields);
              result.put(
                  prefixField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__CLASS),
                  DatastoreConstants.CHUNK__CLASS);

              return result;
            })
        .build();
  }

  private IActivePivotInstanceDescription memoryCube() {
    return StartBuilding.cube(MONITORING_CUBE)
        .withCalculations(this::copperCalculations)
        .withMeasures(this::measures)
        .withDimensions(this::defineDimensions)
        .withSharedContextValue(QueriesTimeLimit.of(15, TimeUnit.SECONDS))
        .withSharedMdxContext()

        .withDefaultMember()
        .onHierarchy(MdxNamingUtil.hierarchyUniqueName(
            IDimension.MEASURES,
            IHierarchy.MEASURES))
        .withMemberPath(CHUNK_COUNT)

        .withCalculatedMembers(calculatedMembers())
        .end()
        .build();
  }

  private ICanBuildCubeDescription<IActivePivotInstanceDescription> defineDimensions(
      final ICanStartBuildingDimensions builder) {
    return builder
        // FROM ChunkStore
        .withDimension(CHUNK_DIMENSION)
        .withHierarchy(CHUNK_ID_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK_ID)
        .withHierarchy(CHUNK_TYPE_LEVEL)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE)
        .withHierarchy(CHUNK_PARENT_ID_LEVEL)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_ID)
        .withProperty("description", "What are chunks for")
        .withSingleLevelHierarchy(CHUNK_DICO_ID_LEVEL)
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_DICO_ID)
        .withSingleLevelHierarchy(CHUNK_INDEX_ID_LEVEL)
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_INDEX_ID)
        .withSingleLevelHierarchy(CHUNK_REF_ID_LEVEL)
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_REF_ID)
        .withHierarchy(CHUNK_CLASS_LEVEL)
        .withLevelOfSameName()
        .withPropertyName(prefixField(DatastoreConstants.CHUNK_STORE,
            DatastoreConstants.CHUNK__CLASS))
        .withFormatter(ClassFormatter.KEY)
        .withProperty("description", "Class of the chunks")
        .withDimension(PARTITION_DIMENSION)
        .withSingleLevelHierarchy(PARTITION_HIERARCHY)
        .withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)
        .withFormatter(PartitionIdFormatter.KEY)
        .withDimension(CHUNK_DUMP_NAME_LEVEL)
        .withHierarchyOfSameName()
        .slicing()
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__DUMP_NAME)
        .withComparator(ReverseOrderComparator.type)
        .withHierarchy(DATE_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.APPLICATION__DATE)
        .withType(ILevelInfo.LevelType.TIME)
        .withComparator(ReverseOrderComparator.type)
        .withProperty("description", "Date at which statistics were retrieved")
        .withDimension(AGGREGATE_PROVIDER_DIMENSION)
        .withHierarchy(MANAGER_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.PIVOT__MANAGER_ID)
        .withHierarchy(PIVOT_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.PIVOT__PIVOT_ID)
        .withHierarchy(PROVIDER_TYPE_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.PROVIDER_COMPONENT__TYPE)
        .withHierarchy(PROVIDER_PARTITION_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)
        .withHierarchy(PROVIDER_ID_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PROVIDER_ID)

        .withDimension(VERSION_DIMENSION)
        .withHierarchy(INTERNAL_EPOCH_ID_HIERARCHY)
        .hidden()
        .withLevel(INTERNAL_EPOCH_ID_HIERARCHY)
        .withPropertyName(DatastoreConstants.VERSION__EPOCH_ID)
        .withComparator(ReverseOrderComparator.type)

        .withHierarchy(BRANCH_HIERARCHY)
        .slicing()
        .withLevel(BRANCH_HIERARCHY)
        .withPropertyName(DatastoreConstants.VERSION__BRANCH_NAME)

        .withSingleLevelHierarchy(USED_BY_VERSION_DIMENSION)
        .withPropertyName(DatastoreConstants.CHUNK__USED_BY_VERSION)

        .withDimension(OWNER_DIMENSION)
        .withSingleLevelHierarchy(OWNER_HIERARCHY)
        .withPropertyName(DatastoreConstants.OWNER__OWNER)

        .withDimension(COMPONENT_DIMENSION)
        .withSingleLevelHierarchy(COMPONENT_HIERARCHY)
        .withPropertyName(DatastoreConstants.OWNER__COMPONENT)

        .withDimension(FIELD_DIMENSION)
        .withSingleLevelHierarchy(FIELD_HIERARCHY)
        .withPropertyName(DatastoreConstants.OWNER__FIELD);
  }

  private IHasAtLeastOneMeasure measures(ICanStartBuildingMeasures builder) {
    return builder
        .withContributorsCount()
        .withFormatter(NUMBER_FORMATTER)
        .withUpdateTimestamp()
        .withFormatter(DateFormatter.TYPE + "[HH:mm:ss]");
  }

  private ICalculatedMemberDescription[] calculatedMembers() {
    return new ICalculatedMemberDescription[] {
        createMdxMeasureDescription("Owner.COUNT",
            ownershipCountMdxExpression(OWNER_DIMENSION, OWNER_HIERARCHY),
            OWNERSHIP_FOLDER),
        createMdxMeasureDescription("Component.COUNT",
            ownershipCountMdxExpression(COMPONENT_DIMENSION, COMPONENT_HIERARCHY),
            OWNERSHIP_FOLDER),
        createMdxMeasureDescription("Field.COUNT",
            ownershipCountMdxExpression(FIELD_DIMENSION, FIELD_HIERARCHY),
            OWNERSHIP_FOLDER)
    };
  }

  private ICalculatedMemberDescription createMdxMeasureDescription(
      final String measureName, final String mdxExpression) {
    return new CalculatedMemberDescription("[Measures].[" + measureName + "]", mdxExpression);
  }

  private ICalculatedMemberDescription createMdxMeasureDescription(
      final String measureName, final String mdxExpression, final String folder) {
    final ICalculatedMemberDescription description =
        createMdxMeasureDescription(measureName, mdxExpression);
    description.setFolder(folder);
    return description;
  }

  private String ownershipCountMdxExpression(
      final String dimensionName, final String hierarchyName) {
    return ownershipCountMdxExpression(
        MdxNamingUtil.hierarchyUniqueName(dimensionName, hierarchyName));
  }

  private String ownershipCountMdxExpression(final String hierarchyUniqueName) {
    return "DistinctCount("
        + "  Generate("
        + "    NonEmpty("
        + "      [Chunks].[ChunkId].[ALL].[AllMember].Children,"
        + "      {[Measures].[contributors.COUNT]}"
        + "    ),"
        + "    NonEmpty("
        + "      " + hierarchyUniqueName + ".[ALL].[AllMember].Children,"
        + "      {[Measures].[contributors.COUNT]}"
        + "    )"
        + "  )"
        + ")";
  }

  private void copperCalculations(final ICopperContext context) {
    memoryMeasure(context);
    joinHierarchies(context);
    chunkMeasures(context);
    applicationMeasure(context);
  }

  private void memoryMeasure(final ICopperContext context) {
    Copper.agg(DatastoreConstants.APPLICATION__USED_ON_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(USED_HEAP)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(MEMORY_FOLDER)
        .publish(context);
    Copper.agg(DatastoreConstants.APPLICATION__MAX_ON_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(COMMITTED_HEAP)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(MEMORY_FOLDER)
        .publish(context);
    Copper.agg(DatastoreConstants.APPLICATION__USED_OFF_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(USED_DIRECT)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(MEMORY_FOLDER)
        .publish(context);
    Copper.agg(DatastoreConstants.APPLICATION__MAX_OFF_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(MAX_DIRECT)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(MEMORY_FOLDER)
        .publish(context);
  }

  private void joinHierarchies(final ICopperContext context) {

    // --------------------
    // Define Copper Joins

    // --------------------
    // Epoch view store
    CopperStore epochViewStore =
        Copper.store(DatastoreConstants.EPOCH_VIEW_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.OWNER__OWNER, OWNER_HIERARCHY)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL)
            .withMapping(DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID, INTERNAL_EPOCH_ID_HIERARCHY);

    Copper.newSingleLevelHierarchy(VERSION_DIMENSION, EPOCH_ID_HIERARCHY, EPOCH_ID_HIERARCHY)
        .from(epochViewStore.field(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID))
        .withComparator(EpochViewComparator.PLUGIN_KEY)
        .publish(context);

    // --------------------
    // Dictionary store
    chunkToDicoStore = Copper.store(DatastoreConstants.DICTIONARY_STORE)
        .joinToCube()
        .withMapping(DatastoreConstants.DICTIONARY_ID, CHUNK_DICO_ID_LEVEL)
        .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL)
        .withMapping(DatastoreConstants.VERSION__EPOCH_ID, INTERNAL_EPOCH_ID_HIERARCHY);

    // --------------------
    // Reference store
    CopperStore chunkToReferenceStore =
        Copper.store(DatastoreConstants.REFERENCE_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.REFERENCE_ID, CHUNK_REF_ID_LEVEL)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL)
            .withMapping(DatastoreConstants.VERSION__EPOCH_ID, INTERNAL_EPOCH_ID_HIERARCHY);

    Copper.newSingleLevelHierarchy(REFERENCE_NAMES_HIERARCHY)
        .from(chunkToReferenceStore.field(DatastoreConstants.REFERENCE_NAME))
        .publish(context);

    // --------------------
    // Index store
    CopperStore chunkToIndexStore =
        Copper.store(DatastoreConstants.INDEX_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.INDEX_ID, CHUNK_INDEX_ID_LEVEL)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL)
            .withMapping(DatastoreConstants.VERSION__EPOCH_ID, INTERNAL_EPOCH_ID_HIERARCHY);

    Copper.newSingleLevelHierarchy(INDEX_DIMENSION, INDEXED_FIELDS_HIERARCHY,
        INDEXED_FIELDS_HIERARCHY)
        .from(chunkToIndexStore.field(DatastoreConstants.INDEX__FIELDS))
        .publish(context);

    Copper.newSingleLevelHierarchy(INDEX_DIMENSION, INDEX_TYPE_HIERARCHY, INDEX_TYPE_HIERARCHY)
        .from(chunkToIndexStore.field(DatastoreConstants.INDEX_TYPE))
        .publish(context);
  }

  private CopperMeasureToAggregateAbove perChunkAggregation(final String fieldName) {
    return perChunkAggregation(
        Copper.agg(fieldName, SingleValueFunction.PLUGIN_KEY));
  }

  private CopperMeasureToAggregateAbove perChunkAggregation(final CopperMeasure measure) {
    return measure.per(Copper.level(CHUNK_ID_HIERARCHY), Copper.level(INTERNAL_EPOCH_ID_HIERARCHY),
        Copper.level(CHUNK_DUMP_NAME_LEVEL));
  }

  private void chunkMeasures(final ICopperContext context) {
    perChunkAggregation(Copper.constant(1L))
        .sum()
        .as(CHUNK_COUNT)
        .publish(context);

    Copper.agg(
        chunkToDicoStore.field(DatastoreConstants.DICTIONARY_SIZE),
        SingleValueFunction.PLUGIN_KEY)
        .filter(Copper.level(COMPONENT_HIERARCHY)
            .eq(ParentType.DICTIONARY))
        .per(
            Copper.level(CHUNK_ID_HIERARCHY),
            Copper.level(INTERNAL_EPOCH_ID_HIERARCHY),
            Copper.level(CHUNK_DUMP_NAME_LEVEL),
            Copper.level(FIELD_HIERARCHY),
            Copper.level(OWNER_HIERARCHY))
        .sum()
        .as(DICTIONARY_SIZE)
        .withFormatter(NUMBER_FORMATTER)
        .publish(context);

    final CopperMeasure directMemory =
        perChunkAggregation(DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
            .sum()
            .as(DIRECT_MEMORY_SUM)
            .withFormatter(ByteFormatter.KEY)
            .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__ON_HEAP_SIZE)
        .sum()
        .as(HEAP_MEMORY_SUM)
        .withFormatter(ByteFormatter.KEY)
        .publish(context);

    final CopperMeasure chunkSize =
        perChunkAggregation(DatastoreConstants.CHUNK__SIZE)
            .sum()
            .as(CHUNK_SIZE_SUM)
            .withFormatter(ByteFormatter.KEY)
            .publish(context);

    final CopperMeasure nonWrittenRowsCount =
        perChunkAggregation(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS)
            .sum()
            .as(NON_WRITTEN_ROWS_COUNT)
            .withFormatter(NUMBER_FORMATTER)
            .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__FREE_ROWS)
        .sum()
        .withFormatter(NUMBER_FORMATTER)
        .as(DELETED_ROWS_COUNT)
        .withType(ILiteralType.DOUBLE)
        .divide(chunkSize)
        .withFormatter(PERCENT_FORMATTER)
        .as(DELETED_ROWS_RATIO)
        .publish(context);

    nonWrittenRowsCount
        .withType(ILiteralType.DOUBLE)
        .as("0") // FIXME: workaround PIVOT-4458
        .divide(chunkSize)
        .withFormatter(PERCENT_FORMATTER)
        .as(NON_WRITTEN_ROWS_RATIO)
        .publish(context);

    final CopperMeasure committedRows = chunkSize
        .minus(nonWrittenRowsCount)
        .withFormatter(NUMBER_FORMATTER)
        .withType(ILiteralType.DOUBLE) // Overflow happens if we don't cast it to double
        .as(COMITTED_ROWS)
        .publish(context);

    committedRows
        .divide(chunkSize)
        .multiply(directMemory)
        .withType(ILiteralType.LONG)
        .withFormatter(ByteFormatter.KEY)
        .as(COMITTED_CHUNK)
        .publish(context);

    committedRows
        .divide(chunkSize)
        .withFormatter(PERCENT_FORMATTER)
        .as(COMMITTED_MEMORY_RATIO)
        .publish(context);

    directMemory
        .withType(ILiteralType.DOUBLE)
        .as("1") // FIXME: workaround
        .divide(directMemory.grandTotal())
        .withFormatter(PERCENT_FORMATTER)
        .as(DIRECT_MEMORY_RATIO)
        .publish(context);

    directMemory
        .withType(ILiteralType.DOUBLE)
        .as("2") // FIXME: workaround
        .divide(Copper.measure(USED_DIRECT))
        .withFormatter(PERCENT_FORMATTER)
        .as(USED_MEMORY_RATIO)
        .publish(context);

    directMemory
        .withType(ILiteralType.DOUBLE)
        .as("3") // FIXME: workaround
        .divide(Copper.measure(MAX_DIRECT))
        .withFormatter(PERCENT_FORMATTER)
        .as(MAX_MEMORY_RATIO)
        .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__VECTOR_BLOCK_REF_COUNT)
        .sum()
        .per(Copper.hierarchy(FIELD_HIERARCHY).level(FIELD_HIERARCHY))
        .doNotAggregateAbove()
        .as(VECTOR_BLOCK_REFCOUNT)
        .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__VECTOR_BLOCK_LENGTH)
        .sum()
        .per(Copper.level(FIELD_HIERARCHY))
        .doNotAggregateAbove()
        .as(VECTOR_BLOCK_SIZE)
        .publish(context);
  }

  private void applicationMeasure(final ICopperContext context) {}
}
