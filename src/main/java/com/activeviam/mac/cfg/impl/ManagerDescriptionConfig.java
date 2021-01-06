/*
 * (C) ActiveViam 2018-2021
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.builders.StartBuilding;
import com.activeviam.comparators.ReverseEpochViewComparator;
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
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.ChunkOwner.OwnerType;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.qfs.agg.impl.SingleValueFunction;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.qfs.multiversion.IEpoch;
import com.qfs.pivot.util.impl.MdxNamingUtil;
import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.quartetfs.biz.pivot.context.impl.QueriesTimeLimit;
import com.quartetfs.biz.pivot.cube.dimension.IDimension;
import com.quartetfs.biz.pivot.cube.hierarchy.IHierarchy;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.ISelectionDescription;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import com.quartetfs.fwk.format.impl.DateFormatter;
import com.quartetfs.fwk.format.impl.NumberFormatter;
import com.quartetfs.fwk.ordering.impl.NaturalOrderComparator;
import com.quartetfs.fwk.ordering.impl.ReverseOrderComparator;
import java.time.Duration;
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

  /** The main monitoring cube name. */
  public static final String MONITORING_CUBE = "MemoryCube";

  /** The main monitoring schema name. */
  public static final String MONITORING_SCHEMA = "MemorySchema";

  /** The {@link QueriesTimeLimit} timeout duration. */
  public static final Duration TIMEOUT_DURATION = Duration.ofSeconds(15);

  // region formatters
  /** Formatter for Numbers. */
  public static final String NUMBER_FORMATTER = NumberFormatter.TYPE + "[#,###]";
  /** Formatter for Percentages. */
  public static final String PERCENT_FORMATTER = NumberFormatter.TYPE + "[#.##%]";
  // endregion

  // region dimensions
  /** Name of the Chunk Hierarchy. */
  public static final String CHUNK_DIMENSION = "Chunks";

  /** Name of the component dimension. */
  public static final String COMPONENT_DIMENSION = "Components";
  /** Name of the component dimension. */
  public static final String OWNER_DIMENSION = "Owners";
  /** Name of the owner type analysis hierarchy. */
  public static final String OWNER_TYPE_HIERARCHY = "Owner Type";
  /** Name of the field dimension. */
  public static final String FIELD_DIMENSION = "Fields";
  /** Name of the index dimension. */
  public static final String INDEX_DIMENSION = "Indices";
  /** Name of the version dimension. */
  public static final String VERSION_DIMENSION = "Versions";
  /** Name of the aggregate provider dimension. */
  public static final String AGGREGATE_PROVIDER_DIMENSION = "Aggregate Provider";
  /** Name of the partition dimension. */
  public static final String PARTITION_DIMENSION = "Partitions";
  /** Name of the used by version dimension. */
  public static final String USED_BY_VERSION_DIMENSION = "Used by Version";
  // endregion

  // region hierarchies
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
  /** The name of the hierarchy of provider types. */
  public static final String PROVIDER_TYPE_HIERARCHY = "ProviderType";
  /** The name of the hierarchy of provider categories. */
  public static final String PROVIDER_CATEGORY_HIERARCHY = "ProviderCategory";
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
  /** Name of the date hierarchy. */
  public static final String DATE_HIERARCHY = "Date";
  // endregion

  // region levels
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
  // endregion

  // region measures
  /** Measure of the summed off-heap memory footprint. */
  public static final String DIRECT_MEMORY_SUM = "DirectMemory.SUM";

  /**
   * Measure for the ratio of off-heap memory relative to the application's total memory
   * consumption.
   */
  public static final String DIRECT_MEMORY_RATIO = "DirectMemory.Ratio";

  /**
   * Measure for counting the number of chunks.
   *
   * <p>Not equal to {@code contributors.COUNT} since MAC's base store granularity is finer than the
   * granularity of the chunk.
   */
  public static final String CHUNK_COUNT = "Chunks.COUNT";

  /**
   * Measure of the summed on-heap memory footprint.
   *
   * <p>This measure only sums the on-heap memory held by chunk, and therefore do not contains the
   * entire on-heap footprint of the entire ActivePivot Application
   */
  public static final String HEAP_MEMORY_SUM = "HeapMemory.SUM";
  /** Total on-heap memory footprint of the application. */
  public static final String USED_HEAP = "UsedHeapMemory";
  /** Total on-heap memory committed by the JVM. */
  public static final String COMMITTED_HEAP = "CommittedHeapMemory";
  /** Total off-heap memory footprint of the application. */
  public static final String USED_DIRECT = "UsedDirectMemory";
  /** Total off-heap memory committed by the JVM. */
  public static final String MAX_DIRECT = "MaxDirectMemory";

  /** Total on-heap memory footprint, relative to the total memory footprint of the application. */
  public static final String USED_MEMORY_RATIO = "UsedMemory.Ratio";
  /** Total off-heap memory footprint, relative to the total memory committed by the JVM. */
  public static final String MAX_MEMORY_RATIO = "MaxMemory.Ratio";

  /**
   * The dictionary size of the dictionary associated to a chunk.
   *
   * <p>Summed in case of multiple dictionaries.
   */
  public static final String DICTIONARY_SIZE = "Dictionary Size";

  /** For vector block facts, the number of references to the vector block. */
  public static final String VECTOR_BLOCK_REFCOUNT = "VectorBlock.RefCount";
  /** For vector block facts, the size the vector block. */
  public static final String VECTOR_BLOCK_SIZE = "VectorBlock.Length";

  /** Measure of the size of Chunks (in Bytes). */
  public static final String CHUNK_SIZE_SUM = "ChunkSize.SUM";

  /** Measure of the the non-written rows in Chunks. */
  public static final String NON_WRITTEN_ROWS_COUNT = "NonWrittenRows.COUNT";

  /**
   * Measure of the the non-written rows in Chunks, relative to the total non-written rows in the
   * application.
   */
  public static final String NON_WRITTEN_ROWS_RATIO = "NonWrittenRows.Ratio";

  /** Measure of the deleted rows in Chunks. */
  public static final String DELETED_ROWS_COUNT = "DeletedRows.COUNT";

  /**
   * Measure of the deleted rows in Chunks, relative to the total deleted rows in the application.
   */
  public static final String DELETED_ROWS_RATIO = "DeletedRows.Ratio";

  /** The number of committed rows within chunks. */
  public static final String COMMITTED_ROWS = "CommittedRows";
  /** The number of committed chunks. */
  public static final String COMMITTED_CHUNK = "CommittedChunk";
  /** The ratio of committed rows within chunks. */
  public static final String COMMITTED_MEMORY_RATIO = "CommittedMemory.Ratio";
  // endregion

  // region folders
  /** The name of the folder for measures related to application memory metrics. */
  public static final String APPLICATION_FOLDER = "Application Memory";
  /** The name of the folder for measures related to dictionaries. */
  public static final String DICTIONARY_FOLDER = "Dictionary";
  /** The name of the folder for measures related to chunks. */
  public static final String CHUNK_FOLDER = "Chunk";
  /** The name of the folder for measures related to chunk memory usage. */
  public static final String CHUNK_MEMORY_FOLDER = "Chunk Memory";
  /** The name of the folder for measures related to vectors. */
  public static final String VECTOR_FOLDER = "Vector";
  /** The name of the folder for internal measures not intended for regular uses. */
  public static final String INTERNAL_FOLDER = "Internal";
  // endregion

  @Bean
  @Override
  public IActivePivotManagerDescription userManagerDescription() {
    return ActivePivotManagerBuilder.postProcess(
        StartBuilding.managerDescription()
            .withSchema(MONITORING_SCHEMA)
            .withSelection(memorySelection())
            .withCube(memoryCube())
            .build(),
        userSchemaDescription());
  }

  @Override
  public IDatastoreSchemaDescription userSchemaDescription() {
    return new MemoryAnalysisDatastoreDescription();
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

  /**
   * Prefixes a field by another string.
   *
   * @param prefix string to prepend
   * @param field field to be prefixed
   * @return the prefixed string
   */
  private static String prefixField(String prefix, String field) {
    return prefix + field.substring(0, 1).toUpperCase() + field.substring(1);
  }

  private IActivePivotInstanceDescription memoryCube() {
    return StartBuilding.cube(MONITORING_CUBE)
        .withCalculations(this::copperCalculations)
        .withMeasures(this::nativeMeasures)
        .withDimensions(this::defineDimensions)
        .withSharedContextValue(
            QueriesTimeLimit.of(TIMEOUT_DURATION.getSeconds(), TimeUnit.SECONDS))
        .withSharedMdxContext()
        .withDefaultMember()
        .onHierarchy(MdxNamingUtil.hierarchyUniqueName(IDimension.MEASURES, IHierarchy.MEASURES))
        .withMemberPath(CHUNK_COUNT)
        .end()
        .build();
  }

  private ICanBuildCubeDescription<IActivePivotInstanceDescription> defineDimensions(
      final ICanStartBuildingDimensions builder) {
    return builder
        .withDimension(CHUNK_DIMENSION)
        .withHierarchy(CHUNK_ID_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK_ID)
        .withProperty("description", "The ID of the chunk")
        .withHierarchy(CHUNK_TYPE_LEVEL)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE)
        .withProperty(
            "description",
            "The kind of data the chunk holds"
                + " (e.g. RECORDS, DICTIONARY, INDEX, AGGREGATE_STORE, ...)")
        .withHierarchy(CHUNK_PARENT_ID_LEVEL)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_ID)
        .withProperty(
            "description",
            "The internal ID associated with the parent"
                + " structure holding the chunk (e.g. dictionary, index, aggregate store...)")
        .withHierarchy(CHUNK_DICO_ID_LEVEL)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_DICO_ID)
        .withProperty("description", "The ID of the dictionary the chunk is attributed to, if any")
        .withHierarchy(CHUNK_INDEX_ID_LEVEL)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_INDEX_ID)
        .withProperty("description", "The ID of the index the chunk is attributed to, if any")
        .withHierarchy(CHUNK_REF_ID_LEVEL)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_REF_ID)
        .withProperty("description", "The ID of the reference the chunk is attributed to, if any")
        .withHierarchy(CHUNK_CLASS_LEVEL)
        .withLevelOfSameName()
        .withPropertyName(
            prefixField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__CLASS))
        .withFormatter(ClassFormatter.KEY)
        .withProperty("description", "The java class of the chunk")
        .withDimension(PARTITION_DIMENSION)
        .withHierarchy(PARTITION_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)
        .withFormatter(PartitionIdFormatter.KEY)
        .withProperty("description", "The ID of the store or cube partition that holds the chunk")
        .withDimension(CHUNK_DUMP_NAME_LEVEL)
        .withHierarchyOfSameName()
        .slicing()
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__DUMP_NAME)
        .withComparator(NaturalOrderComparator.type)
        .withProperty(
            "description", "The source folder name from which the statistics were retrieved")
        .withHierarchy(DATE_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.APPLICATION__DATE)
        .withType(ILevelInfo.LevelType.TIME)
        .withComparator(ReverseOrderComparator.type)
        .withProperty("description", "Date at which statistics were retrieved")
        .withDimension(AGGREGATE_PROVIDER_DIMENSION)
        .withHierarchy(MANAGER_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.PROVIDER__MANAGER_ID)
        .withProperty("description", "The java class of the chunk")
        .withHierarchy(PROVIDER_TYPE_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.PROVIDER__TYPE)
        .withProperty(
            "description",
            "The type of the aggregate provider associated with the chunk," + " if any")
        .withHierarchy(PROVIDER_CATEGORY_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.PROVIDER__CATEGORY)
        .withProperty(
            "description",
            "The category of the aggregate provider associated with the " + "chunk, if any")
        .withHierarchy(PROVIDER_ID_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PROVIDER_ID)
        .withProperty(
            "description",
            "The ID of the aggregate provider associated with the " + "chunk, if any")
        .withDimension(VERSION_DIMENSION)
        .withHierarchy(INTERNAL_EPOCH_ID_HIERARCHY)
        .hidden()
        .withLevel(INTERNAL_EPOCH_ID_HIERARCHY)
        .withPropertyName(DatastoreConstants.VERSION__EPOCH_ID)
        .withProperty(
            "description",
            "The internal epoch ID of the chunk (may be less than the " + "epoch to view)")
        .withComparator(ReverseOrderComparator.type)
        .withHierarchy(BRANCH_HIERARCHY)
        .slicing()
        .withLevel(BRANCH_HIERARCHY)
        .withPropertyName(DatastoreConstants.VERSION__BRANCH_NAME)
        .withFirstObjects(IEpoch.MASTER_BRANCH_NAME)
        .withProperty("description", "The branch of the chunk version")
        .withHierarchy(USED_BY_VERSION_DIMENSION)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__USED_BY_VERSION)
        .withProperty(
            "description",
            "Whether or not the chunk is known to be used by the currently" + " viewed version")
        .withDimension(OWNER_DIMENSION)
        .withHierarchy(OWNER_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.OWNER__OWNER)
        .withProperty("description", "The cube(s) or store(s) owning the chunk")
        .withDimension(COMPONENT_DIMENSION)
        .withHierarchy(COMPONENT_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.OWNER__COMPONENT)
        .withProperty(
            "description",
            "The owning structure associated with the chunk (dictionary," + " index, ...)")
        .withDimension(FIELD_DIMENSION)
        .withHierarchy(FIELD_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.OWNER__FIELD)
        .withProperty("description", "The field(s) associated with the chunk, if any");
  }

  private IHasAtLeastOneMeasure nativeMeasures(ICanStartBuildingMeasures builder) {
    return builder
        .withContributorsCount()
        .withFormatter(NUMBER_FORMATTER)
        .withinFolder(INTERNAL_FOLDER)
        .withUpdateTimestamp()
        .withFormatter(DateFormatter.TYPE + "[HH:mm:ss]")
        .withinFolder(INTERNAL_FOLDER);
  }

  private void copperCalculations(final ICopperContext context) {
    joinHierarchies(context);
    bucketingHierarchies(context);

    applicationMeasure(context);
    chunkMeasures(context);
    dictionaryMeasures(context);
    vectorMeasures(context);
  }

  private void joinHierarchies(final ICopperContext context) {
    joinViewVersion(context);
    joinReferencesToChunks(context);
    joinIndexesToChunks(context);
  }

  private void joinViewVersion(ICopperContext context) {
    final CopperStore epochViewStore =
        Copper.store(DatastoreConstants.EPOCH_VIEW_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.OWNER__OWNER, OWNER_HIERARCHY)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL)
            .withMapping(DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID, INTERNAL_EPOCH_ID_HIERARCHY);

    Copper.newSingleLevelHierarchy(VERSION_DIMENSION, EPOCH_ID_HIERARCHY, EPOCH_ID_HIERARCHY)
        .from(epochViewStore.field(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID))
        .withComparator(ReverseEpochViewComparator.PLUGIN_KEY)
        .publish(context);
  }

  private void joinReferencesToChunks(ICopperContext context) {
    final CopperStore chunkToReferenceStore =
        Copper.store(DatastoreConstants.REFERENCE_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.REFERENCE_ID, CHUNK_REF_ID_LEVEL)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL)
            .withMapping(DatastoreConstants.VERSION__EPOCH_ID, INTERNAL_EPOCH_ID_HIERARCHY);

    Copper.newSingleLevelHierarchy(REFERENCE_NAMES_HIERARCHY)
        .from(chunkToReferenceStore.field(DatastoreConstants.REFERENCE_NAME))
        .publish(context);
  }

  private void joinIndexesToChunks(ICopperContext context) {
    final CopperStore chunkToIndexStore =
        Copper.store(DatastoreConstants.INDEX_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.INDEX_ID, CHUNK_INDEX_ID_LEVEL)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL)
            .withMapping(DatastoreConstants.VERSION__EPOCH_ID, INTERNAL_EPOCH_ID_HIERARCHY);

    Copper.newSingleLevelHierarchy(
            INDEX_DIMENSION, INDEXED_FIELDS_HIERARCHY, INDEXED_FIELDS_HIERARCHY)
        .from(chunkToIndexStore.field(DatastoreConstants.INDEX__FIELDS))
        .publish(context);

    Copper.newSingleLevelHierarchy(INDEX_DIMENSION, INDEX_TYPE_HIERARCHY, INDEX_TYPE_HIERARCHY)
        .from(chunkToIndexStore.field(DatastoreConstants.INDEX_TYPE))
        .publish(context);
  }

  private void bucketingHierarchies(final ICopperContext context) {
    Copper.newSingleLevelHierarchy(OWNER_DIMENSION, OWNER_TYPE_HIERARCHY, OWNER_TYPE_HIERARCHY)
        .from(Copper.level(OWNER_HIERARCHY).map(ChunkOwner::getType))
        .withMemberList((Object[]) OwnerType.values())
        .publish(context);
  }

  private void applicationMeasure(final ICopperContext context) {
    Copper.agg(DatastoreConstants.APPLICATION__USED_ON_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(USED_HEAP)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(APPLICATION_FOLDER)
        .publish(context);
    Copper.agg(DatastoreConstants.APPLICATION__MAX_ON_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(COMMITTED_HEAP)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(APPLICATION_FOLDER)
        .publish(context);
    Copper.agg(DatastoreConstants.APPLICATION__USED_OFF_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(USED_DIRECT)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(APPLICATION_FOLDER)
        .publish(context);
    Copper.agg(DatastoreConstants.APPLICATION__MAX_OFF_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(MAX_DIRECT)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(APPLICATION_FOLDER)
        .publish(context);
  }

  private void chunkMeasures(final ICopperContext context) {
    perChunkAggregation(Copper.constant(1L))
        .sum()
        .as(CHUNK_COUNT)
        .withinFolder(CHUNK_FOLDER)
        .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__ON_HEAP_SIZE)
        .sum()
        .as(HEAP_MEMORY_SUM)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(CHUNK_MEMORY_FOLDER)
        .publish(context);

    final CopperMeasure chunkSize =
        perChunkAggregation(DatastoreConstants.CHUNK__SIZE)
            .sum()
            .as(CHUNK_SIZE_SUM)
            .withFormatter(ByteFormatter.KEY)
            .withinFolder(CHUNK_FOLDER)
            .publish(context);

    final CopperMeasure nonWrittenRowsCount =
        perChunkAggregation(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS)
            .sum()
            .as(NON_WRITTEN_ROWS_COUNT)
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__FREE_ROWS)
        .sum()
        .withFormatter(NUMBER_FORMATTER)
        .as(DELETED_ROWS_COUNT)
        .withinFolder(CHUNK_FOLDER)
        .publish(context)
        .withType(ILiteralType.DOUBLE)
        .as("-") // FIXME: workaround PIVOT-4458
        .divide(chunkSize)
        .withFormatter(PERCENT_FORMATTER)
        .as(DELETED_ROWS_RATIO)
        .withinFolder(CHUNK_FOLDER)
        .publish(context);

    nonWrittenRowsCount
        .withType(ILiteralType.DOUBLE)
        .as("0") // FIXME: workaround
        .divide(chunkSize)
        .withFormatter(PERCENT_FORMATTER)
        .as(NON_WRITTEN_ROWS_RATIO)
        .withinFolder(CHUNK_FOLDER)
        .publish(context);

    final CopperMeasure committedRows =
        chunkSize
            .minus(nonWrittenRowsCount)
            .withFormatter(NUMBER_FORMATTER)
            .withType(ILiteralType.DOUBLE) // Overflow happens if we don't cast it to double
            .as(COMMITTED_ROWS)
            .withinFolder(CHUNK_FOLDER)
            .publish(context);

    committedRows
        .divide(chunkSize)
        .withFormatter(PERCENT_FORMATTER)
        .as(COMMITTED_MEMORY_RATIO)
        .withinFolder(CHUNK_FOLDER)
        .publish(context);

    final CopperMeasure directMemory =
        Copper.agg(DatastoreConstants.CHUNK__OFF_HEAP_SIZE, SingleValueFunction.PLUGIN_KEY)
            .per(Copper.level(CHUNK_ID_HIERARCHY), Copper.level(CHUNK_DUMP_NAME_LEVEL))
            .sum()
            .as(DIRECT_MEMORY_SUM)
            .withFormatter(ByteFormatter.KEY)
            .withinFolder(CHUNK_MEMORY_FOLDER)
            .publish(context);

    committedRows
        .divide(chunkSize)
        .multiply(directMemory)
        .withType(ILiteralType.LONG)
        .withFormatter(ByteFormatter.KEY)
        .as(COMMITTED_CHUNK)
        .withinFolder(CHUNK_MEMORY_FOLDER)
        .publish(context);

    directMemory
        .withType(ILiteralType.DOUBLE)
        .as("1") // FIXME: workaround
        .divide(directMemory.grandTotal())
        .withFormatter(PERCENT_FORMATTER)
        .as(DIRECT_MEMORY_RATIO)
        .withinFolder(CHUNK_MEMORY_FOLDER)
        .publish(context);

    directMemory
        .withType(ILiteralType.DOUBLE)
        .as("2") // FIXME: workaround
        .divide(Copper.measure(USED_DIRECT))
        .withFormatter(PERCENT_FORMATTER)
        .as(USED_MEMORY_RATIO)
        .withinFolder(CHUNK_MEMORY_FOLDER)
        .publish(context);

    directMemory
        .withType(ILiteralType.DOUBLE)
        .as("3") // FIXME: workaround
        .divide(Copper.measure(MAX_DIRECT))
        .withFormatter(PERCENT_FORMATTER)
        .as(MAX_MEMORY_RATIO)
        .withinFolder(CHUNK_MEMORY_FOLDER)
        .publish(context);
  }

  private void dictionaryMeasures(ICopperContext context) {
    final var chunkToDicoStore =
        Copper.store(DatastoreConstants.DICTIONARY_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.DICTIONARY_ID, CHUNK_DICO_ID_LEVEL)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL)
            .withMapping(DatastoreConstants.VERSION__EPOCH_ID, INTERNAL_EPOCH_ID_HIERARCHY);

    Copper.agg(
            chunkToDicoStore.field(DatastoreConstants.DICTIONARY_SIZE),
            SingleValueFunction.PLUGIN_KEY)
        .filter(Copper.level(COMPONENT_HIERARCHY).eq(ParentType.DICTIONARY))
        .per(
            Copper.level(INTERNAL_EPOCH_ID_HIERARCHY),
            Copper.level(CHUNK_DUMP_NAME_LEVEL),
            Copper.level(CHUNK_DICO_ID_LEVEL))
        .sum()
        .as(DICTIONARY_SIZE)
        .withFormatter(NUMBER_FORMATTER)
        .withinFolder(DICTIONARY_FOLDER)
        .publish(context);
  }

  private void vectorMeasures(ICopperContext context) {
    perChunkAggregation(DatastoreConstants.CHUNK__VECTOR_BLOCK_REF_COUNT)
        .sum()
        .per(Copper.hierarchy(FIELD_HIERARCHY).level(FIELD_HIERARCHY))
        .doNotAggregateAbove()
        .as(VECTOR_BLOCK_REFCOUNT)
        .withinFolder(VECTOR_FOLDER)
        .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__VECTOR_BLOCK_LENGTH)
        .sum()
        .per(Copper.level(FIELD_HIERARCHY))
        .doNotAggregateAbove()
        .as(VECTOR_BLOCK_SIZE)
        .withinFolder(VECTOR_FOLDER)
        .publish(context);
  }

  private CopperMeasureToAggregateAbove perChunkAggregation(final String fieldName) {
    return perChunkAggregation(Copper.agg(fieldName, SingleValueFunction.PLUGIN_KEY));
  }

  private CopperMeasureToAggregateAbove perChunkAggregation(final CopperMeasure measure) {
    return measure.per(Copper.level(CHUNK_ID_HIERARCHY), Copper.level(CHUNK_DUMP_NAME_LEVEL));
  }
}
