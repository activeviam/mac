/*
 * (C) ActiveViam 2018-2023
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.activepivot.copper.api.Copper;
import com.activeviam.activepivot.copper.api.CopperMeasure;
import com.activeviam.activepivot.copper.api.CopperMeasureToAggregateAbove;
import com.activeviam.activepivot.copper.api.CopperStore;
import com.activeviam.activepivot.core.datastore.api.builder.StartBuilding;
import com.activeviam.activepivot.core.impl.api.contextvalues.QueriesResultLimit;
import com.activeviam.activepivot.core.impl.api.contextvalues.QueriesTimeLimit;
import com.activeviam.activepivot.core.impl.internal.util.impl.MdxNamingUtil;
import com.activeviam.activepivot.core.intf.api.copper.ICopperContext;
import com.activeviam.activepivot.core.intf.api.cube.hierarchy.IDimension;
import com.activeviam.activepivot.core.intf.api.cube.hierarchy.IHierarchy;
import com.activeviam.activepivot.core.intf.api.cube.metadata.ILevelInfo;
import com.activeviam.activepivot.core.intf.api.cube.metadata.LevelIdentifier;
import com.activeviam.activepivot.core.intf.api.description.IActivePivotInstanceDescription;
import com.activeviam.activepivot.core.intf.api.description.IActivePivotManagerDescription;
import com.activeviam.activepivot.core.intf.api.description.ISelectionDescription;
import com.activeviam.activepivot.core.intf.api.description.builder.ICanBuildCubeDescription;
import com.activeviam.activepivot.core.intf.api.description.builder.ICanStartBuildingMeasures;
import com.activeviam.activepivot.core.intf.api.description.builder.IHasAtLeastOneMeasure;
import com.activeviam.activepivot.core.intf.api.description.builder.ISelectionDescriptionBuilder;
import com.activeviam.activepivot.core.intf.api.description.builder.dimension.ICanStartBuildingDimensions;
import com.activeviam.activepivot.server.spring.api.config.IActivePivotManagerDescriptionConfig;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.api.schema.FieldPath;
import com.activeviam.mac.comparators.ReverseEpochViewComparator;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.ChunkOwner.OwnerType;
import com.activeviam.mac.formatter.ByteFormatter;
import com.activeviam.mac.formatter.ClassFormatter;
import com.activeviam.mac.formatter.PartitionIdFormatter;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.tech.aggregation.internal.impl.SingleValueFunction;
import com.activeviam.tech.chunks.api.types.Types;
import com.activeviam.tech.core.api.format.IFormatter;
import com.activeviam.tech.core.api.ordering.IComparator;
import com.activeviam.tech.mvcc.api.IEpoch;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Manager Description Config that defines the manager description which contains the cube
 * dimensions and every CopperMeasure.
 *
 * @author ActiveViam
 */
@Configuration
@Import(MemoryAnalysisDatastoreDescriptionConfig.class)
public class ManagerDescriptionConfig implements IActivePivotManagerDescriptionConfig {

  /** The main monitoring cube name. */
  public static final String MONITORING_CUBE = "MemoryCube";

  /** The main monitoring schema name. */
  public static final String MONITORING_SCHEMA = "MemorySchema";

  /** The {@link QueriesTimeLimit} timeout duration. */
  public static final Duration TIMEOUT_DURATION = Duration.ofSeconds(15);

  // region formatters
  /** Formatter for Numbers. */
  public static final String NUMBER_FORMATTER = IFormatter.NUMBER_PLUGIN_KEY + "[#,###]";

  /** Formatter for Percentages. */
  public static final String PERCENT_FORMATTER = IFormatter.NUMBER_PLUGIN_KEY + "[#.##%]";

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
  public static final String USED_MEMORY_RATIO = "UsedDirectMemory.Ratio";

  /** Total off-heap memory footprint, relative to the total memory committed by the JVM. */
  public static final String MAX_MEMORY_RATIO = "MaxDirectMemory.Ratio";

  /** The dictionary size of the dictionary associated to a chunk. */
  public static final String DICTIONARY_SIZE = "DictionarySize.SUM";

  /** For vector block facts, the number of references to the vector block. */
  public static final String VECTOR_BLOCK_REFCOUNT = "VectorBlock.RefCount";

  /** For vector block facts, the size the vector block. */
  public static final String VECTOR_BLOCK_SIZE = "VectorBlock.Length";

  /** Measure of the chunk size. */
  public static final String CHUNK_SIZE_SUM = "ChunkSize.SUM";

  /** Measure of the the non-written rows in Chunks. */
  public static final String NON_WRITTEN_ROWS_COUNT = "Unused rows";

  /**
   * Measure of the the non-written rows in Chunks, relative to the total non-written rows in the
   * application.
   */
  public static final String NON_WRITTEN_ROWS_RATIO = "Unused rows ratio";

  /** Measure of the deleted rows in Chunks. */
  public static final String DELETED_ROWS_COUNT = "Deleted rows";

  /**
   * Measure of the deleted rows in Chunks, relative to the total deleted rows in the application.
   */
  public static final String DELETED_ROWS_RATIO = "Deleted rows ratio";

  /** The number of committed rows within chunks. */
  public static final String COMMITTED_ROWS_COUNT = "Used rows";

  /** The size in bytes of chunk memory used to store effective data. */
  public static final String COMMITTED_CHUNK_MEMORY = "CommittedChunkMemory.SUM";

  /** The ratio of committed rows within chunks. */
  public static final String COMMITTED_ROWS_RATIO = "CommittedRows.Ratio";

  // endregion

  // region folders
  /** The name of the folder for measures related to application memory metrics. */
  public static final String APPLICATION_FOLDER = "Application Memory";

  /** The name of the folder for measures related to dictionaries. */
  public static final String DICTIONARY_FOLDER = "Dictionary";

  /** The name of the folder for measures related to chunks. */
  public static final String CHUNK_FOLDER = "Chunk";

  /** The name of the folder for measures related to datastore-related chunks. */
  public static final String STORE_CHUNK_FOLDER = "Datastore Chunk";

  /** The name of the folder for measures related to chunk memory usage. */
  public static final String CHUNK_MEMORY_FOLDER = "Chunk Memory";

  /** The name of the folder for measures related to vectors. */
  public static final String VECTOR_FOLDER = "Vector";

  /** The name of the folder for internal measures not intended for regular uses. */
  public static final String INTERNAL_FOLDER = "Internal";

  // endregion

  protected IDatastoreSchemaDescriptionConfig datastoreDescriptionConfig =
      new MemoryAnalysisDatastoreDescriptionConfig();

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

  @Bean
  @Override
  public IActivePivotManagerDescription managerDescription() {
    return StartBuilding.managerDescription()
        .withSchema(MONITORING_SCHEMA)
        .withSelection(memorySelection())
        .withCube(memoryCube())
        .build();
  }

  private ISelectionDescription memorySelection() {
    return StartBuilding.selection(datastoreDescriptionConfig.datastoreSchemaDescription())
        .fromBaseStore(DatastoreConstants.CHUNK_STORE)
        .withAllReachableFields(
            allReachableFields -> {
              allReachableFields.remove(DatastoreConstants.CHUNK__CLASS);
              final Map<String, FieldPath> result =
                  ISelectionDescriptionBuilder.FieldsCollisionHandler.CLOSEST.handle(
                      allReachableFields);
              result.put(
                  prefixField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__CLASS),
                  FieldPath.of(DatastoreConstants.CHUNK__CLASS));

              return result;
            })
        .build();
  }

  private IActivePivotInstanceDescription memoryCube() {
    return StartBuilding.cube(MONITORING_CUBE)
        .withCalculations(this::copperCalculations)
        .withMeasures(this::nativeMeasures)
        .withDimensions(this::defineDimensions)
        .withSharedContextValue(
            QueriesTimeLimit.of(TIMEOUT_DURATION.getSeconds(), TimeUnit.SECONDS))
        .withSharedContextValue(QueriesResultLimit.withoutLimit())
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
        .withHierarchyProperty("description", "Holds low-level hierarchies for classifying chunks")
        .withLevelOfSameName()
        .withLevelProperty("description", "The ID of the chunk")
        .withPropertyName(DatastoreConstants.CHUNK_ID)
        .withHierarchy(CHUNK_TYPE_LEVEL)
        .withHierarchyProperty(
            "description",
            "The kind of data the chunk holds"
                + " (e.g. RECORDS, DICTIONARY, INDEX, AGGREGATE_STORE, ...)")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE)
        .withLevelProperty(
            "description",
            "The kind of data the chunk holds"
                + " (e.g. RECORDS, DICTIONARY, INDEX, AGGREGATE_STORE, ...)")
        .withHierarchy(CHUNK_PARENT_ID_LEVEL)
        .withHierarchyProperty(
            "description",
            "The internal ID associated with the parent"
                + " structure holding the chunk (e.g. dictionary, index, aggregate store...)")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_ID)
        .withLevelProperty(
            "description",
            "The internal ID associated with the parent"
                + " structure holding the chunk (e.g. dictionary, index, aggregate store...)")
        .withHierarchy(CHUNK_DICO_ID_LEVEL)
        .withHierarchyProperty(
            "description", "The ID of the dictionary the chunk is attributed to, if any")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_DICO_ID)
        .withLevelProperty(
            "description", "The ID of the dictionary the chunk is attributed to, if any")
        .withHierarchy(CHUNK_INDEX_ID_LEVEL)
        .withHierarchyProperty(
            "description", "The ID of the index the chunk is attributed to, if any")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_INDEX_ID)
        .withLevelProperty("description", "The ID of the index the chunk is attributed to, if any")
        .withHierarchy(CHUNK_REF_ID_LEVEL)
        .withHierarchyProperty(
            "description", "The ID of the reference the chunk is attributed to, if any")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PARENT_REF_ID)
        .withLevelProperty(
            "description", "The ID of the reference the chunk is attributed to, if any")
        .withHierarchy(CHUNK_CLASS_LEVEL)
        .withHierarchyProperty("description", "The java class of the chunk")
        .withLevelOfSameName()
        .withPropertyName(
            prefixField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__CLASS))
        .withFormatter(ClassFormatter.KEY)
        .withLevelProperty("description", "The java class of the chunk")
        .withDimension(PARTITION_DIMENSION)
        .withDimensionProperty(
            "description", "The ID of the store or cube partition that holds the chunk")
        .withHierarchy(PARTITION_HIERARCHY)
        .withLevelOfSameName()
        .withLevelProperty(
            "description", "The ID of the store or cube partition that holds the chunk")
        .withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)
        .withFormatter(PartitionIdFormatter.KEY)
        .withLevelProperty(
            "description", "The ID of the store or cube partition that holds the chunk")
        .withDimension(CHUNK_DUMP_NAME_LEVEL)
        .withDimensionProperty(
            "description",
            "Holds hierarchies with metadata on the statistics from which the chunks were "
                + "retrieved")
        .withHierarchyOfSameName()
        .slicing()
        .withHierarchyProperty(
            "description", "The source folder name from which the statistics were retrieved")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__DUMP_NAME)
        .withComparator(IComparator.NATURAL_ORDER_PLUGIN_KEY)
        .withLevelProperty(
            "description", "The source folder name from which the statistics were retrieved")
        .withHierarchy(DATE_HIERARCHY)
        .withHierarchyProperty("description", "Date at which statistics were retrieved")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.APPLICATION__DATE)
        .withType(ILevelInfo.LevelType.TIME)
        .withComparator(IComparator.DESCENDING_NATURAL_ORDER_PLUGIN_KEY)
        .withLevelProperty("description", "Date at which statistics were retrieved")
        .withDimension(AGGREGATE_PROVIDER_DIMENSION)
        .withDimensionProperty(
            "description", "Holds hierarchies relative to pivot aggregate providers")
        .withHierarchy(MANAGER_HIERARCHY)
        .withHierarchyProperty(
            "description", "The id of the manager associated with the chunk, if any")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.PROVIDER__MANAGER_ID)
        .withLevelProperty("description", "The id of the manager associated with the chunk, if any")
        .withHierarchy(PROVIDER_TYPE_HIERARCHY)
        .withHierarchyProperty(
            "description", "The type of the aggregate provider associated with the chunk, if any")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.PROVIDER__TYPE)
        .withLevelProperty(
            "description", "The type of the aggregate provider associated with the chunk, if any")
        .withHierarchy(PROVIDER_CATEGORY_HIERARCHY)
        .withHierarchyProperty(
            "description",
            "The category of the aggregate provider associated with the chunk, if any")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.PROVIDER__CATEGORY)
        .withLevelProperty(
            "description",
            "The category of the aggregate provider associated with the chunk, if any")
        .withHierarchy(PROVIDER_ID_HIERARCHY)
        .withHierarchyProperty(
            "description", "The ID of the aggregate provider associated with the chunk, if any")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__PROVIDER_ID)
        .withLevelProperty(
            "description", "The ID of the aggregate provider associated with the chunk, if any")
        .withDimension(VERSION_DIMENSION)
        .withDimensionProperty(
            "description", "Holds hierarchies relative to the versioning of the chunks")
        .withHierarchy(INTERNAL_EPOCH_ID_HIERARCHY)
        .hidden()
        .withHierarchyProperty(
            "description",
            "The internal epoch ID of the chunk (may be less than the epoch to view)")
        .withLevel(INTERNAL_EPOCH_ID_HIERARCHY)
        .withPropertyName(DatastoreConstants.VERSION__EPOCH_ID)
        .withLevelProperty(
            "description",
            "The internal epoch ID of the chunk (may be less than the epoch to view)")
        .withComparator(IComparator.DESCENDING_NATURAL_ORDER_PLUGIN_KEY)
        .withHierarchy(BRANCH_HIERARCHY)
        .withHierarchyProperty("description", "The branch of the chunk")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.VERSION__BRANCH_NAME)
        .withFirstObjects(IEpoch.MASTER_BRANCH_NAME)
        .withLevelProperty("description", "The branch of the chunk")
        .withHierarchy(USED_BY_VERSION_DIMENSION)
        .withHierarchyProperty(
            "description",
            "Whether or not the chunk is known to be used by the currently viewed version")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__USED_BY_VERSION)
        .withLevelProperty(
            "description",
            "Whether or not the chunk is known to be used by the currently viewed version")
        .withDimension(OWNER_DIMENSION)
        .withDimensionProperty("description", "The cube(s) or store(s) owning the chunk")
        .withHierarchy(OWNER_HIERARCHY)
        .withHierarchyProperty("description", "The cube(s) or store(s) owning the chunk")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.OWNER__OWNER)
        .withLevelProperty("description", "The cube(s) or store(s) owning the chunk")
        .withDimension(COMPONENT_DIMENSION)
        .withDimensionProperty(
            "description",
            "The owning structure associated with the chunk (dictionary, index, ...)")
        .withHierarchy(COMPONENT_HIERARCHY)
        .withHierarchyProperty(
            "description",
            "The owning structure associated with the chunk (dictionary, index, ...)")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.OWNER__COMPONENT)
        .withLevelProperty(
            "description",
            "The owning structure associated with the chunk (dictionary, index, ...)")
        .withDimension(FIELD_DIMENSION)
        .withDimensionProperty("description", "The field(s) associated with the chunk, if any")
        .withHierarchy(FIELD_HIERARCHY)
        .withHierarchyProperty("description", "The field(s) associated with the chunk, if any")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.OWNER__FIELD)
        .withLevelProperty("description", "The field(s) associated with the chunk, if any");
  }

  private IHasAtLeastOneMeasure nativeMeasures(ICanStartBuildingMeasures builder) {
    return builder
        .withContributorsCount()
        .withFormatter(NUMBER_FORMATTER)
        .withinFolder(INTERNAL_FOLDER)
        .withUpdateTimestamp()
        .withFormatter(IFormatter.DATE_PLUGIN_KEY + "[HH:mm:ss]")
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
            .withMapping(
                DatastoreConstants.OWNER__OWNER,
                Copper.level(OWNER_DIMENSION, OWNER_HIERARCHY, OWNER_HIERARCHY))
            .withMapping(
                DatastoreConstants.CHUNK__DUMP_NAME,
                Copper.level(
                    new LevelIdentifier(
                        CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL)))
            .withMapping(
                DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID,
                Copper.level(
                    new LevelIdentifier(
                        VERSION_DIMENSION,
                        INTERNAL_EPOCH_ID_HIERARCHY,
                        INTERNAL_EPOCH_ID_HIERARCHY)));

    Copper.newHierarchy(VERSION_DIMENSION, EPOCH_ID_HIERARCHY)
        .fromField(epochViewStore.field(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID))
        .withLevelOfSameName()
        .withComparator(ReverseEpochViewComparator.PLUGIN_KEY)
        .publish(context);
  }

  private void joinReferencesToChunks(ICopperContext context) {
    final CopperStore chunkToReferenceStore =
        Copper.store(DatastoreConstants.REFERENCE_STORE)
            .joinToCube()
            .withMapping(
                DatastoreConstants.REFERENCE_ID,
                Copper.level(
                    new LevelIdentifier(CHUNK_DIMENSION, CHUNK_REF_ID_LEVEL, CHUNK_REF_ID_LEVEL)))
            .withMapping(
                DatastoreConstants.CHUNK__DUMP_NAME,
                Copper.level(
                    new LevelIdentifier(
                        CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL)))
            .withMapping(
                DatastoreConstants.VERSION__EPOCH_ID,
                Copper.level(
                    new LevelIdentifier(
                        VERSION_DIMENSION,
                        INTERNAL_EPOCH_ID_HIERARCHY,
                        INTERNAL_EPOCH_ID_HIERARCHY)));

    Copper.newHierarchy(REFERENCE_NAMES_HIERARCHY)
        .fromField(chunkToReferenceStore.field(DatastoreConstants.REFERENCE_NAME))
        .withLevelOfSameName()
        .publish(context);
  }

  private void joinIndexesToChunks(ICopperContext context) {
    final CopperStore chunkToIndexStore =
        Copper.store(DatastoreConstants.INDEX_STORE)
            .joinToCube()
            .withMapping(
                DatastoreConstants.INDEX_ID,
                Copper.level(
                    new LevelIdentifier(
                        CHUNK_DIMENSION, CHUNK_INDEX_ID_LEVEL, CHUNK_INDEX_ID_LEVEL)))
            .withMapping(
                DatastoreConstants.CHUNK__DUMP_NAME,
                Copper.level(
                    new LevelIdentifier(
                        CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL)))
            .withMapping(
                DatastoreConstants.VERSION__EPOCH_ID,
                Copper.level(
                    new LevelIdentifier(
                        VERSION_DIMENSION,
                        INTERNAL_EPOCH_ID_HIERARCHY,
                        INTERNAL_EPOCH_ID_HIERARCHY)));

    Copper.newHierarchy(INDEX_DIMENSION, INDEXED_FIELDS_HIERARCHY)
        .fromField(chunkToIndexStore.field(DatastoreConstants.INDEX__FIELDS))
        .withLevelOfSameName()
        .publish(context);

    Copper.newHierarchy(INDEX_DIMENSION, INDEX_TYPE_HIERARCHY)
        .fromField(chunkToIndexStore.field(DatastoreConstants.INDEX_TYPE))
        .withLevelOfSameName()
        .publish(context);
  }

  private void bucketingHierarchies(final ICopperContext context) {
    Copper.newHierarchy(OWNER_DIMENSION, OWNER_TYPE_HIERARCHY)
        .fromValues(
            Copper.level(OWNER_DIMENSION, OWNER_HIERARCHY, OWNER_HIERARCHY)
                .map(ChunkOwner::getType))
        .withMemberList((Object[]) OwnerType.values())
        .withLevelOfSameName()
        .publish(context);
  }

  private void applicationMeasure(final ICopperContext context) {
    Copper.agg(DatastoreConstants.APPLICATION__USED_ON_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(USED_HEAP)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(APPLICATION_FOLDER)
        .withDescription("the amount of heap memory used by the application")
        .publish(context);
    Copper.agg(DatastoreConstants.APPLICATION__MAX_ON_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(COMMITTED_HEAP)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(APPLICATION_FOLDER)
        .withDescription("the total size of the JVM heap")
        .publish(context);
    Copper.agg(DatastoreConstants.APPLICATION__USED_OFF_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(USED_DIRECT)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(APPLICATION_FOLDER)
        .withDescription("the amount of off-heap memory used by the application")
        .publish(context);
    Copper.agg(DatastoreConstants.APPLICATION__MAX_OFF_HEAP, SingleValueFunction.PLUGIN_KEY)
        .as(MAX_DIRECT)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(APPLICATION_FOLDER)
        .withDescription("the amount of off-heap memory reserved by the application")
        .publish(context);
  }

  private void chunkMeasures(final ICopperContext context) {
    perChunkAggregation(Copper.constant(1L))
        .sum()
        .as(CHUNK_COUNT)
        .withinFolder(CHUNK_FOLDER)
        .withDescription("the number of contributing chunks")
        .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__ON_HEAP_SIZE)
        .sum()
        .as(HEAP_MEMORY_SUM)
        .withFormatter(ByteFormatter.KEY)
        .withinFolder(CHUNK_MEMORY_FOLDER)
        .withDescription("an estimate of the on-heap size of the chunks")
        .publish(context);

    final CopperMeasure chunkSize =
        perChunkAggregation(DatastoreConstants.CHUNK__SIZE)
            .sum()
            .as(CHUNK_SIZE_SUM)
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .withDescription("a sum aggregation of the chunk sizes")
            .publish(context);

    final CopperMeasure nonWrittenRowsCount =
        perChunkAggregation(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS)
            .sum()
            .as(NON_WRITTEN_ROWS_COUNT)
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .withDescription("the number of unused rows within the chunks")
            .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__FREE_ROWS)
        .sum()
        .mapToDouble(a -> a.readDouble(0))
        .withFormatter(NUMBER_FORMATTER)
        .as(DELETED_ROWS_COUNT)
        .withinFolder(CHUNK_FOLDER)
        .withDescription("the number of freed rows within the chunks")
        .publish(context)
        .divide(chunkSize)
        .withType(Types.TYPE_DOUBLE)
        .withFormatter(PERCENT_FORMATTER)
        .as(DELETED_ROWS_RATIO)
        .withinFolder(CHUNK_FOLDER)
        .withDescription("the ratio of freed rows within the chunks")
        .publish(context);

    final CopperMeasure totalRows =
        Copper.sum(DatastoreConstants.CHUNK__SIZE)
            .withDescription("the total number of rows in chunks")
            .as("Physical row count")
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .publish(context);

    nonWrittenRowsCount
        .divide(totalRows)
        .withType(Types.TYPE_DOUBLE)
        .withFormatter(PERCENT_FORMATTER)
        .as(NON_WRITTEN_ROWS_RATIO)
        .withinFolder(CHUNK_FOLDER)
        .withDescription("the ratio of unused rows within the chunks")
        .publish(context);

    chunkSize
        .minus(nonWrittenRowsCount)
        .withFormatter(NUMBER_FORMATTER)
        .as(COMMITTED_ROWS_COUNT)
        .withinFolder(CHUNK_FOLDER)
        .withDescription("the number of committed (i.e. used) rows inside the chunks")
        .publish(context);

    Copper.measure(COMMITTED_ROWS_COUNT)
        .divide(chunkSize)
        .withType(Types.TYPE_DOUBLE)
        .withFormatter(PERCENT_FORMATTER)
        .as(COMMITTED_ROWS_RATIO)
        .withinFolder(CHUNK_FOLDER)
        .withDescription("the ratio of committed (i.e. used) rows inside the chunks")
        .publish(context);

    final CopperMeasure directMemory =
        perChunkAggregation(DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
            .sum()
            .as(DIRECT_MEMORY_SUM)
            .withFormatter(ByteFormatter.KEY)
            .withinFolder(CHUNK_MEMORY_FOLDER)
            .withDescription("the off-heap size of the chunks")
            .publish(context);

    Copper.measure(COMMITTED_ROWS_RATIO)
        .multiply(directMemory)
        .mapToLong(a -> (long) a.readDouble(0))
        .withFormatter(ByteFormatter.KEY)
        .as(COMMITTED_CHUNK_MEMORY)
        .withinFolder(CHUNK_MEMORY_FOLDER)
        .withDescription("the amount of memory in bytes used to store committed rows in chunks")
        .publish(context);

    directMemory
        .divide(directMemory.grandTotal())
        .withType(Types.TYPE_DOUBLE)
        .withFormatter(PERCENT_FORMATTER)
        .as(DIRECT_MEMORY_RATIO)
        .withinFolder(CHUNK_MEMORY_FOLDER)
        .withDescription(
            "the total ratio of off-heap memory consumed by the chunks relative to the total used "
                + "chunk memory")
        .publish(context);

    directMemory
        .divide(Copper.measure(USED_DIRECT))
        .withType(Types.TYPE_DOUBLE)
        .withFormatter(PERCENT_FORMATTER)
        .as(USED_MEMORY_RATIO)
        .withinFolder(CHUNK_MEMORY_FOLDER)
        .withDescription(
            "the total ratio of off-heap memory consumed by the chunks relative to the total used "
                + "application committed memory")
        .publish(context);

    directMemory
        .divide(Copper.measure(MAX_DIRECT))
        .withType(Types.TYPE_DOUBLE)
        .withFormatter(PERCENT_FORMATTER)
        .as(MAX_MEMORY_RATIO)
        .withinFolder(CHUNK_MEMORY_FOLDER)
        .withDescription(
            "the total ratio of off-heap memory consumed by the chunks relative to the total "
                + "application committed memory")
        .publish(context);
  }

  private void dictionaryMeasures(ICopperContext context) {
    final var chunkToDicoStore =
        Copper.store(DatastoreConstants.DICTIONARY_STORE)
            .joinToCube()
            .withMapping(
                DatastoreConstants.DICTIONARY_ID,
                Copper.level(
                    new LevelIdentifier(CHUNK_DIMENSION, CHUNK_DICO_ID_LEVEL, CHUNK_DICO_ID_LEVEL)))
            .withMapping(
                DatastoreConstants.CHUNK__DUMP_NAME,
                Copper.level(
                    new LevelIdentifier(
                        CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL)))
            .withMapping(
                DatastoreConstants.VERSION__EPOCH_ID,
                Copper.level(
                    new LevelIdentifier(
                        VERSION_DIMENSION,
                        INTERNAL_EPOCH_ID_HIERARCHY,
                        INTERNAL_EPOCH_ID_HIERARCHY)));

    Copper.agg(
            chunkToDicoStore.field(DatastoreConstants.DICTIONARY_SIZE),
            SingleValueFunction.PLUGIN_KEY)
        .filter(
            Copper.level(
                    new LevelIdentifier(
                        COMPONENT_DIMENSION, COMPONENT_HIERARCHY, COMPONENT_HIERARCHY))
                .eq(ParentType.DICTIONARY))
        .per(
            Copper.level(
                new LevelIdentifier(
                    VERSION_DIMENSION, INTERNAL_EPOCH_ID_HIERARCHY, INTERNAL_EPOCH_ID_HIERARCHY)),
            Copper.level(
                new LevelIdentifier(
                    CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL)),
            Copper.level(
                new LevelIdentifier(CHUNK_DIMENSION, CHUNK_DICO_ID_LEVEL, CHUNK_DICO_ID_LEVEL)))
        .sum()
        .as(DICTIONARY_SIZE)
        .withFormatter(NUMBER_FORMATTER)
        .withinFolder(DICTIONARY_FOLDER)
        .withDescription("the number of entries in the corresponding dictionary, when relevant")
        .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__SIZE)
        .max()
        .per(
            Copper.hierarchy(OWNER_DIMENSION, OWNER_HIERARCHY).level(OWNER_HIERARCHY),
            Copper.hierarchy(FIELD_DIMENSION, FIELD_HIERARCHY).level(FIELD_HIERARCHY),
            Copper.hierarchy(PARTITION_DIMENSION, PARTITION_HIERARCHY).level(PARTITION_HIERARCHY),
            Copper.hierarchy(CHUNK_DIMENSION, CHUNK_CLASS_LEVEL).level(CHUNK_CLASS_LEVEL))
        .min()
        .as("Chunk size")
        .withFormatter(NUMBER_FORMATTER)
        .withinFolder(STORE_CHUNK_FOLDER)
        .withDescription("the size of each chunk for the store")
        .publish(context);
  }

  private void vectorMeasures(ICopperContext context) {
    perChunkAggregation(DatastoreConstants.CHUNK__VECTOR_BLOCK_REF_COUNT)
        .sum()
        .as(VECTOR_BLOCK_REFCOUNT)
        .withinFolder(VECTOR_FOLDER)
        .withDescription(
            "The amount of references held towards the vectors contained in the vector block of the chunks, when relevant")
        .publish(context);

    perChunkAggregation(DatastoreConstants.CHUNK__VECTOR_BLOCK_LENGTH)
        .custom(SingleValueFunction.PLUGIN_KEY)
        // The underlying vector block length should be the same for all the chunks of an
        // application
        .per(Copper.level(new LevelIdentifier(FIELD_DIMENSION, FIELD_HIERARCHY, FIELD_HIERARCHY)))
        .doNotAggregateAbove()
        .as(VECTOR_BLOCK_SIZE)
        .withinFolder(VECTOR_FOLDER)
        .withDescription("the length of the Block holding the vector data, when relevant")
        .publish(context);
  }

  private CopperMeasureToAggregateAbove perChunkAggregation(final String fieldName) {
    return perChunkAggregation(Copper.agg(fieldName, SingleValueFunction.PLUGIN_KEY));
  }

  private CopperMeasureToAggregateAbove perChunkAggregation(final CopperMeasure measure) {
    return measure.per(
        Copper.level(new LevelIdentifier(CHUNK_DIMENSION, CHUNK_ID_HIERARCHY, CHUNK_ID_HIERARCHY)),
        Copper.level(
            new LevelIdentifier(
                CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL)));
  }
}
