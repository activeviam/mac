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
import com.activeviam.activepivot.core.api.foundry.configurator.ICubeConfigurator;
import com.activeviam.activepivot.core.api.foundry.configurator.ICubeManagerConfigurator;
import com.activeviam.activepivot.core.api.foundry.configurator.IDimensionConfigurator;
import com.activeviam.activepivot.core.api.foundry.configurator.IDimensionRelationship;
import com.activeviam.activepivot.core.api.foundry.configurator.IDimensionRelationship.ManyToManyRelationship;
import com.activeviam.activepivot.core.api.foundry.configurator.IDimensionRelationship.ManyToOneRelationship;
import com.activeviam.activepivot.core.api.foundry.configurator.IDimensionsConfigurator;
import com.activeviam.activepivot.core.api.foundry.configurator.IHierarchyConfigurator;
import com.activeviam.activepivot.core.api.foundry.configurator.ILevelConfigurator;
import com.activeviam.activepivot.core.api.foundry.configurator.IMeasuresConfigurator;
import com.activeviam.activepivot.core.api.foundry.definition.hierarchy.ICustomHierarchyDefinition;
import com.activeviam.activepivot.core.api.foundry.definition.hierarchy.IDerivedHierarchyInput;
import com.activeviam.activepivot.core.api.foundry.definition.hierarchy.ITableHierarchyDefinition;
import com.activeviam.activepivot.core.api.foundry.measure.CopperNext;
import com.activeviam.activepivot.core.impl.api.contextvalues.QueriesResultLimit;
import com.activeviam.activepivot.core.impl.api.contextvalues.QueriesTimeLimit;
import com.activeviam.activepivot.core.impl.api.contextvalues.mdx.MdxContext;
import com.activeviam.activepivot.core.impl.api.description.impl.ComparatorDescription;
import com.activeviam.activepivot.core.impl.api.description.impl.ComparatorOrderDescription;
import com.activeviam.activepivot.core.impl.internal.description.ComparatorOrderDescriptionNames;
import com.activeviam.activepivot.core.intf.api.cube.hierarchy.IDimension;
import com.activeviam.activepivot.core.intf.api.cube.hierarchy.IHierarchy;
import com.activeviam.activepivot.core.intf.api.cube.hierarchy.IMeasureHierarchy;
import com.activeviam.activepivot.core.intf.api.cube.metadata.HierarchyIdentifier;
import com.activeviam.activepivot.core.intf.api.cube.metadata.ILevelInfo;
import com.activeviam.activepivot.core.intf.api.cube.metadata.LevelIdentifier;
import com.activeviam.database.api.schema.FieldPath;
import com.activeviam.database.api.schema.JoinPath;
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
import com.activeviam.tech.chunks.api.types.ContentType;
import com.activeviam.tech.chunks.api.types.Types;
import com.activeviam.tech.core.api.format.IFormatter;
import com.activeviam.tech.core.api.ordering.IComparator;
import com.activeviam.tech.mvcc.api.IEpoch;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Defines the monitoring cube, with its dimensions and its measures.
 *
 * @author ActiveViam
 */
public class ManagerDescriptionConfig {

  /** The main monitoring cube name. */
  public static final String MONITORING_CUBE = "MemoryCube";

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

  /** The total number of rows in chunks, without deduplicating the chunks. */
  public static final String PHYSICAL_ROW_COUNT = "Physical row count";

  /** The size of each chunk of a store. */
  public static final String STORE_CHUNK_SIZE = "Chunk size";

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

  /** Name of the dimension holding the epochs viewing the chunks. */
  public static final String EPOCH_DIMENSION = "Epochs";

  private static final LevelIdentifier CHUNK_ID_LEVEL =
      new LevelIdentifier(CHUNK_DIMENSION, CHUNK_ID_HIERARCHY, CHUNK_ID_HIERARCHY);

  private static final LevelIdentifier DICO_ID_LEVEL =
      new LevelIdentifier(CHUNK_DIMENSION, CHUNK_DICO_ID_LEVEL, CHUNK_DICO_ID_LEVEL);

  private static final LevelIdentifier INDEX_ID_LEVEL =
      new LevelIdentifier(CHUNK_DIMENSION, CHUNK_INDEX_ID_LEVEL, CHUNK_INDEX_ID_LEVEL);

  private static final LevelIdentifier REFERENCE_ID_LEVEL =
      new LevelIdentifier(CHUNK_DIMENSION, CHUNK_REF_ID_LEVEL, CHUNK_REF_ID_LEVEL);

  private static final LevelIdentifier DUMP_NAME_LEVEL =
      new LevelIdentifier(CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL, CHUNK_DUMP_NAME_LEVEL);

  private static final LevelIdentifier INTERNAL_EPOCH_ID_LEVEL =
      new LevelIdentifier(
          VERSION_DIMENSION, INTERNAL_EPOCH_ID_HIERARCHY, INTERNAL_EPOCH_ID_HIERARCHY);

  private static final LevelIdentifier OWNER_LEVEL =
      new LevelIdentifier(OWNER_DIMENSION, OWNER_HIERARCHY, OWNER_HIERARCHY);

  private static final LevelIdentifier COMPONENT_LEVEL =
      new LevelIdentifier(COMPONENT_DIMENSION, COMPONENT_HIERARCHY, COMPONENT_HIERARCHY);

  private static final LevelIdentifier FIELD_LEVEL =
      new LevelIdentifier(FIELD_DIMENSION, FIELD_HIERARCHY, FIELD_HIERARCHY);

  private static final LevelIdentifier PARTITION_LEVEL =
      new LevelIdentifier(PARTITION_DIMENSION, PARTITION_HIERARCHY, PARTITION_HIERARCHY);

  private static final LevelIdentifier CLASS_LEVEL =
      new LevelIdentifier(CHUNK_DIMENSION, CHUNK_CLASS_LEVEL, CHUNK_CLASS_LEVEL);

  /**
   * Defines the monitoring cube: its dimensions, its measures and its shared context values.
   *
   * @param cubeManager the object used to configure the cubes
   */
  public void configureCubes(final ICubeManagerConfigurator cubeManager) {
    final ICubeConfigurator cube =
        cubeManager.addCube(MONITORING_CUBE, DatastoreConstants.CHUNK_STORE);
    cube.withSharedContextValue(
            QueriesTimeLimit.of(TIMEOUT_DURATION.getSeconds(), TimeUnit.SECONDS))
        .withSharedContextValue(QueriesResultLimit.withoutLimit())
        .withSharedContextValue(mdxContext());

    defineDimensions(cube.dimensions());
    defineMeasures(cube.measures());
  }

  private static MdxContext mdxContext() {
    final MdxContext mdxContext = new MdxContext();
    mdxContext.setDefaultMembers(
        Map.of(
            new HierarchyIdentifier(IDimension.MEASURES, IHierarchy.MEASURES),
            List.of(CHUNK_COUNT)));
    return mdxContext;
  }

  // region dimensions

  private void defineDimensions(final IDimensionsConfigurator dimensions) {
    defineChunkDimension(dimensions);
    definePartitionDimension(dimensions);
    defineImportInfoDimension(dimensions);
    defineAggregateProviderDimension(dimensions);
    defineVersionDimension(dimensions);
    defineOwnerDimension(dimensions);

    final String componentDescription =
        "The owning structure associated with the chunk (dictionary, index, ...)";
    addFactHierarchy(
        dimensions,
        addFactDimension(dimensions, COMPONENT_DIMENSION, componentDescription),
        COMPONENT_HIERARCHY,
        DatastoreConstants.OWNER__COMPONENT,
        componentDescription);

    final String fieldDescription = "The field(s) associated with the chunk, if any";
    addFactHierarchy(
        dimensions,
        addFactDimension(dimensions, FIELD_DIMENSION, fieldDescription),
        FIELD_HIERARCHY,
        DatastoreConstants.OWNER__FIELD,
        fieldDescription);

    defineEpochDimension(dimensions);
    defineReferenceDimension(dimensions);
    defineIndexDimension(dimensions);
  }

  private void defineChunkDimension(final IDimensionsConfigurator dimensions) {
    final IDimensionConfigurator chunks = addFactDimension(dimensions, CHUNK_DIMENSION, null);
    addFactHierarchy(
        dimensions,
        chunks,
        CHUNK_ID_HIERARCHY,
        FieldPath.of(DatastoreConstants.CHUNK_ID),
        "Holds low-level hierarchies for classifying chunks",
        "The ID of the chunk");
    addFactHierarchy(
        dimensions,
        chunks,
        CHUNK_TYPE_LEVEL,
        DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE,
        "The kind of data the chunk holds (e.g. RECORDS, DICTIONARY, INDEX, AGGREGATE_STORE, ...)");
    addFactHierarchy(
        dimensions,
        chunks,
        CHUNK_PARENT_ID_LEVEL,
        DatastoreConstants.CHUNK__PARENT_ID,
        "The internal ID associated with the parent"
            + " structure holding the chunk (e.g. dictionary, index, aggregate store...)");
    addFactHierarchy(
        dimensions,
        chunks,
        CHUNK_DICO_ID_LEVEL,
        DatastoreConstants.CHUNK__PARENT_DICO_ID,
        "The ID of the dictionary the chunk is attributed to, if any");
    addFactHierarchy(
        dimensions,
        chunks,
        CHUNK_INDEX_ID_LEVEL,
        DatastoreConstants.CHUNK__PARENT_INDEX_ID,
        "The ID of the index the chunk is attributed to, if any");
    addFactHierarchy(
        dimensions,
        chunks,
        CHUNK_REF_ID_LEVEL,
        DatastoreConstants.CHUNK__PARENT_REF_ID,
        "The ID of the reference the chunk is attributed to, if any");
    addFactHierarchy(
        dimensions,
        chunks,
        CHUNK_CLASS_LEVEL,
        DatastoreConstants.CHUNK__CLASS,
        "The java class of the chunk");
    dimensions.level(CLASS_LEVEL).formatter(ClassFormatter.KEY);
  }

  private void definePartitionDimension(final IDimensionsConfigurator dimensions) {
    addFactDimension(
            dimensions,
            PARTITION_DIMENSION,
            "The ID of the store or cube partition that holds the chunk")
        .addHierarchy(
            singleLevelHierarchy(
                PARTITION_HIERARCHY, FieldPath.of(DatastoreConstants.CHUNK__PARTITION_ID)));
    describe(
            dimensions,
            PARTITION_LEVEL,
            "The ID of the store or cube partition that holds the chunk")
        .formatter(PartitionIdFormatter.KEY);
  }

  private void defineImportInfoDimension(final IDimensionsConfigurator dimensions) {
    final IDimensionConfigurator importInfo =
        addFactDimension(
            dimensions,
            CHUNK_DUMP_NAME_LEVEL,
            "Holds hierarchies with metadata on the statistics from which the chunks were "
                + "retrieved");
    importInfo
        .addHierarchy(
            ITableHierarchyDefinition.builder(CHUNK_DUMP_NAME_LEVEL)
                .slicing()
                .withFieldLevel(
                    CHUNK_DUMP_NAME_LEVEL, FieldPath.of(DatastoreConstants.CHUNK__DUMP_NAME))
                .build())
        .description("The source folder name from which the statistics were retrieved");
    describe(
            dimensions,
            DUMP_NAME_LEVEL,
            "The source folder name from which the statistics were retrieved")
        .comparator(IComparator.NATURAL_ORDER_PLUGIN_KEY);

    addFactHierarchy(
            dimensions,
            importInfo,
            DATE_HIERARCHY,
            FieldPath.of(
                MemoryAnalysisDatastoreDescriptionConfig.CHUNK_TO_APP,
                DatastoreConstants.APPLICATION__DATE),
            "Date at which statistics were retrieved",
            "Date at which statistics were retrieved")
        .type(ILevelInfo.LevelType.TIME)
        .comparator(IComparator.DESCENDING_NATURAL_ORDER_PLUGIN_KEY);
  }

  private void defineAggregateProviderDimension(final IDimensionsConfigurator dimensions) {
    final IDimensionConfigurator providers =
        addFactDimension(
            dimensions,
            AGGREGATE_PROVIDER_DIMENSION,
            "Holds hierarchies relative to pivot aggregate providers");
    addFactHierarchy(
        dimensions,
        providers,
        MANAGER_HIERARCHY,
        providerField(DatastoreConstants.PROVIDER__MANAGER_ID),
        "The id of the manager associated with the chunk, if any");
    addFactHierarchy(
        dimensions,
        providers,
        PROVIDER_TYPE_HIERARCHY,
        providerField(DatastoreConstants.PROVIDER__TYPE),
        "The type of the aggregate provider associated with the chunk, if any");
    addFactHierarchy(
        dimensions,
        providers,
        PROVIDER_CATEGORY_HIERARCHY,
        providerField(DatastoreConstants.PROVIDER__CATEGORY),
        "The category of the aggregate provider associated with the chunk, if any");
    addFactHierarchy(
        dimensions,
        providers,
        PROVIDER_ID_HIERARCHY,
        DatastoreConstants.CHUNK__PROVIDER_ID,
        "The ID of the aggregate provider associated with the chunk, if any");
  }

  private static FieldPath providerField(final String field) {
    return FieldPath.of(MemoryAnalysisDatastoreDescriptionConfig.CHUNK_TO_PROVIDER, field);
  }

  private void defineVersionDimension(final IDimensionsConfigurator dimensions) {
    final IDimensionConfigurator versions =
        addFactDimension(
            dimensions,
            VERSION_DIMENSION,
            "Holds hierarchies relative to the versioning of the chunks");
    final String internalEpochDescription =
        "The internal epoch ID of the chunk (may be less than the epoch to view)";
    addFactHierarchy(
            dimensions,
            versions,
            INTERNAL_EPOCH_ID_HIERARCHY,
            FieldPath.of(DatastoreConstants.VERSION__EPOCH_ID),
            internalEpochDescription,
            internalEpochDescription)
        .comparator(IComparator.DESCENDING_NATURAL_ORDER_PLUGIN_KEY);
    versions.hierarchy(INTERNAL_EPOCH_ID_HIERARCHY).hidden();

    addFactHierarchy(
            dimensions,
            versions,
            BRANCH_HIERARCHY,
            FieldPath.of(
                MemoryAnalysisDatastoreDescriptionConfig.CHUNK_TO_VERSION,
                DatastoreConstants.VERSION__BRANCH_NAME),
            "The branch of the chunk",
            "The branch of the chunk")
        .comparator(
            new ComparatorDescription(IComparator.CUSTOM_PLUGIN_KEY)
                .setOrders(
                    List.of(
                        new ComparatorOrderDescription(
                            ComparatorOrderDescriptionNames.FIRST_OBJECTS,
                            List.of(IEpoch.MASTER_BRANCH_NAME)))));

    addFactHierarchy(
        dimensions,
        versions,
        USED_BY_VERSION_DIMENSION,
        DatastoreConstants.CHUNK__USED_BY_VERSION,
        "Whether or not the chunk is known to be used by the currently viewed version");
  }

  private void defineOwnerDimension(final IDimensionsConfigurator dimensions) {
    final String description = "The cube(s) or store(s) owning the chunk";
    final IDimensionConfigurator owners =
        addFactDimension(dimensions, OWNER_DIMENSION, description);
    addFactHierarchy(
        dimensions, owners, OWNER_HIERARCHY, DatastoreConstants.OWNER__OWNER, description);

    owners.addHierarchy(
        ICustomHierarchyDefinition.builder(OWNER_TYPE_HIERARCHY)
            .derived()
            .withAllMember()
            .withLevel(
                OWNER_TYPE_HIERARCHY,
                IDerivedHierarchyInput.create(OWNER_LEVEL),
                reader -> ((ChunkOwner) reader.read(0)).getType())
            .withMembers((Object[]) OwnerType.values())
            .build());
  }

  /**
   * Defines the dimension of the epochs viewing the chunks.
   *
   * <p>A chunk of a given epoch can be viewed by several epochs: the dimension table is joined to
   * the cube with a many-to-many relationship. Such a dimension cannot hold the hierarchies of
   * another table, hence this dimension being separated from the {@link #VERSION_DIMENSION}.
   */
  private void defineEpochDimension(final IDimensionsConfigurator dimensions) {
    final IDimensionConfigurator epochs =
        dimensions.addDimension(
            EPOCH_DIMENSION,
            DatastoreConstants.EPOCH_VIEW_STORE,
            ManyToManyRelationship.builder()
                .withMapping(DatastoreConstants.EPOCH_VIEW__OWNER, OWNER_LEVEL)
                .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, DUMP_NAME_LEVEL)
                .withMapping(DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID, INTERNAL_EPOCH_ID_LEVEL)
                .build());
    addJoinedHierarchy(epochs, EPOCH_ID_HIERARCHY, DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID)
        .level(EPOCH_ID_HIERARCHY)
        .comparator(ReverseEpochViewComparator.PLUGIN_KEY);
  }

  private void defineReferenceDimension(final IDimensionsConfigurator dimensions) {
    addJoinedHierarchy(
        dimensions.addDimension(
            REFERENCE_NAMES_HIERARCHY,
            DatastoreConstants.REFERENCE_STORE,
            componentRelationship(DatastoreConstants.REFERENCE_ID, REFERENCE_ID_LEVEL)),
        REFERENCE_NAMES_HIERARCHY,
        DatastoreConstants.REFERENCE_NAME);
  }

  private void defineIndexDimension(final IDimensionsConfigurator dimensions) {
    final IDimensionConfigurator indices =
        dimensions.addDimension(
            INDEX_DIMENSION,
            DatastoreConstants.INDEX_STORE,
            componentRelationship(DatastoreConstants.INDEX_ID, INDEX_ID_LEVEL));
    addJoinedHierarchy(indices, INDEXED_FIELDS_HIERARCHY, DatastoreConstants.INDEX__FIELDS);
    addJoinedHierarchy(indices, INDEX_TYPE_HIERARCHY, DatastoreConstants.INDEX_TYPE);
  }

  /**
   * Adds a single-level hierarchy to a dimension whose table is joined to the cube with a
   * many-to-many relationship.
   *
   * <p>The unknown member is only contributed when the hierarchy is empty: the facts without any
   * matching row in the dimension table are not expected to be displayed under the unknown member.
   */
  private static IHierarchyConfigurator addJoinedHierarchy(
      final IDimensionConfigurator dimension, final String name, final String field) {
    return dimension
        .addHierarchy(singleLevelHierarchy(name, FieldPath.of(field)))
        .withProperty(
            IHierarchyConfigurator.AUTO_CONTRIBUTE_UNKNOWN_MEMBER_PROPERTY,
            IHierarchyConfigurator.AUTO_CONTRIBUTE_UNKNOWN_MEMBER_IF_EMPTY);
  }

  /**
   * Returns the relationship between the cube and the table of a chunk component (dictionary,
   * index, reference...), for the given import and epoch.
   */
  private static IDimensionRelationship componentRelationship(
      final String componentIdField, final LevelIdentifier componentIdLevel) {
    return ManyToManyRelationship.builder()
        .withMapping(componentIdField, componentIdLevel)
        .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, DUMP_NAME_LEVEL)
        .withMapping(DatastoreConstants.VERSION__EPOCH_ID, INTERNAL_EPOCH_ID_LEVEL)
        .build();
  }

  /** Adds a dimension whose levels are fed by the fact table and the tables it joins. */
  private static IDimensionConfigurator addFactDimension(
      final IDimensionsConfigurator dimensions, final String name, final String description) {
    final IDimensionConfigurator dimension =
        dimensions.addDimension(
            name, DatastoreConstants.CHUNK_STORE, new ManyToOneRelationship(JoinPath.empty()));
    if (description != null) {
      dimension.description(description);
    }
    return dimension;
  }

  /** Adds a single-level hierarchy, of the same name as its level, fed by the fact table. */
  private static ILevelConfigurator addFactHierarchy(
      final IDimensionsConfigurator dimensions,
      final IDimensionConfigurator dimension,
      final String name,
      final String field,
      final String description) {
    return addFactHierarchy(dimensions, dimension, name, FieldPath.of(field), description);
  }

  private static ILevelConfigurator addFactHierarchy(
      final IDimensionsConfigurator dimensions,
      final IDimensionConfigurator dimension,
      final String name,
      final FieldPath field,
      final String description) {
    return addFactHierarchy(dimensions, dimension, name, field, description, description);
  }

  private static ILevelConfigurator addFactHierarchy(
      final IDimensionsConfigurator dimensions,
      final IDimensionConfigurator dimension,
      final String name,
      final FieldPath field,
      final String hierarchyDescription,
      final String levelDescription) {
    dimension.addHierarchy(singleLevelHierarchy(name, field)).description(hierarchyDescription);
    return describe(
        dimensions,
        new LevelIdentifier(dimension.getIdentifier().getName(), name, name),
        levelDescription);
  }

  private static ITableHierarchyDefinition singleLevelHierarchy(
      final String name, final FieldPath field) {
    return ITableHierarchyDefinition.builder(name)
        .withAllMember()
        .withFieldLevel(name, field)
        .build();
  }

  private static ILevelConfigurator describe(
      final IDimensionsConfigurator dimensions,
      final LevelIdentifier level,
      final String description) {
    return dimensions.level(level).description(description);
  }

  // endregion

  // region measures

  private void defineMeasures(final IMeasuresConfigurator measures) {
    measures
        .updateMeasureMetadata(
            IMeasureHierarchy.COUNT_ID,
            metadata -> metadata.folder(INTERNAL_FOLDER).formatter(forAllTypes(NUMBER_FORMATTER)))
        .updateMeasureMetadata(
            IMeasureHierarchy.TIMESTAMP_ID,
            metadata ->
                metadata
                    .folder(INTERNAL_FOLDER)
                    .formatter(forAllTypes(IFormatter.DATE_PLUGIN_KEY + "[HH:mm:ss]")));

    applicationMeasures(measures);
    chunkMeasures(measures);
    dictionaryMeasures(measures);
    vectorMeasures(measures);
  }

  /** Returns the formatter mapping that uses the given formatter, whatever the measure type. */
  private static Map<ContentType, String> forAllTypes(final String formatter) {
    return Arrays.stream(ContentType.values())
        .collect(Collectors.toMap(Function.identity(), type -> formatter));
  }

  private void applicationMeasures(final IMeasuresConfigurator measures) {
    addApplicationMeasure(
        measures,
        USED_HEAP,
        DatastoreConstants.APPLICATION__USED_ON_HEAP,
        "the amount of heap memory used by the application");
    addApplicationMeasure(
        measures,
        COMMITTED_HEAP,
        DatastoreConstants.APPLICATION__MAX_ON_HEAP,
        "the total size of the JVM heap");
    addApplicationMeasure(
        measures,
        USED_DIRECT,
        DatastoreConstants.APPLICATION__USED_OFF_HEAP,
        "the amount of off-heap memory used by the application");
    addApplicationMeasure(
        measures,
        MAX_DIRECT,
        DatastoreConstants.APPLICATION__MAX_OFF_HEAP,
        "the amount of off-heap memory reserved by the application");
  }

  private static void addApplicationMeasure(
      final IMeasuresConfigurator measures,
      final String name,
      final String field,
      final String description) {
    measures.addMeasure(
        name,
        CopperNext.agg(
                SingleValueFunction.PLUGIN_KEY,
                FieldPath.of(MemoryAnalysisDatastoreDescriptionConfig.CHUNK_TO_APP, field))
            .withFormatter(ByteFormatter.KEY)
            .withinFolder(APPLICATION_FOLDER)
            .withDescription(description));
  }

  private void chunkMeasures(final IMeasuresConfigurator measures) {
    measures.addMeasure(
        CHUNK_COUNT,
        perChunkAggregation(Copper.constant(1L))
            .sum()
            .withinFolder(CHUNK_FOLDER)
            .withDescription("the number of contributing chunks"));

    measures.addMeasure(
        HEAP_MEMORY_SUM,
        perChunkAggregation(DatastoreConstants.CHUNK__ON_HEAP_SIZE)
            .sum()
            .withFormatter(ByteFormatter.KEY)
            .withinFolder(CHUNK_MEMORY_FOLDER)
            .withDescription("an estimate of the on-heap size of the chunks"));

    measures.addMeasure(
        CHUNK_SIZE_SUM,
        perChunkAggregation(DatastoreConstants.CHUNK__SIZE)
            .sum()
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .withDescription("a sum aggregation of the chunk sizes"));

    measures.addMeasure(
        NON_WRITTEN_ROWS_COUNT,
        perChunkAggregation(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS)
            .sum()
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .withDescription("the number of unused rows within the chunks"));

    measures.addMeasure(
        DELETED_ROWS_COUNT,
        perChunkAggregation(DatastoreConstants.CHUNK__FREE_ROWS)
            .sum()
            .mapToDouble(a -> a.readDouble(0))
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .withDescription("the number of freed rows within the chunks"));

    measures.addMeasure(
        DELETED_ROWS_RATIO,
        Copper.measure(DELETED_ROWS_COUNT)
            .divide(Copper.measure(CHUNK_SIZE_SUM))
            .withType(Types.TYPE_DOUBLE)
            .withFormatter(PERCENT_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .withDescription("the ratio of freed rows within the chunks"));

    measures.addMeasure(
        PHYSICAL_ROW_COUNT,
        CopperNext.sum(FieldPath.of(DatastoreConstants.CHUNK__SIZE))
            .withDescription("the total number of rows in chunks")
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(CHUNK_FOLDER));

    measures.addMeasure(
        NON_WRITTEN_ROWS_RATIO,
        Copper.measure(NON_WRITTEN_ROWS_COUNT)
            .divide(Copper.measure(PHYSICAL_ROW_COUNT))
            .withType(Types.TYPE_DOUBLE)
            .withFormatter(PERCENT_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .withDescription("the ratio of unused rows within the chunks"));

    measures.addMeasure(
        COMMITTED_ROWS_COUNT,
        Copper.measure(CHUNK_SIZE_SUM)
            .minus(Copper.measure(NON_WRITTEN_ROWS_COUNT))
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .withDescription("the number of committed (i.e. used) rows inside the chunks"));

    measures.addMeasure(
        COMMITTED_ROWS_RATIO,
        Copper.measure(COMMITTED_ROWS_COUNT)
            .divide(Copper.measure(CHUNK_SIZE_SUM))
            .withType(Types.TYPE_DOUBLE)
            .withFormatter(PERCENT_FORMATTER)
            .withinFolder(CHUNK_FOLDER)
            .withDescription("the ratio of committed (i.e. used) rows inside the chunks"));

    measures.addMeasure(
        DIRECT_MEMORY_SUM,
        perChunkAggregation(DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
            .sum()
            .withFormatter(ByteFormatter.KEY)
            .withinFolder(CHUNK_MEMORY_FOLDER)
            .withDescription("the off-heap size of the chunks"));

    final CopperMeasure directMemory = Copper.measure(DIRECT_MEMORY_SUM);
    measures.addMeasure(
        COMMITTED_CHUNK_MEMORY,
        Copper.measure(COMMITTED_ROWS_RATIO)
            .multiply(directMemory)
            .mapToLong(a -> (long) a.readDouble(0))
            .withFormatter(ByteFormatter.KEY)
            .withinFolder(CHUNK_MEMORY_FOLDER)
            .withDescription(
                "the amount of memory in bytes used to store committed rows in chunks"));

    measures.addMeasure(
        DIRECT_MEMORY_RATIO,
        directMemory
            .divide(directMemory.grandTotal())
            .withType(Types.TYPE_DOUBLE)
            .withFormatter(PERCENT_FORMATTER)
            .withinFolder(CHUNK_MEMORY_FOLDER)
            .withDescription(
                "the total ratio of off-heap memory consumed by the chunks relative to the total"
                    + " used chunk memory"));

    measures.addMeasure(
        USED_MEMORY_RATIO,
        directMemory
            .divide(Copper.measure(USED_DIRECT))
            .withType(Types.TYPE_DOUBLE)
            .withFormatter(PERCENT_FORMATTER)
            .withinFolder(CHUNK_MEMORY_FOLDER)
            .withDescription(
                "the total ratio of off-heap memory consumed by the chunks relative to the total"
                    + " used application committed memory"));

    measures.addMeasure(
        MAX_MEMORY_RATIO,
        directMemory
            .divide(Copper.measure(MAX_DIRECT))
            .withType(Types.TYPE_DOUBLE)
            .withFormatter(PERCENT_FORMATTER)
            .withinFolder(CHUNK_MEMORY_FOLDER)
            .withDescription(
                "the total ratio of off-heap memory consumed by the chunks relative to the total"
                    + " application committed memory"));
  }

  private void dictionaryMeasures(final IMeasuresConfigurator measures) {
    final CopperStore chunkToDicoStore =
        Copper.store(DatastoreConstants.DICTIONARY_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.DICTIONARY_ID, Copper.level(DICO_ID_LEVEL))
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, Copper.level(DUMP_NAME_LEVEL))
            .withMapping(
                DatastoreConstants.VERSION__EPOCH_ID, Copper.level(INTERNAL_EPOCH_ID_LEVEL));

    measures.addMeasure(
        DICTIONARY_SIZE,
        Copper.agg(
                chunkToDicoStore.field(DatastoreConstants.DICTIONARY_SIZE),
                SingleValueFunction.PLUGIN_KEY)
            .filter(Copper.level(COMPONENT_LEVEL).eq(ParentType.DICTIONARY))
            .per(
                Copper.level(INTERNAL_EPOCH_ID_LEVEL),
                Copper.level(DUMP_NAME_LEVEL),
                Copper.level(DICO_ID_LEVEL))
            .sum()
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(DICTIONARY_FOLDER)
            .withDescription(
                "the number of entries in the corresponding dictionary, when relevant"));

    measures.addMeasure(
        STORE_CHUNK_SIZE,
        perChunkAggregation(DatastoreConstants.CHUNK__SIZE)
            .max()
            .per(
                Copper.level(OWNER_LEVEL),
                Copper.level(FIELD_LEVEL),
                Copper.level(PARTITION_LEVEL),
                Copper.level(CLASS_LEVEL))
            .min()
            .withFormatter(NUMBER_FORMATTER)
            .withinFolder(STORE_CHUNK_FOLDER)
            .withDescription("the size of each chunk for the store"));
  }

  private void vectorMeasures(final IMeasuresConfigurator measures) {
    measures.addMeasure(
        VECTOR_BLOCK_REFCOUNT,
        perChunkAggregation(DatastoreConstants.CHUNK__VECTOR_BLOCK_REF_COUNT)
            .sum()
            .withinFolder(VECTOR_FOLDER)
            .withDescription(
                "The amount of references held towards the vectors contained in the vector block"
                    + " of the chunks, when relevant"));

    measures.addMeasure(
        VECTOR_BLOCK_SIZE,
        perChunkAggregation(DatastoreConstants.CHUNK__VECTOR_BLOCK_LENGTH)
            .max()
            // The underlying vector block length might change from one vector to another
            // depending on how the Aggregate Provider computes the vector measure, since this
            // measure does not give as much information as the others, we decide to only show the
            // maximum length.
            .per(Copper.level(FIELD_LEVEL))
            .doNotAggregateAbove()
            .withinFolder(VECTOR_FOLDER)
            .withDescription("the length of the Block holding the vector data, when relevant"));
  }

  private static CopperMeasureToAggregateAbove perChunkAggregation(final String fieldName) {
    return perChunkAggregation(
        CopperNext.agg(SingleValueFunction.PLUGIN_KEY, FieldPath.of(fieldName)));
  }

  private static CopperMeasureToAggregateAbove perChunkAggregation(final CopperMeasure measure) {
    return measure.per(Copper.level(CHUNK_ID_LEVEL), Copper.level(DUMP_NAME_LEVEL));
  }

  // endregion
}
