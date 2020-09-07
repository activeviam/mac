/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.builders.StartBuilding;
import com.activeviam.copper.ICopperContext;
import com.activeviam.copper.api.Copper;
import com.activeviam.copper.api.CopperElement;
import com.activeviam.copper.api.CopperHierarchy;
import com.activeviam.copper.api.CopperMeasure;
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
import com.qfs.agg.impl.DistinctCountFunction;
import com.qfs.agg.impl.SingleValueFunction;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.ILiteralType;
import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.quartetfs.biz.pivot.context.impl.QueriesTimeLimit;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.ISelectionDescription;
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
  /** The index-related cube. */
  public static final String INDEX_CUBE = "Indexes";
  /** The dictionary related cube. */
  public static final String DICTIONARY_CUBE = "Dictionaries";
  /** The provider related cube. */
  public static final String PROVIDER_CUBE = "Providers";
  /** The references related Cube. */
  public static final String REFERENCE_CUBE = "References";

  /** Measure of the summed off-heap memory footprint. */
  public static final String DIRECT_MEMORY_SUM = "DirectMemory.SUM";
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

  /** Name of the DictionaryID AH. */
  public static final String DICO_ID_HIERARCHY = "Dictionary ID";

  /** Name of the DictionaryID AH. */
  public static final String INDEX_ID_HIERARCHY = "Index ID";

  /** Name of the ReferenceID AH. */
  public static final String REF_ID_HIERARCHY = "Reference ID";

  /** Name of the component dimension. */
  public static final String COMPONENT_DIMENSION = "Components";
  /** Name of the component analysis hierarchy. */
  public static final String COMPONENT_HIERARCHY = "Component";
  /** Name of the component dimension. */
  public static final String OWNER_DIMENSION = "Owners";
  /** Name of the component analysis hierarchy. */
  public static final String OWNER_HIERARCHY = "Owner";
  /** Name of the component dimension. */
  public static final String FIELD_DIMENSION = "Fields";
  /** Name of the component analysis hierarchy. */
  public static final String FIELD_HIERARCHY = "Field";

  /** Name of the component analysis hierarchy. */
  public static final String SHARED_HIERARCHY = "Shared";

  /** Total on-heap memory footprint of the application. */
  public static final String USED_HEAP = "UsedHeapMemory";
  /** Total on-heap memory committed by the JVM. */
  public static final String COMMITTED_HEAP = "CommittedHeapMemory";
  /** Total off-heap memory footprint of the application. */
  public static final String USED_DIRECT = "UsedDirectMemory";
  /** Total off-heap memory committed by the JVM. */
  public static final String MAX_DIRECT = "MaxDirectMemory";

  /** Measure of the size of Chunks (in Bytes). */
  public static final String CHUNK_SIZE_SUM = "ChunkSize.SUM";

  /** Measure of the the non written rows in Chunks. */
  public static final String NON_WRITTEN_ROWS_COUNT = "NonWrittenRows.COUNT";

  /** Measure of the deleted rows in Chunks. */
  public static final String DELETED_ROWS_COUNT = "DeletedRows.COUNT";

  /** Formatter for Numbers. */
  public static final String NUMBER_FORMATTER = NumberFormatter.TYPE + "[#,###]";
  /** Formatter for Percentages. */
  public static final String PERCENT_FORMATTER = NumberFormatter.TYPE + "[#.##%]";

  /** The name of the hierarchy of indexed fields. */
  public static final String INDEXED_FIELDS_HIERARCHY = "Indexed Fields";
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
  public static final String OWNER_COMPONENT_HIERARCHY = "Owner component";
  /** The name of the hierarchy of chunk ids. */
  public static final String CHUNK_ID_HIERARCHY = "ChunkId";
  /** The name of the hierarchy of partitions. */
  public static final String PARTITION_HIERARCHY = "Partition";

  /** The name of the folder for measures related to chunk ownership. */
  public static final String OWNERSHIP_FOLDER = "Ownership";
  /** The name of the folder for measures related to memory metrics. */
  public static final String MEMORY_FOLDER = "Memory";

  protected CopperStore chunkToOwnerStore;
  protected CopperHierarchy ownerHierarchy;
  protected CopperHierarchy componentHierarchy;
  protected CopperHierarchy fieldHierarchy;

  @Bean
  @Override
  public IActivePivotManagerDescription userManagerDescription() {
    return StartBuilding.managerDescription()
        .withSchema("MemorySchema")
        .withSelection(memorySelection())
        .withCube(memoryCube())
        .build();
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
        .withPropertyName(
            prefixField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__CLASS))
        .withFormatter(ClassFormatter.KEY)
        .withProperty("description", "Class of the chunks")
        .withDimension("Chunk Owners")
        .withHierarchy(OWNER_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__OWNER)
        .withLevel(PARTITION_HIERARCHY)
        .withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)
        .withFormatter(PartitionIdFormatter.KEY)
        .withHierarchy(OWNER_COMPONENT_HIERARCHY)
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__COMPONENT)
        .withDimension(CHUNK_DUMP_NAME_LEVEL)
        .withHierarchyOfSameName()
        .slicing()
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.CHUNK__DUMP_NAME)
        .withComparator(ReverseOrderComparator.type)
        .withHierarchy("Date")
        .withLevelOfSameName()
        .withPropertyName(DatastoreConstants.APPLICATION__DATE)
        .withType(ILevelInfo.LevelType.TIME)
        .withComparator(ReverseOrderComparator.type)
        .withProperty("description", "Date at which statistics were retrieved")
        .withDimension("Aggregate Provider")
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
        .withPropertyName(DatastoreConstants.CHUNK__PROVIDER_ID);
  }

  private IHasAtLeastOneMeasure measures(ICanStartBuildingMeasures builder) {
    return builder
        .withContributorsCount()
        .withAlias("Chunks.COUNT")
        .withFormatter(NUMBER_FORMATTER)
        .withUpdateTimestamp()
        .withFormatter(DateFormatter.TYPE + "[HH:mm:ss]");
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
    // 1- Chunk to Dicos
    CopperStore chunkToDicoStore =
        Copper.store(DatastoreConstants.DICTIONARY_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.DICTIONARY_ID, CHUNK_DICO_ID_LEVEL)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL);

    Copper.sum(chunkToDicoStore.field(DatastoreConstants.DICTIONARY_SIZE))
        .as("Dictionary Size")
        .withFormatter(NUMBER_FORMATTER)
        .publish(context);

    // --------------------
    // 2- Chunk to references
    CopperStore chunkToReferenceStore =
        Copper.store(DatastoreConstants.REFERENCE_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.REFERENCE_ID, CHUNK_REF_ID_LEVEL)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL);

    // Reference name
    Copper.newSingleLevelHierarchy(REFERENCE_NAMES_HIERARCHY)
        .from(chunkToReferenceStore.field(DatastoreConstants.REFERENCE_NAME))
        .publish(context);

    // --------------------
    // 3- Chunk to indexes
    CopperStore chunkToIndexStore =
        Copper.store(DatastoreConstants.INDEX_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.INDEX_ID, CHUNK_INDEX_ID_LEVEL)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL);

    Copper.newSingleLevelHierarchy(INDEXED_FIELDS_HIERARCHY)
        .from(chunkToIndexStore.field(DatastoreConstants.INDEX__FIELDS))
        .publish(context);

    // --------------------
    // 4- Chunk to owners
    CopperStore chunkToOwnerStore =
        Copper.store(DatastoreConstants.OWNER_STORE)
            .joinToCube()
            .withMapping(DatastoreConstants.OWNER__CHUNK_ID, CHUNK_ID_HIERARCHY)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, CHUNK_DUMP_NAME_LEVEL);

    ownerHierarchy =
        Copper.newSingleLevelHierarchy(OWNER_DIMENSION, OWNER_HIERARCHY, OWNER_HIERARCHY)
            .from(chunkToOwnerStore.field(DatastoreConstants.OWNER__OWNER))
            .publish(context);

    componentHierarchy = Copper
        .newSingleLevelHierarchy(COMPONENT_DIMENSION, COMPONENT_HIERARCHY, COMPONENT_HIERARCHY)
        .from(chunkToOwnerStore.field(DatastoreConstants.OWNER__COMPONENT))
        .publish(context);

    fieldHierarchy =
        Copper.newSingleLevelHierarchy(FIELD_DIMENSION, FIELD_HIERARCHY, FIELD_HIERARCHY)
            .from(chunkToOwnerStore.field(DatastoreConstants.OWNER__FIELD))
            .publish(context);
  }

  private void chunkMeasures(final ICopperContext context) {
    final var directMemory =
        Copper.sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
            .as(DIRECT_MEMORY_SUM)
            .withFormatter(ByteFormatter.KEY)
            .publish(context);

    final CopperMeasure ownerMeasure =
        Copper.agg(chunkToOwnerStore.field(DatastoreConstants.OWNER__OWNER),
            DistinctCountFunction.PLUGIN_KEY)
            .totalOn(ownerHierarchy)
            .per(Copper.level(CHUNK_ID_HIERARCHY))
            .max();

    final CopperMeasure componentMeasure =
        Copper.agg(chunkToOwnerStore.field(DatastoreConstants.OWNER__COMPONENT),
            DistinctCountFunction.PLUGIN_KEY)
            .totalOn(componentHierarchy)
            .per(Copper.level(CHUNK_ID_HIERARCHY))
            .max();

    Copper.newSingleLevelHierarchy(COMPONENT_DIMENSION, SHARED_HIERARCHY, SHARED_HIERARCHY)
        .from(Copper.combine((CopperElement) componentMeasure)
            .map(a -> (long) a.read(0) > 1L ? "Shared Component" : "Exclusive Component"))
        .withMemberList("Shared Component", "Exclusive Component")
        .publish(context);

    final CopperMeasure fieldMeasure =
        Copper.agg(chunkToOwnerStore.field(DatastoreConstants.OWNER__FIELD),
            DistinctCountFunction.PLUGIN_KEY)
            .totalOn(fieldHierarchy)
            .per(Copper.level(CHUNK_ID_HIERARCHY))
            .max();

    Copper.combine(ownerMeasure, directMemory)
        .map(reader -> reader.readLong(0) > 1L ? 0L : reader.readLong(1))
        .per(Copper.level(CHUNK_ID_HIERARCHY))
        .sum()
        .map(Double::longValue)
        .withType(ILiteralType.LONG)
        .as(DIRECT_MEMORY_SUM + ".exclusiveOwner")
        .withinFolder(MEMORY_FOLDER)
        .withFormatter(ByteFormatter.KEY)
        .publish(context);

    Copper.combine(componentMeasure, directMemory)
        .map(reader -> reader.readLong(0) > 1L ? 0L : reader.readLong(1))
        .per(Copper.level(CHUNK_ID_HIERARCHY))
        .sum()
        .map(Double::longValue)
        .withType(ILiteralType.LONG)
        .as(DIRECT_MEMORY_SUM + ".exclusiveComponent")
        .withinFolder(MEMORY_FOLDER)
        .withFormatter(ByteFormatter.KEY)
        .publish(context);

    Copper.combine(fieldMeasure, directMemory)
        .map(reader -> {
          if (reader.isNull(0)) {
            return null;
          }

          if (reader.readLong(0) > 1L) {
            return 0L;
          }

          return reader.readLong(1);
        })
        .per(Copper.level(CHUNK_ID_HIERARCHY))
        .sum()
        .map(Double::longValue)
        .withType(ILiteralType.LONG)
        .as(DIRECT_MEMORY_SUM + ".exclusiveField")
        .withinFolder(MEMORY_FOLDER)
        .withFormatter(ByteFormatter.KEY)
        .publish(context);

    Copper.sum(DatastoreConstants.CHUNK__ON_HEAP_SIZE)
        .as(HEAP_MEMORY_SUM)
        .withFormatter(ByteFormatter.KEY)
        .publish(context);

    final var chunkSize =
        Copper.sum(DatastoreConstants.CHUNK__SIZE)
            .as(CHUNK_SIZE_SUM)
            .withFormatter(ByteFormatter.KEY)
            .publish(context);

    Copper.sum(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS)
        .as(NON_WRITTEN_ROWS_COUNT)
        .withFormatter(NUMBER_FORMATTER)
        .publish(context);

    Copper.sum(DatastoreConstants.CHUNK__FREE_ROWS)
        .withFormatter(NUMBER_FORMATTER)
        .as(DELETED_ROWS_COUNT)
        .withType(ILiteralType.DOUBLE)
        .divide(chunkSize)
        .withFormatter(PERCENT_FORMATTER)
        .as("DeletedRows.Ratio")
        .publish(context);

    Copper.sum(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS)
        .withFormatter(NUMBER_FORMATTER)
        .as(NON_WRITTEN_ROWS_COUNT)
        .withType(ILiteralType.DOUBLE)
        .divide(chunkSize)
        .withFormatter(PERCENT_FORMATTER)
        .as("NonWrittenRows.Ratio")
        .publish(context);

    chunkSize
        .minus(Copper.sum(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS))
        .withFormatter(NUMBER_FORMATTER)
        .withType(ILiteralType.DOUBLE) // Overflow happens if we don't cast it to double
        .as("CommittedRows")
        .publish(context);

    Copper.measure("CommittedRows")
        .divide(chunkSize)
        .multiply(directMemory)
        .withType(ILiteralType.LONG)
        .withFormatter(ByteFormatter.KEY)
        .as("CommittedChunk")
        .publish(context);

    Copper.measure("CommittedRows")
        .divide(chunkSize)
        .withFormatter(PERCENT_FORMATTER)
        .as("CommittedMemory.Ratio")
        .publish(context);

    directMemory
        .withType(ILiteralType.DOUBLE)
        .divide(directMemory.grandTotal())
        .withFormatter(PERCENT_FORMATTER)
        .as("DirectMemory.Ratio")
        .publish(context);

    directMemory
        .withType(ILiteralType.DOUBLE)
        .divide(Copper.measure(USED_DIRECT))
        .withFormatter(PERCENT_FORMATTER)
        .as("UsedMemory.Ratio")
        .publish(context);

    directMemory
        .withType(ILiteralType.DOUBLE)
        .divide(Copper.measure(MAX_DIRECT))
        .withFormatter(PERCENT_FORMATTER)
        .as("MaxMemory.Ratio")
        .publish(context);

    Copper.sum(DatastoreConstants.CHUNK__VECTOR_BLOCK_REF_COUNT)
        .per(Copper.hierarchy(FIELD_HIERARCHY).level(FIELD_HIERARCHY))
        .doNotAggregateAbove()
        .as("VectorBlock.RefCount")
        .publish(context);

    Copper.max(DatastoreConstants.CHUNK__VECTOR_BLOCK_LENGTH)
        .per(Copper.level(FIELD_HIERARCHY))
        .doNotAggregateAbove()
        .as("VectorBlock.Length")
        .publish(context);
  }

  private void applicationMeasure(final ICopperContext context) {}
}
