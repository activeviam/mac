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
import com.google.common.base.Objects;
import com.qfs.agg.impl.SingleValueFunction;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.qfs.store.Types;
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
 * @author Quartet FS
 */
@Configuration
public class ManagerDescriptionConfig implements IActivePivotManagerDescriptionConfig {

	/**
	 * The main monitoring cube
	 *
	 * <p>This Cube is based from Chunk facts
	 */
	public static final String MONITORING_CUBE = "MemoryCube";
	/**
	 * The index-related cube
	 */
	public static final String INDEX_CUBE = "Indexes";
	/**
	 * The dictionary related cube
	 */
	public static final String DICTIONARY_CUBE = "Dictionaries";
	/**
	 * The provider related cube
	 */
	public static final String PROVIDER_CUBE = "Providers";
	/**
	 * The references related Cube
	 */
	public static final String REFERENCE_CUBE = "References";

	/**
	 * Measure of the summed off-heap memory footprint
	 */
	public static final String DIRECT_MEMORY_SUM = "DirectMemory.SUM";
	/**
	 * Measure of the summed on-heap memory footprint
	 *
	 * <p>This measure only sums the on-heap memory held by chunk, and therefore do not contains the
	 * entire on-heap footprint of the entire ActivePivot Application
	 */
	public static final String HEAP_MEMORY_SUM = "HeapMemory.SUM";

	/**
	 * Java class of the chunk
	 */
	public static final String CHUNK_CLASS_LEVEL = "Class";
	/**
	 * Type of the structure owning the chunk
	 */
	public static final String CHUNK_TYPE_LEVEL = "Type";
	/**
	 * Measure counting the number of off-Heap Chunks
	 */
	public static final String DIRECT_CHUNKS_COUNT = "DirectChunks.COUNT";
	/**
	 * Measure counting the number of on-Heap Chunks
	 */
	public static final String HEAP_CHUNKS_COUNT = "HeapChunks.COUNT";

	/**
	 * Name of the Chunk Hierarchy
	 */
	public static final String CHUNK_HIERARCHY = "Chunks";

	/**
	 * Total on-heap memory footprint of the application
	 */
	public static final String USED_HEAP = "UsedHeapMemory";
	/**
	 * Total on-heap memory committed by the JVM
	 */
	public static final String COMMITTED_HEAP = "CommittedHeapMemory";
	/**
	 * Total off-heap memory footprint of the application
	 */
	public static final String USED_DIRECT = "UsedDirectMemory";
	/**
	 * Total off-heap memory committed by the JVM
	 */
	public static final String MAX_DIRECT = "MaxDirectMemory";

	/**
	 * Name of the folder in which "hidden" copper measures will be hidden
	 *
	 * <p>Measures contained in this folder should never be used
	 */
	public static final String BLACK_MAGIC_FOLDER = "BlackMagic";
	/**
	 * Name of the hierarchy used to perform join
	 */
	public static final String BLACK_MAGIC_HIERARCHY = "BlackMagic";

	/**
	 * Measure of the size of Chunks (in Bytes)
	 */
	public static final String CHUNK_SIZE_SUM = "ChunkSize.SUM";

	/**
	 * Measure of the the non written rows in Chunks
	 */
	public static final String NON_WRITTEN_ROWS_COUNT = "NonWrittenRows.COUNT";

	/**
	 * Measure of the deleted rows in Chunks
	 */
	public static final String DELETED_ROWS_COUNT = "DeletedRows.COUNT";

	/**
	 * Formatter for Numbers
	 */
	public static final String NUMBER_FORMATTER = NumberFormatter.TYPE + "[#,###]";
	/**
	 * Formatter for Percentages
	 */
	public static final String PERCENT_FORMATTER = NumberFormatter.TYPE + "[#.##%]";

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

	private ISelectionDescription referenceSelection() {
		return StartBuilding.selection(this.userSchemaDescription())
				.fromBaseStore(DatastoreConstants.REFERENCE_STORE)
				.withAllFields()
				.build();
	}

	private IActivePivotInstanceDescription referenceCube() {
		return StartBuilding.cube(REFERENCE_CUBE)
				.withDimension("Reference Id")
				.withHierarchyOfSameName()
				.withLevel(DatastoreConstants.REFERENCE_ID)
				.withDimension("Data feed")
				.withHierarchyOfSameName()
				.slicing()
				.withLevel(DatastoreConstants.APPLICATION__DUMP_NAME)
				.withSingleLevelDimension(DatastoreConstants.REFERENCE_CLASS)
				.withSingleLevelDimension(DatastoreConstants.REFERENCE_NAME)
				.withDimension("Base stores")
				.withHierarchyOfSameName()
				.withLevel(DatastoreConstants.REFERENCE_FROM_STORE)
				.withLevel(DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID)
				.withDimension("Target stores")
				.withHierarchyOfSameName()
				.withLevel(DatastoreConstants.REFERENCE_TO_STORE)
				.withLevel(DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID)
				.build();
	}

	private ISelectionDescription indexSelection() {
		return StartBuilding.selection(this.userSchemaDescription())
				.fromBaseStore(DatastoreConstants.INDEX_STORE)
				.withAllFields()
				.build();
	}

	private IActivePivotInstanceDescription indexCube() {
		return StartBuilding.cube(INDEX_CUBE)
				.withDimension("Index Id")
				.withHierarchyOfSameName()
				.withLevel(DatastoreConstants.INDEX_ID)
				.withDimension("Data feed")
				.withHierarchyOfSameName()
				.slicing()
				.withLevel(DatastoreConstants.APPLICATION__DUMP_NAME)
				.withDimension("Index Type")
				.withHierarchyOfSameName()
				.withLevel(DatastoreConstants.INDEX_TYPE)
				.withLevel(DatastoreConstants.INDEX_CLASS)
				.withDimension("Indexed Fields")
				.withHierarchyOfSameName()
				.withLevel(DatastoreConstants.INDEX__FIELDS)
				.build();
	}

	private ISelectionDescription dictionarySelection() {
		return StartBuilding.selection(this.userSchemaDescription())
				.fromBaseStore(DatastoreConstants.DICTIONARY_STORE)
				.withAllFields()
				.build();
	}

	private IActivePivotInstanceDescription dictionaryCube() {
		return StartBuilding.cube(DICTIONARY_CUBE)
				.withAggregatedMeasure()
				.sum(DatastoreConstants.DICTIONARY_SIZE)
				.withDimension("Dictionary Id")
				.withHierarchyOfSameName()
				.withLevel(DatastoreConstants.DICTIONARY_ID)
				.withDimension("Data feed")
				.withHierarchyOfSameName()
				.slicing()
				.withLevel(DatastoreConstants.APPLICATION__DUMP_NAME)
				.withDimension("Dictionary classes")
				.withHierarchyOfSameName()
				.withLevel(DatastoreConstants.DICTIONARY_CLASS)
				.build();
	}

	private ISelectionDescription providerSelection() {
		return StartBuilding.selection(this.userSchemaDescription())
				.fromBaseStore(DatastoreConstants.PROVIDER_STORE)
				.withAllFields()
				.build();
	}

	private IActivePivotInstanceDescription providerCube() {
		return StartBuilding.cube(PROVIDER_CUBE)
				.withDimension("Providers")
				.withHierarchyOfSameName()
				.withLevel(DatastoreConstants.PROVIDER__MANAGER_ID)
				.withLevel(DatastoreConstants.PROVIDER__PIVOT_ID)
				.withDimension("Data feed")
				.withHierarchyOfSameName()
				.slicing()
				.withLevel(DatastoreConstants.APPLICATION__DUMP_NAME)
				.withDimension("Provider indexes")
				.withSingleLevelHierarchy(DatastoreConstants.PROVIDER__INDEX)
				.withDimension("Provider Type")
				.withSingleLevelHierarchy(DatastoreConstants.PROVIDER__TYPE)
				.withDimension("Provider category")
				.withSingleLevelHierarchy(DatastoreConstants.PROVIDER__CATEGORY)
				.build();
	}

	/**
	 * Prefixes a field by another string
	 *
	 * @param prefix string to prepend
	 * @param field  field to be prefixed
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
				.withDimension(CHUNK_HIERARCHY)
				.withHierarchy(CHUNK_TYPE_LEVEL)
				.withLevelOfSameName()
				.withPropertyName(DatastoreConstants.CHUNK__PARENT_TYPE)
				.withProperty("description", "What are chunks for")

				.withHierarchy(CHUNK_CLASS_LEVEL)
				.withLevelOfSameName()
				.withPropertyName(
						prefixField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__CLASS))
				.withFormatter(ClassFormatter.KEY)
				.withProperty("description", "Class of the chunks")

				.withDimension("Chunk Owners")
				.withHierarchy("Owner")
				.withLevelOfSameName()
				.withPropertyName(DatastoreConstants.CHUNK__OWNER)
				.withLevel("Partition")
				.withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)
				.withFormatter(PartitionIdFormatter.KEY)

				.withHierarchy("Owner component")
				.withLevelOfSameName()
				.withPropertyName(DatastoreConstants.CHUNK__COMPONENT)


				.withDimension("Import info")
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
				.withHierarchy("Manager")
				.withLevelOfSameName()
				.withPropertyName(DatastoreConstants.PIVOT__MANAGER_ID)
				.withHierarchy("Pivot")
				.withLevelOfSameName()
				.withPropertyName(DatastoreConstants.PIVOT__PIVOT_ID)
				.withHierarchy("ProviderType")
				.withLevelOfSameName()
				.withPropertyName(DatastoreConstants.PROVIDER_COMPONENT__TYPE)

				.withHierarchy("ProviderPartition")
				.withLevelOfSameName()
				.withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)
				.withHierarchy("ProviderId")
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
		memoryMeasures(context);
		chunkMeasures(context);
		applicationMeasure(context);
	}

	private void memoryMeasures(final ICopperContext context) {
	}

	private void chunkMeasures(final ICopperContext context) {
	}

	private void applicationMeasure(final ICopperContext context) {
	}

}
