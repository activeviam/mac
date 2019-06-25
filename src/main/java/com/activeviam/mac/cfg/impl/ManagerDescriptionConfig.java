/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.impl;

import static com.activeviam.copper.columns.Columns.col;
import static com.activeviam.copper.columns.Columns.customAgg;
import static com.activeviam.copper.columns.Columns.sum;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.activeviam.builders.StartBuilding;
import com.activeviam.copper.builders.BuildingContext;
import com.activeviam.copper.builders.dataset.Datasets.StoreDataset;
import com.activeviam.copper.columns.Columns;
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
import com.qfs.agg.impl.CopyFunction;
import com.qfs.agg.impl.SingleValueFunction;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.literal.impl.LiteralType;
import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;
import com.qfs.store.Types;
import com.quartetfs.biz.pivot.context.impl.QueriesTimeLimit;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.ISelectionDescription;
import com.quartetfs.fwk.format.impl.DateFormatter;
import com.quartetfs.fwk.format.impl.NumberFormatter;
import com.quartetfs.fwk.ordering.impl.ReverseOrderComparator;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Quartet FS
 *
 */
@Configuration
public class ManagerDescriptionConfig implements IActivePivotManagerDescriptionConfig {

	/**
	 * The main monitoring cube
	 * <p>
	 * This Cube is based from Chunk facts
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
	 * <p>
	 * This measure only sums the on-heap memory held by chunk, and therefore do not
	 * contains the entire on-heap footprint of the entire ActivePivot Application
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
	 * <p>
	 * Measures contained in this folder should never be used
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


	/** The datastore schema {@link IDatastoreSchemaDescription description}.  */
	@Autowired
	private IDatastoreDescriptionConfig datastoreDescriptionConfig;
	@Bean
	@Override
	public IActivePivotManagerDescription managerDescription() {
		return StartBuilding.managerDescription()
				.withCatalog("Memory Analysis").containingCubes(MONITORING_CUBE)
				.withCatalog("Additional Data").containingCubes(INDEX_CUBE, DICTIONARY_CUBE, PROVIDER_CUBE, REFERENCE_CUBE)
				.withSchema("MemorySchema")
					.withSelection(memorySelection())
					.withCube(memoryCube())
				.withSchema("References Schema")
				.withSelection(referenceSelection())
					.withCube(referenceCube())
				.withSchema("IndexSchema")
				.withSelection(indexSelection())
				.withCube(indexCube())
				.withSchema("DictionarySchema")
				.withSelection(dictionarySelection())
				.withCube(dictionaryCube())
				.withSchema("Provider Schema")
				.withSelection(providerSelection())
				.withCube(providerCube())
				.build();
	}

	private ISelectionDescription referenceSelection() {
		return StartBuilding.selection(this.datastoreDescriptionConfig.schemaDescription())
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
		return StartBuilding.selection(this.datastoreDescriptionConfig.schemaDescription())
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
		return StartBuilding.selection(this.datastoreDescriptionConfig.schemaDescription())
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
		return StartBuilding.selection(this.datastoreDescriptionConfig.schemaDescription())
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
	 * @param prefix string to prepend
	 * @param field field to be prefixed
	 * @return the prefixed string
	 */
	protected static String prefixField(String prefix, String field) {
		return prefix + field.substring(0, 1).toUpperCase() + field.substring(1);
	}

	private ISelectionDescription memorySelection() {
		return StartBuilding.selection(this.datastoreDescriptionConfig.schemaDescription())
				.fromBaseStore(DatastoreConstants.CHUNK_STORE)
				.withAllReachableFields(allReachableFields -> {
					allReachableFields.remove(DatastoreConstants.CHUNK__CLASS);

					final Map<String, String> result = ISelectionDescriptionBuilder.FieldsCollisionHandler.CLOSEST.handle(allReachableFields);
					result.put(prefixField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__CLASS), DatastoreConstants.CHUNK__CLASS);

					return result;
				})
				.build();
	}

	private IActivePivotInstanceDescription memoryCube() {
		return StartBuilding.cube(MONITORING_CUBE)
				.withMeasures(this::measures)

				.withDimensions(this::defineDimensions)

				.withDescriptionPostProcessor(
						StartBuilding
								.copperCalculations()
								.withDefinition(this::copperCalculations)
								.build())

				.withSharedContextValue(QueriesTimeLimit.of(15, TimeUnit.SECONDS))

				.build();
	}

	private ICanBuildCubeDescription<IActivePivotInstanceDescription> defineDimensions(
			final ICanStartBuildingDimensions builder) {
		return builder

				// FROM ChunkStore
				.withDimension(CHUNK_HIERARCHY)

				.withHierarchy("ChunkId")
				.withLevelOfSameName()
				.withPropertyName(DatastoreConstants.CHUNK_ID)

				.withHierarchy(CHUNK_TYPE_LEVEL)
				.withLevelOfSameName()
				.withPropertyName(DatastoreConstants.CHUNK__PARENT_TYPE)
				.withProperty("description", "What are chunks for")

				.withHierarchy(CHUNK_CLASS_LEVEL)
				.withLevelOfSameName()
				.withPropertyName(prefixField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__CLASS))
				.withFormatter(ClassFormatter.KEY)
				.withProperty("description", "Class of the chunks")

				.withSingleLevelDimension("Date").withPropertyName(DatastoreConstants.APPLICATION__DATE)
				.withType(ILevelInfo.LevelType.TIME)
				.withComparator(ReverseOrderComparator.type)
				.withProperty("description", "Date at which statistics were retrieved")

				.withDimension("Owners")
				.withHierarchy("Owner")
					.withLevelOfSameName()
					.withPropertyName(DatastoreConstants.CHUNK__OWNER)
				.withHierarchy("Component")
					.withLevelOfSameName()
					.withPropertyName(DatastoreConstants.CHUNK__COMPONENT)
				.withHierarchy("Partition")
					.withLevelOfSameName()
					.withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)
					.withFormatter(PartitionIdFormatter.KEY)

				.withDimension("Imported Data")
				.withHierarchyOfSameName()
				.slicing()
				.withLevelOfSameName().withPropertyName(DatastoreConstants.CHUNK__DUMP_NAME)
				.withComparator(ReverseOrderComparator.type)

				.withDimension("Aggregate Provider")
				.withHierarchy("Manager").withLevelOfSameName().withPropertyName(DatastoreConstants.PIVOT__MANAGER_ID)
				.withHierarchy("Pivot").withLevelOfSameName().withPropertyName(DatastoreConstants.PIVOT__PIVOT_ID)
				.withHierarchy("ProviderType").withLevelOfSameName().withPropertyName(DatastoreConstants.PROVIDER_COMPONENT__TYPE)
//				.withHierarchy("ProviderClass").withLevelOfSameName().withPropertyName(DatastoreConstants.PROVIDER_COMPONENT__CLASS)
				.withHierarchy("ProviderPartition").withLevelOfSameName().withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)
				.withHierarchy("ProviderId").withLevelOfSameName().withPropertyName(DatastoreConstants.CHUNK__PROVIDER_ID)

				.withDimension(BLACK_MAGIC_HIERARCHY)
				.withHierarchy("ParentId").withLevelOfSameName().withPropertyName(DatastoreConstants.CHUNK__PARENT_ID)
				.withHierarchy("ChunkId").withLevelOfSameName().withPropertyName(DatastoreConstants.CHUNK_ID)
				;
	}

	private IHasAtLeastOneMeasure measures(ICanStartBuildingMeasures builder) {
		return builder
				.withContributorsCount()
				.withAlias("Chunks.COUNT")
				.withFormatter(NUMBER_FORMATTER)
				.withUpdateTimestamp()
				.withFormatter(DateFormatter.TYPE + "[HH:mm:ss]");

	}

	private void copperCalculations(final BuildingContext context) {
		memoryMeasures(context);
		chunkMeasures(context);
		applicationMeasure(context);
		joinHierarchies(context);

	}


	private void memoryMeasures(final BuildingContext context) {
		context.withFormatter(ByteFormatter.KEY)
				.createDatasetFromFacts()
				.agg(
						sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE).as(DIRECT_MEMORY_SUM),
						sum(DatastoreConstants.CHUNK__ON_HEAP_SIZE).as(HEAP_MEMORY_SUM))
				.publish();

		// TODO(ope) may need to optimize this as we don't need to go to chunkId to just get the count
		context.withFormatter(NUMBER_FORMATTER)
				.createDatasetFromFacts()
				.filter(
						Columns.combine(
								col(DatastoreConstants.CHUNK__OWNER)
								,col(DatastoreConstants.CHUNK__COMPONENT)
								,col(DatastoreConstants.CHUNK__PARTITION_ID))
						.map(arr ->
							(Objects.equal(arr.read(0), MemoryAnalysisDatastoreDescription.SHARED_OWNER) ||
							Objects.equal(arr.read(1), MemoryAnalysisDatastoreDescription.SHARED_COMPONENT) ||
							Objects.equal(arr.read(2), MemoryAnalysisDatastoreDescription.MANY_PARTITIONS)? 1 : 0))
						.mapToBoolean(a-> a.equals(1) ? true : false))
				.agg(
						Columns.count(DatastoreConstants.CHUNK_ID).as("Shared.COUNT"))
				.publish();
	}

	private void chunkMeasures(final BuildingContext context) {
		context.withFormatter(NUMBER_FORMATTER)
				.createDatasetFromFacts()
				.groupBy(DatastoreConstants.CHUNK_ID)
				.agg(
					Columns.count(DatastoreConstants.CHUNK_ID).as("cc"),
					sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE).as("ofh"))
				.withColumn(
						DIRECT_CHUNKS_COUNT,
						Columns.combine(col("cc"), col("ofh"))
								.map(reader -> {
									final long ofh = reader.readLong(1);
									return ofh > 0 ? reader.read(0) : 0L;
								})
								.cast(Types.TYPE_LONG))
				.withColumn(
						HEAP_CHUNKS_COUNT,
						col("cc").minus(col(DIRECT_CHUNKS_COUNT)))
				.agg(
						sum(DIRECT_CHUNKS_COUNT).as(DIRECT_CHUNKS_COUNT),
						sum(HEAP_CHUNKS_COUNT).as(HEAP_CHUNKS_COUNT))
				.publish();

		context.withinFolder("Chunks")
				.createDatasetFromFacts()
				.withColumn(CHUNK_SIZE_SUM, sum(DatastoreConstants.CHUNK__SIZE))
				.withColumn(NON_WRITTEN_ROWS_COUNT, sum(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS))
				.withColumn(DELETED_ROWS_COUNT, sum(DatastoreConstants.CHUNK__FREE_ROWS))
				.agg(
						sum(CHUNK_SIZE_SUM).withFormatter(NUMBER_FORMATTER).as(CHUNK_SIZE_SUM),
						sum(NON_WRITTEN_ROWS_COUNT).withFormatter(NUMBER_FORMATTER).as(NON_WRITTEN_ROWS_COUNT),
						sum(DELETED_ROWS_COUNT).withFormatter(NUMBER_FORMATTER).as(DELETED_ROWS_COUNT))
				.withColumn(
						"NonWrittenRows.Ratio",
						col(NON_WRITTEN_ROWS_COUNT).cast(LiteralType.DOUBLE).divide(col(CHUNK_SIZE_SUM))
								.withFormatter(PERCENT_FORMATTER))
				.withColumn(
						"DeletedRows.Ratio",
						col(DELETED_ROWS_COUNT).cast(LiteralType.DOUBLE).divide(col(CHUNK_SIZE_SUM))
								.withFormatter(PERCENT_FORMATTER))
				.publish();
	}

	private void applicationMeasure(final BuildingContext context) {
		context.withFormatter(ByteFormatter.KEY)
				.createDatasetFromFacts()
				.agg(
						customAgg(DatastoreConstants.APPLICATION__USED_ON_HEAP, SingleValueFunction.PLUGIN_KEY).as(USED_HEAP),
						customAgg(DatastoreConstants.APPLICATION__MAX_ON_HEAP, SingleValueFunction.PLUGIN_KEY).as(COMMITTED_HEAP),
						customAgg(DatastoreConstants.APPLICATION__USED_OFF_HEAP, SingleValueFunction.PLUGIN_KEY).as(USED_DIRECT),
						customAgg(DatastoreConstants.APPLICATION__MAX_OFF_HEAP, SingleValueFunction.PLUGIN_KEY).as(MAX_DIRECT))
				.publish();
	}

	private void joinHierarchies(final BuildingContext context) {
		joinLevelsToChunks(context);
		joinFieldToChunks(context);
		joinIndexesToChunks(context);
		joinRefsToChunks(context);
		joinDicosToChunks(context);
	}

	/**
	 * Performs a CoPPer join between the Chunk store and the Chunk_To_Levels store
	 * The join mapping is the following :
	 *  - Chunk_Parent_ID
	 *  - Chunk_Parent_Type
	 *
	 * This join creates the following Analysis Hierarchies on the cube :
	 *  - fieldName
	 *  - storeName
	 *  - storePartition
	 *  since those are key fields of the joined store
	 * @param context
	 */
	private void joinFieldToChunks(BuildingContext context) {
		final StoreDataset fieldDataset = context.createDatasetFromStore(DatastoreConstants.CHUNK_TO_FIELD_STORE);
		context.createDatasetFromFacts()
				.join(
						fieldDataset,
						Columns.mapping(DatastoreConstants.CHUNK__PARENT_ID).to(DatastoreConstants.CHUNK_TO_FIELD__PARENT_ID)
								.and(DatastoreConstants.CHUNK__PARENT_TYPE).to(DatastoreConstants.CHUNK_TO_FIELD__PARENT_TYPE))
				.withColumn(DatastoreConstants.CHUNK_TO_FIELD__FIELD, col(DatastoreConstants.CHUNK_TO_FIELD__FIELD).asHierarchy().inDimension("Fields"))
				.withColumn(DatastoreConstants.CHUNK_TO_FIELD__STORE, col(DatastoreConstants.CHUNK_TO_FIELD__STORE).asHierarchy().inDimension("Fields"))
				.agg(Columns.sum(DatastoreConstants.CHUNK_ID).as("___a").withinFolder(BLACK_MAGIC_FOLDER))
				.doNotAggregateAbove()
				.publish();
	}

	/**
	 * Performs a coPPer join between the Chunk store and the Chunks_to_References store
	 * @param context
	 */
	private void joinRefsToChunks(BuildingContext context) {
		final StoreDataset fieldDataset = context.createDatasetFromStore(DatastoreConstants.CHUNK_TO_REF_STORE);
		context.createDatasetFromFacts()

				.join(
						fieldDataset,
						Columns.mapping(DatastoreConstants.CHUNK__PARENT_ID).to(DatastoreConstants.CHUNK_TO_REF__PARENT_ID)
								.and(DatastoreConstants.CHUNK__PARENT_TYPE).to(DatastoreConstants.CHUNK_TO_REF__PARENT_TYPE))
				.withColumn(DatastoreConstants.CHUNK_TO_REF__REF_ID, col(DatastoreConstants.CHUNK_TO_REF__REF_ID).asHierarchy().inDimension("References"))
				.groupBy(Columns.col(DatastoreConstants.CHUNK_TO_REF__REF_ID))
				.agg(customAgg(DatastoreConstants.CHUNK__PARENT_ID, CopyFunction.PLUGIN_KEY).as("___b").withinFolder(BLACK_MAGIC_FOLDER))
				.doNotAggregateAbove()
				.publish();
	}

	/**
	 * Performs a coPPer join between the Chunk store and the Chunks_to_Indexes store
	 * @param context
	 */
	private void joinIndexesToChunks(BuildingContext context) {
		final StoreDataset indexDataset = context.createDatasetFromStore(DatastoreConstants.CHUNK_TO_INDEX_STORE);

		context.createDatasetFromFacts()
				.join(
						indexDataset,
						Columns.mapping(DatastoreConstants.CHUNK__PARENT_ID).to(DatastoreConstants.CHUNK_TO_INDEX__PARENT_ID)
								.and(DatastoreConstants.CHUNK__PARENT_TYPE).to(DatastoreConstants.CHUNK_TO_INDEX__PARENT_TYPE))
				.withColumn(DatastoreConstants.CHUNK_TO_INDEX__INDEX_ID, col(DatastoreConstants.CHUNK_TO_INDEX__INDEX_ID).asHierarchy().inDimension("Indexes"))
				.agg(sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE).as("___c").withinFolder(BLACK_MAGIC_FOLDER))
				.publish();
	}

	/**
	 * Performs a CoPPer join between the Chunk store and the Chunk_To_Levels store
	 * The join mapping is the following :
	 *  - Chunk_Parent_ID
	 *  - Chunk_Parent_Type
	 *
	 * This join creates the following Analysis Hierarchies on the cube :
	 *  - managerId
	 *  - pivotId
	 *  - dimension
	 *  - hierarchy
	 *  - level
	 *  since those are key fields of the joined store
	 * @param context
	 */
	private void joinLevelsToChunks(BuildingContext context) {
		// TODO (men) : hack copper to allow multiple renaming of analysis hierarchies ??
		final StoreDataset levelsDataset = context.createDatasetFromStore(DatastoreConstants.CHUNK_TO_LEVEL_STORE);
		context.createDatasetFromFacts()
				.join(
						levelsDataset,
						Columns.mapping(DatastoreConstants.CHUNK__PARENT_ID).to(DatastoreConstants.CHUNK_TO_LEVEL__PARENT_ID)
								.and(DatastoreConstants.CHUNK__PARENT_TYPE).to(DatastoreConstants.CHUNK_TO_LEVEL__PARENT_TYPE))
				.withColumn(DatastoreConstants.CHUNK_TO_LEVEL__DIMENSION, col(DatastoreConstants.CHUNK_TO_LEVEL__DIMENSION).asHierarchy().inDimension("Levels"))
				.withColumn(DatastoreConstants.CHUNK_TO_LEVEL__HIERARCHY, col(DatastoreConstants.CHUNK_TO_LEVEL__HIERARCHY).asHierarchy().inDimension("Levels"))
				.withColumn(DatastoreConstants.CHUNK_TO_LEVEL__LEVEL, col(DatastoreConstants.CHUNK_TO_LEVEL__LEVEL).asHierarchy().inDimension("Levels"))
				.withColumn(DatastoreConstants.CHUNK_TO_LEVEL__MANAGER_ID, col(DatastoreConstants.CHUNK_TO_LEVEL__MANAGER_ID).asHierarchy().inDimension("Levels"))
				.withColumn(DatastoreConstants.CHUNK_TO_LEVEL__PIVOT_ID, col(DatastoreConstants.CHUNK_TO_LEVEL__PIVOT_ID).asHierarchy().inDimension("Levels"))

				.agg(sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE).as("___d").withinFolder(BLACK_MAGIC_FOLDER))
				.publish();
	}

	/**
	 * Performs a coPPer join between the Chunk store and the Chunks_to_Dicos store
	 * @param context
	 */
	private void joinDicosToChunks(BuildingContext context) {
		final StoreDataset dicosDataset = context.createDatasetFromStore(DatastoreConstants.CHUNK_TO_DICO_STORE);
		context.createDatasetFromFacts()
			.join(
					dicosDataset,
					Columns.mapping(DatastoreConstants.CHUNK__PARENT_ID).to(DatastoreConstants.CHUNK_TO_DICO__PARENT_ID)
					.and(DatastoreConstants.CHUNK__PARENT_TYPE).to(DatastoreConstants.CHUNK_TO_DICO__PARENT_TYPE))
			.withColumn(DatastoreConstants.CHUNK_TO_DICO__DICO_ID, col(DatastoreConstants.CHUNK_TO_DICO__DICO_ID).asHierarchy().inDimension("Dictionaries"))
			.agg(sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE).as("___e").withinFolder(BLACK_MAGIC_FOLDER))
			.publish();
	}
}
