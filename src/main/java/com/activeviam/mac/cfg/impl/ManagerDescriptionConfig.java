/*
 * (C) Quartet FS 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.impl;

import static com.activeviam.copper.columns.Columns.col;
import static com.activeviam.copper.columns.Columns.count;
import static com.activeviam.copper.columns.Columns.customAgg;
import static com.activeviam.copper.columns.Columns.sum;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.activeviam.builders.StartBuilding;
import com.activeviam.copper.builders.BuildingContext;
import com.activeviam.copper.builders.dataset.Datasets;
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
import com.activeviam.postprocessor.impl.DirectMemoryOnlyPostProcessor;
import com.qfs.agg.impl.SingleValueFunction;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.fwk.format.impl.EpochFormatter;
import com.qfs.fwk.ordering.impl.ReverseEpochComparator;
import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;
import com.qfs.store.Types;
import com.quartetfs.biz.pivot.context.impl.QueriesTimeLimit;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IMeasureHierarchy;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.ISelectionDescription;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessorConstants;
import com.quartetfs.biz.pivot.postprocessing.impl.ArithmeticFormulaPostProcessor;
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

	public static final String MONITORING_CUBE = "MemoryCube";
	public static final String DIRECT_MEMORY_SUM = "DirectMemory.SUM";
	public static final String HEAP_MEMORY_SUM = "HeapMemory.SUM";
	public static final String HEAP_MEMORY_CHUNK_USAGE_SUM = "HeapMemoryChunkUsage.SUM";
	public static final String CHUNK_CLASS_LEVEL = "Class";
	public static final String CHUNK_TYPE_LEVEL = "Type";
	public static final String DIRECT_CHUNKS_COUNT = "DirectChunks.COUNT";
	public static final String HEAP_CHUNKS_COUNT = "HeapChunks.COUNT";
	public static final String CHUNK_CLASS_FIELD = "ChunkClass";
	public static final String CHUNKSET_CLASS_FIELD = "ChunkSetClass";
	public static final String INDEX_CLASS_FIELD = "IndexClass";
	public static final String DICTIONARY_CLASS_FIELD = "DictionaryClass";
	public static final String REFERENCE_CLASS_FIELD = "ReferenceClass";
	public static final String CHUNK_HIERARCHY = "Chunks";
	public static final String DIRECT_MEMORY_CHUNK_USAGE_SUM = "DirectMemoryChunkUsage.SUM";
	public static final String CHUNKSET_SIZE_FIELD = "ChunkSetSize";
	public static final String DICTIONARY_SIZE_FIELD = "DictionarySize";

	public static final String USED_HEAP = "UsedHeapMemory";
	public static final String COMMITTED_HEAP = "CommittedHeapMemory";
	public static final String USED_DIRECT = "UsedDirectMemory";
	public static final String MAX_DIRECT = "MaxDirectMemory";

	public static final String BLACK_MAGIC_FOLDER = "BlackMagic";
	public static final String BLACK_MAGIC_HIERARCHY = "BlackMagic";

	public static final String NUMBER_FORMATTER = NumberFormatter.TYPE + "[#,###]";

	/** The datastore schema {@link IDatastoreSchemaDescription description}.  */
	@Autowired
	private IDatastoreDescriptionConfig datastoreDescriptionConfig;

	@Bean
	@Override
	public IActivePivotManagerDescription managerDescription() {
		return StartBuilding.managerDescription()
				.withSchema("MemorySchema")
				.withSelection(createSelection())
				.withCube(createCube())
				.build();
	}

	protected static String prefixField(String prefix, String field) {
		return prefix + field.substring(0, 1).toUpperCase() + field.substring(1);
	}

	private ISelectionDescription createSelection() {
		return StartBuilding.selection(this.datastoreDescriptionConfig.schemaDescription())
				.fromBaseStore(DatastoreConstants.CHUNK_STORE)
				.withAllReachableFields(allReachableFields -> {
					allReachableFields.remove(DatastoreConstants.CHUNK__CLASS);

					final Map<String, String> result = ISelectionDescriptionBuilder.FieldsCollisionHandler.CLOSEST.handle(allReachableFields);
					result.put(prefixField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__CLASS), DatastoreConstants.CHUNK__CLASS);

					return result;
				})

//				.usingReference(MemoryAnalysisDatastoreDescription.CHUNK_TO_SETS)
//				.withAllFields()
//				.except(DatastoreConstants.CHUNKSET_ID)
//				.withAlias(DatastoreConstants.CHUNK_SET_CLASS, prefixField(DatastoreConstants.CHUNKSET_STORE, DatastoreConstants.CHUNK_SET_CLASS))

//				.usingReference(MemoryAnalysisDatastoreDescription.CHUNK_TO_REF)
//				.withAllFields()
//				.except(DatastoreConstants.REFERENCE_ID)
//				.withAlias(DatastoreConstants.REFERENCE_CLASS, prefixField(DatastoreConstants.REFERENCE_STORE, DatastoreConstants.REFERENCE_CLASS))

//				.usingReference(MemoryAnalysisDatastoreDescription.CHUNK_TO_INDICES)
//				.withAllFields()
//				.except(DatastoreConstants.INDEX_ID)
//				.withAlias(DatastoreConstants.INDEX_TYPE, prefixField(DatastoreConstants.INDEX_STORE, DatastoreConstants.INDEX_TYPE))
//				.withAlias(DatastoreConstants.INDEX_CLASS, prefixField(DatastoreConstants.INDEX_STORE, DatastoreConstants.INDEX_CLASS))

//				.usingReference(MemoryAnalysisDatastoreDescription.CHUNK_TO_PROVIDER)
//				.withAllFields()
//				.except(DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID)
//				.withAlias(DatastoreConstants.PROVIDER_COMPONENT__CLASS, prefixField(DatastoreConstants.PROVIDER_COMPONENT_STORE, DatastoreConstants.PROVIDER_COMPONENT__CLASS))

//				.usingReference(MemoryAnalysisDatastoreDescription.CHUNK_TO_PROVIDER, MemoryAnalysisDatastoreDescription.PROVIDER_COMPONENT_TO_PROVIDER)
//				.withAllFields()
//				.except(DatastoreConstants.PROVIDER__PROVIDER_ID)
//				.withAlias(DatastoreConstants.PROVIDER__INDEX, prefixField(DatastoreConstants.PROVIDER_STORE, DatastoreConstants.PROVIDER__INDEX))
//				.withAlias(DatastoreConstants.PROVIDER__TYPE, prefixField(DatastoreConstants.PROVIDER_STORE, DatastoreConstants.PROVIDER__TYPE))
//				.withAlias(DatastoreConstants.PROVIDER__CATEGORY, prefixField(DatastoreConstants.PROVIDER_STORE, DatastoreConstants.PROVIDER__CATEGORY))

//				.usingReference(MemoryAnalysisDatastoreDescription.CHUNK_TO_DICS)
//				.withAllFields()
//				.except(DatastoreConstants.DICTIONARY_ID)
//				.withAlias(DatastoreConstants.DICTIONARY_CLASS, prefixField(DatastoreConstants.DICTIONARY_STORE, DatastoreConstants.DICTIONARY_CLASS))
//				.withAlias(DatastoreConstants.DICTIONARY_SIZE, prefixField(DatastoreConstants.DICTIONARY_STORE, DatastoreConstants.DICTIONARY_SIZE))

				.build();
	}

	private IActivePivotInstanceDescription createCube() {
		return StartBuilding.cube(MONITORING_CUBE)
				.withMeasures(this::measures)

				.withDimensions(this::defineDimensions)

				.withEpochDimension()
					.withEpochsLevel()
						.withComparator(ReverseEpochComparator.TYPE)
						.withFormatter(EpochFormatter.TYPE + "[HH:mm:ss]")
						.end()

				.withDescriptionPostProcessor(
						StartBuilding
								.copperCalculations()
								.withDefinition(this::copperCalculations)
								.build())

				.withSharedContextValue(QueriesTimeLimit.of(15, TimeUnit.SECONDS))

//				.withSharedDrillthroughProperties()
//				.hideColumn(DatastoreConstants.FIELDS)
//				.withCalculatedColumn()
//				.withName("Field names")
//				.withPluginKey(FieldsColumn.PLUGIN_KEY)
//				.withUnderlyingFields(DatastoreConstants.FIELDS)
//				.end().end()

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
				.withPropertyName(prefixField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__CLASS))
				.withFormatter(ClassFormatter.KEY)
				.withProperty("description", "Class of the chunks")

//				.withDimension("Store")
//				.withHierarchyOfSameName()
//				.withLevel("StoreName").withPropertyName(DatastoreConstants.CHUNK__PARTITION__STORE_NAME)

				.withSingleLevelDimension("PartitionId").withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)

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

				.withDimension("Dump")
				.withHierarchyOfSameName()
				.slicing()
				.withLevelOfSameName().withPropertyName(DatastoreConstants.CHUNK__DUMP_NAME)
				.withComparator(ReverseOrderComparator.type)

				.withDimension("Pivot")
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
				.withAlias("Timestamp")
				.withFormatter(DateFormatter.TYPE + "[HH:mm:ss]");

//				.withPostProcessor(DIRECT_CHUNKS_COUNT)
//				.withPluginKey(DirectMemoryOnlyPostProcessor.PLUGIN_KEY)
//				.withUnderlyingMeasures(IMeasureHierarchy.COUNT_ID)
//				.withProperty(
//						IPostProcessorConstants.DYNAMIC_AGGREGATION_PARAM_LEAF_LEVELS,
//						CHUNK_CLASS_LEVEL + "@" + CHUNK_CLASS_LEVEL)
//				.withProperty(
//						DirectMemoryOnlyPostProcessor.MEASURE_KEY, DIRECT_MEMORY_SUM)
//				.withFormatter(NUMBER_FORMATTER)
//
//				.withPostProcessor(HEAP_CHUNKS_COUNT)
//					.withPluginKey(ArithmeticFormulaPostProcessor.PLUGIN_KEY)
//					.withProperty(
//							ArithmeticFormulaPostProcessor.FORMULA_PROPERTY,
//							"aggregatedValue[" + IMeasureHierarchy.COUNT_ID  + "],aggregatedValue[" + DIRECT_CHUNKS_COUNT + "],-")
//					.withUnderlyingMeasures(IMeasureHierarchy.COUNT_ID, DIRECT_CHUNKS_COUNT);
	}

	private void copperCalculations(final BuildingContext context) {
		basicMeasures(context);
		applicationMeasure(context);
		joinHierarchies(context);
	}

	private void basicMeasures(final BuildingContext context) {
		context.withFormatter(ByteFormatter.KEY)
				.createDatasetFromFacts()
				.agg(
						sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE).as(DIRECT_MEMORY_SUM),
						sum(DatastoreConstants.CHUNK__ON_HEAP_SIZE).as(HEAP_MEMORY_SUM))
				.publish();

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

		context.withinFolder("Technical Chunk")
				.withFormatter(NUMBER_FORMATTER)
				.createDatasetFromFacts()
				.agg(
						sum(DatastoreConstants.CHUNK__SIZE).as("ChunkSize.SUM"),
						sum(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS).as("NonWrittenRows.COUNT"),
						sum(DatastoreConstants.CHUNK__FREE_ROWS).as("DeletedRows.COUNT"))
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
		joinFieldToChunks(context);
		joinIndexesToChunks(context);
		joinRefsToChunks(context);
		joinLevelsToChunks(context);
	}

	private void joinFieldToChunks(BuildingContext context) {
		final StoreDataset fieldDataset = context.createDatasetFromStore(DatastoreConstants.CHUNK_TO_FIELD_STORE);
		context.createDatasetFromFacts()
				.join(
						fieldDataset,
						Columns.mapping(DatastoreConstants.CHUNK__PARENT_ID).to(DatastoreConstants.CHUNK_TO_FIELD__PARENT_ID)
								.and(DatastoreConstants.CHUNK__PARENT_TYPE).to(DatastoreConstants.CHUNK_TO_FIELD__PARENT_TYPE))
				.agg(sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE).as("-ofh.sf").withinFolder(BLACK_MAGIC_FOLDER))
				.publish();
	}

	private void joinRefsToChunks(BuildingContext context) {
		final StoreDataset fieldDataset = context.createDatasetFromStore(DatastoreConstants.CHUNK_TO_REF_STORE);
		context.createDatasetFromFacts()
				.join(
						fieldDataset,
						Columns.mapping(DatastoreConstants.CHUNK__PARENT_ID).to(DatastoreConstants.CHUNK_TO_REF__PARENT_ID)
								.and(DatastoreConstants.CHUNK__PARENT_TYPE).to(DatastoreConstants.CHUNK_TO_REF__PARENT_TYPE))
				.agg(sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE).as("-ofh.ref").withinFolder(BLACK_MAGIC_FOLDER))
				.publish();
	}

	private void joinIndexesToChunks(BuildingContext context) {
		final StoreDataset fieldDataset = context.createDatasetFromStore(DatastoreConstants.CHUNK_TO_INDEX_STORE);
		context.createDatasetFromFacts()
				.join(
						fieldDataset,
						Columns.mapping(DatastoreConstants.CHUNK__PARENT_ID).to(DatastoreConstants.CHUNK_TO_INDEX__PARENT_ID)
								.and(DatastoreConstants.CHUNK__PARENT_TYPE).to(DatastoreConstants.CHUNK_TO_INDEX__PARENT_TYPE))
				.agg(sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE).as("-ofh.idx").withinFolder(BLACK_MAGIC_FOLDER))
				.publish();
	}

	private void joinLevelsToChunks(BuildingContext context) {
		final StoreDataset fieldDataset = context.createDatasetFromStore(DatastoreConstants.CHUNK_TO_LEVEL_STORE);
		context.createDatasetFromFacts()
				.join(
						fieldDataset,
						Columns.mapping(DatastoreConstants.CHUNK__PARENT_ID).to(DatastoreConstants.CHUNK_TO_LEVEL__PARENT_ID)
								.and(DatastoreConstants.CHUNK__PARENT_TYPE).to(DatastoreConstants.CHUNK_TO_LEVEL__PARENT_TYPE))
				.agg(sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE).as("-ofh.lvl").withinFolder(BLACK_MAGIC_FOLDER))
				.publish();
	}

}
