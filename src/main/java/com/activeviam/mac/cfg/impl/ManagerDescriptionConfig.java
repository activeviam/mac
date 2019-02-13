/*
 * (C) Quartet FS 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.impl;

import java.util.concurrent.TimeUnit;

import com.activeviam.builders.StartBuilding;
import com.activeviam.copper.builders.BuildingContext;
import com.activeviam.copper.columns.Columns;
import com.activeviam.desc.build.ICanBuildCubeDescription;
import com.activeviam.desc.build.ICanStartBuildingMeasures;
import com.activeviam.desc.build.IHasAtLeastOneMeasure;
import com.activeviam.desc.build.dimensions.ICanStartBuildingDimensions;
import com.activeviam.formatter.ByteFormatter;
import com.activeviam.formatter.ClassFormatter;
import com.activeviam.formatter.IndexFormatter;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.postprocessor.impl.DirectMemoryOnlyPostProcessor;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.activeviam.drillthrough.impl.FieldsColumn;
import com.qfs.fwk.format.impl.EpochFormatter;
import com.qfs.fwk.ordering.impl.ReverseEpochComparator;
import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;
import com.quartetfs.biz.pivot.context.impl.QueriesTimeLimit;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IMeasureHierarchy;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.ISelectionDescription;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessorConstants;
import com.quartetfs.fwk.format.impl.DateFormatter;
import com.quartetfs.fwk.ordering.impl.CustomComparator;
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
	public static final String HEAP_MEMORY_CHUNK_USAGE_SUM = "HeapMemoryChunkUsage.SUM";
	public static final String CHUNK_CLASS_LEVEL = "Class";
	public static final String CHUNK_TYPE_LEVEL = "Type";
	public static final String DIRECT_CHUNKS_COUNT = "DirectChunks.COUNT";
	public static final String CHUNK_CLASS_FIELD = "ChunkClass";
	public static final String CHUNKSET_CLASS_FIELD = "ChunkSetClass";
	public static final String INDEX_CLASS_FIELD = "IndexClass";
	public static final String DICTIONARY_CLASS_FIELD = "DictionaryClass";
	public static final String REFERENCE_CLASS_FIELD = "ReferenceClass";
	public static final String CHUNK_HIERARCHY = "Chunks";
	public static final String DIRECT_MEMORY_CHUNK_USAGE_SUM = "DirectMemoryChunkUsage.SUM";
	public static final String CHUNKSET_SIZE_FIELD = "ChunkSetSize";
	public static final String DICTIONARY_SIZE_FIELD = "DictionarySize";

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

	private ISelectionDescription createSelection() {
		return StartBuilding.selection(this.datastoreDescriptionConfig.schemaDescription())
				.fromBaseStore(DatastoreConstants.CHUNK_STORE)
				.withAllFields()
				.withAlias(DatastoreConstants.CHUNK__CLASS, CHUNK_CLASS_FIELD)

				.usingReference(MemoryAnalysisDatastoreDescription.CHUNK_TO_REF)
				.withAllFields()
				.withAlias(DatastoreConstants.REFERENCE_CLASS, REFERENCE_CLASS_FIELD)

				.usingReference(MemoryAnalysisDatastoreDescription.CHUNK_TO_INDICES)
				.withAllFields()
				.withAlias(DatastoreConstants.INDEX_CLASS, INDEX_CLASS_FIELD)

				.usingReference(MemoryAnalysisDatastoreDescription.CHUNK_TO_SETS)
				.withAllFields()
				.except(DatastoreConstants.EPOCH_ID, DatastoreConstants.CHUNKSET_ID)
				.withAlias(DatastoreConstants.CHUNK_SET_CLASS, CHUNKSET_CLASS_FIELD)
				.withAlias(DatastoreConstants.CHUNK_SET_PHYSICAL_CHUNK_SIZE, CHUNKSET_SIZE_FIELD)

				.usingReference(MemoryAnalysisDatastoreDescription.CHUNK_TO_DICS)
				.withAllFields()
				.except(DatastoreConstants.EPOCH_ID, DatastoreConstants.DICTIONARY_ID)
				.withAlias(DatastoreConstants.DICTIONARY_CLASS, DICTIONARY_CLASS_FIELD)
				.withAlias(DatastoreConstants.DICTIONARY_SIZE, DICTIONARY_SIZE_FIELD)

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

				.withSharedDrillthroughProperties()
				.hideColumn(DatastoreConstants.FIELDS)
				.withCalculatedColumn()
				.withName("Field names")
				.withPluginKey(FieldsColumn.PLUGIN_KEY)
				.withUnderlyingFields(DatastoreConstants.FIELDS)
				.end().end()

				.build();
	}

	private ICanBuildCubeDescription<IActivePivotInstanceDescription> defineDimensions(
			final ICanStartBuildingDimensions builder) {
		return builder
				.withDimension(CHUNK_HIERARCHY)
				.withHierarchyOfSameName()
				.withLevel(CHUNK_TYPE_LEVEL).withPropertyName(DatastoreConstants.CHUNK__TYPE)
				.withProperty("description", "What are chunks for")
				.withLevel(CHUNK_CLASS_LEVEL).withPropertyName(CHUNK_CLASS_FIELD)
				.withFormatter(ClassFormatter.KEY)
				.withProperty("description", "Class of the chunks")
				.withLevel("ChunkId").withPropertyName(DatastoreConstants.CHUNK_ID)

				.withSingleLevelDimension("Date").withPropertyName(DatastoreConstants.CHUNK__EXPORT_DATE)
				.withType(ILevelInfo.LevelType.TIME)
				.withComparator(ReverseOrderComparator.type)
				.withProperty("description", "Date at which statistics were retrieved")

				.withDimension("Store")
				.withHierarchyOfSameName()
				.withLevel("Store").withPropertyName(DatastoreConstants.CHUNK__STORE_NAME)
				.withLevel("Partition").withPropertyName(DatastoreConstants.CHUNK__PARTITION_ID)

				.withDimension("Chunk Set")
				.withHierarchyOfSameName()
				.withLevel("Class").withPropertyName(CHUNKSET_CLASS_FIELD)
				.withFormatter(ClassFormatter.KEY)
				.withLevel("Id").withPropertyName(DatastoreConstants.CHUNKSET_ID)

				.withSingleLevelDimension("Fields").withPropertyName(DatastoreConstants.FIELDS)

				.withDimension("References and Indices")
				.withHierarchy("References")
				.withLevel("Name").withPropertyName(DatastoreConstants.REFERENCE_NAME)
				.withHierarchy("Indices")
				.withLevel("Type").withPropertyName(INDEX_CLASS_FIELD)
				.withFormatter(IndexFormatter.KEY)
				.withComparator(CustomComparator.type)
				.withFirstObjects(
						"com.qfs.store.impl.MultiVersionCompositePrimaryRecordIndex",
						"com.qfs.store.impl.MultiVersionCompositeSecondaryRecordIndex",
						"com.qfs.store.impl.MultiVersionPrimaryRecordIndex",
						"com.qfs.store.impl.MultiVersionSecondaryRecordIndex")

				.withDimension("Dictionary")
				.withHierarchyOfSameName()
				.withLevel("Class").withPropertyName(DICTIONARY_CLASS_FIELD)
				.withFormatter(ClassFormatter.KEY)
				.withLevel("Order").withPropertyName(DatastoreConstants.DICTIONARY_ORDER)
				.withLevel("Size").withPropertyName(DICTIONARY_SIZE_FIELD)

				.withDimension("Dump name")
				.withHierarchyOfSameName().slicing()
				.withLevelOfSameName().withPropertyName(DatastoreConstants.CHUNK__DUMP_NAME)
				.withComparator(ReverseOrderComparator.type);
	}

	private IHasAtLeastOneMeasure measures(ICanStartBuildingMeasures builder) {
		return builder
				.withContributorsCount()
				.withAlias("Chunks.COUNT")
				.withFormatter("INT[#,###]")
				.withUpdateTimestamp()
				.withAlias("Timestamp")
				.withFormatter(DateFormatter.TYPE + "[HH:mm:ss]")

				.withPostProcessor(DIRECT_CHUNKS_COUNT)
				.withPluginKey(DirectMemoryOnlyPostProcessor.PLUGIN_KEY)
				.withUnderlyingMeasures(IMeasureHierarchy.COUNT_ID)
				.withProperty(
						IPostProcessorConstants.DYNAMIC_AGGREGATION_PARAM_LEAF_LEVELS,
						CHUNK_CLASS_LEVEL + "@" + CHUNK_HIERARCHY)
				.withProperty(
						DirectMemoryOnlyPostProcessor.MEASURE_KEY, DIRECT_MEMORY_SUM)

				/*.withPostProcessor(DIRECT_MEMORY_CHUNK_USAGE_SUM)
				.withPluginKey(DirectMemoryOnlyPostProcessor.PLUGIN_KEY)
				.withUnderlyingMeasures(DIRECT_MEMORY_SUM)
				.withProperty(
						IPostProcessorConstants.DYNAMIC_AGGREGATION_PARAM_LEAF_LEVELS,
						CHUNK_CLASS_LEVEL + "@" + CHUNK_HIERARCHY)
				.withProperty(
						DirectMemoryOnlyPostProcessor.MEASURE_KEY, DIRECT_MEMORY_SUM)
				.withFormatter(ByteFormatter.KEY)*/;
	}

	private void copperCalculations(final BuildingContext buildingContext) {
		buildingContext.withFormatter(ByteFormatter.KEY)
				.createDatasetFromFacts()
				.agg(
						Columns.sum(DatastoreConstants.CHUNK__OFF_HEAP_SIZE)
								.as(DIRECT_MEMORY_SUM),
						Columns.sum(DatastoreConstants.CHUNK__ON_HEAP_SIZE)
								.as(HEAP_MEMORY_CHUNK_USAGE_SUM))
				.publish();

		buildingContext.withinFolder("Technical ChunkSet")
				.createDatasetFromFacts()
				.agg(
						Columns.sum(DatastoreConstants.CHUNK_SET_FREE_ROWS)
								.as("FreeRows.SUM"),
						Columns.sum(CHUNKSET_SIZE_FIELD)
								.as("PhysicalChunkSize.SUM"))
				.publish();

//		buildingContext.createDatasetFromFacts()
//				.groupBy(Columns.col(CHUNK_CLASS_FIELD))
//				.agg(
////						Columns.count(),
//						Columns.sum(DIRECT_MEMORY_SUM).as("m"))
//				.withColumn(
//						DIRECT_CHUNKS_COUNT,
//						Columns.combine(Columns.col("m"), Columns.count())
//								.map(values -> {
//									final Object memory = values.read(0);
//									final Object count = values.read(1);
//									return memory != null ? count : 0;
//								}))
//				.agg(Columns.sum(DIRECT_CHUNKS_COUNT).as(DIRECT_CHUNKS_COUNT))
//				.publish();

		buildingContext.createDatasetFromFacts()
				.groupBy(Columns.col(CHUNK_CLASS_FIELD))
				.agg(Columns.sum(DIRECT_MEMORY_SUM).as("c"))
				.agg(Columns.sum("c").as(DIRECT_MEMORY_CHUNK_USAGE_SUM))
				.publish();

		buildingContext.createDatasetFromFacts()
				.withColumn(
						"c",
						Columns.col(DIRECT_MEMORY_CHUNK_USAGE_SUM)
							.plus(Columns.col(HEAP_MEMORY_CHUNK_USAGE_SUM)).as("cc"))
				.agg(Columns.sum("c").as("TotalMemoryUsage.SUM"))
				.publish();
	}



}
