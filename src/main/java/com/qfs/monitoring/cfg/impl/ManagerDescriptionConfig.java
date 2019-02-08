/*
 * (C) Quartet FS 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.cfg.impl;

import java.util.concurrent.TimeUnit;

import com.activeviam.builders.StartBuilding;
import com.activeviam.copper.builders.BuildingContext;
import com.activeviam.copper.columns.Columns;
import com.activeviam.desc.build.ICanBuildCubeDescription;
import com.activeviam.desc.build.dimensions.ICanStartBuildingDimensions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.drillthrough.impl.FieldsColumn;
import com.qfs.formatter.ByteFormatter;
import com.qfs.formatter.ClassFormatter;
import com.qfs.formatter.IndexFormatter;
import com.qfs.fwk.format.impl.EpochFormatter;
import com.qfs.fwk.ordering.impl.ReverseEpochComparator;
import com.qfs.monitoring.memory.DatastoreConstants;
import com.qfs.server.cfg.IActivePivotManagerDescriptionConfig;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;
import com.quartetfs.biz.pivot.context.impl.QueriesTimeLimit;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.ISelectionDescription;
import com.quartetfs.fwk.format.impl.DateFormatter;
import com.quartetfs.fwk.ordering.impl.CustomComparator;
import com.quartetfs.fwk.ordering.impl.ReverseOrderComparator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

/**
 * @author Quartet FS
 *
 */
@Configuration
public class ManagerDescriptionConfig implements IActivePivotManagerDescriptionConfig {

	public static final String MONITORING_CUBE = "MonitoringCube";
	public static final String DIRECT_MEMORY_SUM = "DirectMemory.SUM";
	public static final String HEAP_MEMORY_CHUNK_USAGE_SUM = "HeapMemoryChunkUsage.SUM";
	public static final String CHUNK_CLASS_LEVEL = "Class";
	public static final String CHUNK_TYPE_LEVEL = "Type";
	public static final String DIRECT_CHUNKS_COUNT = "DirectChunks.COUNT";

	/** The datastore schema {@link IDatastoreSchemaDescription description}.  */
	@Autowired
	private IDatastoreDescriptionConfig datastoreDescriptionConfig;

	@Override
	public IActivePivotManagerDescription managerDescription() {
		return StartBuilding.managerDescription()
				.withSchema("MonitoringSchema")
				.withSelection(createSelection())
				.withCube(createCube())
				.build();
	}

	private ISelectionDescription createSelection() {
		return StartBuilding.selection(this.datastoreDescriptionConfig.schemaDescription())
				.fromBaseStore(DatastoreConstants.CHUNK_STORE)
				.withAllReachableFields()
				.build();
	}

	private IActivePivotInstanceDescription createCube() {
		return StartBuilding.cube(MONITORING_CUBE)
				.withContributorsCount()
				.withAlias("Chunks.COUNT")
				.withFormatter("INT[#,###]")
				.withUpdateTimestamp()
				.withAlias("Timestamp")
				.withFormatter(DateFormatter.TYPE + "[HH:mm:ss]")

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
				.withDimension("Chunks")
				.withHierarchyOfSameName()
				.withLevel(CHUNK_TYPE_LEVEL)
				.withProperty("description", "What are chunks for")
				.withLevel(CHUNK_CLASS_LEVEL)
				.withFormatter(ClassFormatter.KEY)
				.withProperty("description", "Class of the chunks")
				.withLevel("ChunkId")

				.withSingleLevelDimension("Date")
				.withPropertyName(DatastoreConstants.EXPORT_DATE)
				.withType(ILevelInfo.LevelType.TIME)
				.withComparator(ReverseOrderComparator.type)
				.withProperty("description", "Date at which statistics were retrieved")

				.withDimension("Store")
				.withHierarchyOfSameName()
				.withLevel("Store").withPropertyName(DatastoreConstants.STORE_AND_PARTITION_STORE_NAME)
				.withLevel("Partition").withPropertyName(DatastoreConstants.STORE_AND_PARTITION_PARTITION_ID)

				.withDimension("Chunk Set")
				.withHierarchyOfSameName()
				.withLevel("Class")
				.withPropertyName(DatastoreConstants.CHUNK_SET_CLASS)
				.withFormatter(ClassFormatter.KEY)
				.withLevel("Id").withPropertyName(DatastoreConstants.CHUNKSET_ID)

				.withSingleLevelDimension("Fields").withPropertyName(DatastoreConstants.FIELDS)

				.withDimension("References and Indices")
				.withHierarchy("References")
				.withLevel("Name").withPropertyName(DatastoreConstants.REFERENCE_NAME)
				.withHierarchy("Indices")
				.withLevel("Type").withPropertyName(DatastoreConstants.INDEX_CLASS)
				.withFormatter(IndexFormatter.KEY)
				.withComparator(CustomComparator.type)
				.withFirstObjects(
						"com.qfs.store.impl.MultiVersionCompositePrimaryRecordIndex",
						"com.qfs.store.impl.MultiVersionCompositeSecondaryRecordIndex",
						"com.qfs.store.impl.MultiVersionPrimaryRecordIndex",
						"com.qfs.store.impl.MultiVersionSecondaryRecordIndex")

				.withDimension("Dictionary")
				.withHierarchyOfSameName()
				.withLevel("Class").withPropertyName(DatastoreConstants.DICTIONARY_CLASS)
				.withFormatter(ClassFormatter.KEY)
				.withLevel("Order").withPropertyName(DatastoreConstants.DICTIONARY_ORDER)
				.withLevel("Size").withPropertyName(DatastoreConstants.DICTIONARY_SIZE)

				.withDimension("Dump name")
				.withHierarchyOfSameName().slicing()
				.withLevelOfSameName().withPropertyName(DatastoreConstants.DUMP_NAME)
				.withComparator(ReverseOrderComparator.type);
	}

	private void copperCalculations(final BuildingContext buildingContext) {
		buildingContext.withFormatter(ByteFormatter.KEY)
				.createDatasetFromFacts()
				.agg(
						Columns.sum(DatastoreConstants.CHUNK_OFF_HEAP_SIZE)
								.as(DIRECT_MEMORY_SUM),
						Columns.sum(DatastoreConstants.CHUNK_ON_HEAP_SIZE)
								.as(HEAP_MEMORY_CHUNK_USAGE_SUM))
				.publish();

		buildingContext.withinFolder("Technical ChunkSet")
				.createDatasetFromFacts()
				.agg(
						Columns.sum(DatastoreConstants.CHUNK_SET_FREE_ROWS)
								.as("FreeRows.SUM"),
						Columns.sum(DatastoreConstants.CHUNK_SET_PHYSICAL_CHUNK_SIZE)
								.as("PhysicalChunkSize.SUM"))
				.publish();

		buildingContext.createDatasetFromFacts()
				.groupBy(Columns.col(CHUNK_CLASS_LEVEL))
				.agg(
						Columns.combine(Columns.col(DIRECT_MEMORY_SUM), Columns.count())
								.map(values -> {
									final Object memory = values.read(0);
									final Object count = values.read(1);
									return memory != null ? count : 0;
								})
								.as(DIRECT_CHUNKS_COUNT))
				.agg(Columns.sum(DIRECT_CHUNKS_COUNT).as(DIRECT_CHUNKS_COUNT))
				.publish();
	}



}
