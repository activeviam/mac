/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import com.activeviam.copper.api.Copper;
import com.activeviam.copper.api.CopperStore;
import com.activeviam.copper.store.Mapping.JoinType;
import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription.ParentType;
import com.activeviam.mac.statistic.memory.ATestMemoryStatistic;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.activeviam.pivot.builders.StartBuilding;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.dic.ISchemaDictionaryProvider;
import com.qfs.junit.LocalResourcesExtension;
import com.qfs.literal.ILiteralType;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.pivot.monitoring.impl.MemoryAnalysisService;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
import com.qfs.service.monitoring.IMemoryAnalysisService;
import com.qfs.store.IDatastore;
import com.qfs.store.query.ICursor;
import com.qfs.store.record.IRecordReader;
import com.qfs.util.impl.QfsFileTestUtils;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;
import com.quartetfs.biz.pivot.impl.ActivePivotManagerBuilder;
import com.quartetfs.fwk.AgentException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.contributions.impl.ClasspathContributionProvider;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestMultipleFieldsDictionary extends ATestMemoryStatistic {

	@RegisterExtension
	protected LocalResourcesExtension resources = new LocalResourcesExtension();

	protected static final Path TEMP_DIRECTORY =
			QfsFileTestUtils.createTempDirectory(TestMultipleFieldsDictionary.class);

	protected IDatastore monitoredDatastore;
	protected IActivePivotManager monitoredManager;
	protected Path statisticsPath;
	protected IDatastore monitoringDatastore;

	@BeforeAll
	public static void setupRegistry() {
		Registry.setContributionProvider(new ClasspathContributionProvider());
	}

	@BeforeEach
	public void setupAndExportApplication() throws AgentException, IOException {
		final IDatastoreSchemaDescription datastoreSchemaDescription = datastoreSchema();
		final IActivePivotManagerDescription managerDescription =
				managerDescription(datastoreSchemaDescription);

		monitoredDatastore = resources.create(StartBuilding.datastore()
				.setSchemaDescription(datastoreSchemaDescription)
				.addSchemaDescriptionPostProcessors(
						ActivePivotDatastorePostProcessor.createFrom(managerDescription))
				::build);

		monitoredManager = StartBuilding.manager()
				.setDescription(managerDescription)
				.setDatastoreAndPermissions(monitoredDatastore)
				.buildAndStart();
		resources.register(monitoredManager::stop);

		fillApplication();
		performGC();
		exportApplicationMemoryStatistics();

		monitoringDatastore = createAnalysisDatastore();
		loadStatisticsIntoDatastore(loadMemoryStatistic(statisticsPath), monitoringDatastore);
	}

	protected IDatastoreSchemaDescription datastoreSchema() {
		return StartBuilding.datastoreSchema()
				.withStore(
						StartBuilding.store()
								.withStoreName("Forex")
								.withField("currency", ILiteralType.STRING)
								.asKeyField()
								.withField("targetCurrency", ILiteralType.STRING)
								.asKeyField()
								.build())
				.withStore(
						StartBuilding.store()
								.withStoreName("Currency")
								.withField("currency", ILiteralType.STRING)
								.asKeyField()
								.withField("countryId", ILiteralType.INT)
								.build())
				.build();
	}

	protected IActivePivotManagerDescription managerDescription(
			IDatastoreSchemaDescription datastoreSchemaDescription) {
		final IActivePivotManagerDescription managerDescription = StartBuilding.managerDescription()
				.withSchema()
				.withSelection(StartBuilding.selection(datastoreSchemaDescription)
						.fromBaseStore("Forex")
						.withAllReachableFields()
						.build())
				.withCube(StartBuilding.cube("Cube")
						.withContributorsCount()
						.withCalculations(context -> {
							// these joins make Forex.currency and Forex.targetCurrency share the same dictionary
							CopperStore currencyStore1 = Copper.store("Currency").joinToCube(JoinType.INNER)
									.withMapping("currency", "Currency");

							Copper.newSingleLevelHierarchy("Currency", "Geography", "Geography")
									.from(currencyStore1.field("countryId"))
									.slicing()
									.publish(context);

							CopperStore currencyStore2 = Copper.store("Currency").joinToCube(JoinType.INNER)
									.withMapping("currency", "Target Currency");

							Copper.newSingleLevelHierarchy("Target Currency", "Geography", "Geography")
									.from(currencyStore2.field("countryId"))
									.slicing()
									.publish(context);
						})
						.withSingleLevelDimension("Currency")
						.withPropertyName("currency")
						.withSingleLevelDimension("Target Currency")
						.withPropertyName("targetCurrency")
						.build())
				.build();

		return ActivePivotManagerBuilder.postProcess(managerDescription, datastoreSchemaDescription);
	}


	protected void fillApplication() {
		final String[] currencies = new String[] {"EUR", "GBP", "USD"};

		monitoredDatastore.edit(transactionManager -> {
			int countryId = 0;
			for (final String currency : currencies) {
				transactionManager.add("Currency", currency, countryId++);
				for (final String targetCurrency : currencies) {
					transactionManager.add("Forex", currency, targetCurrency);
				}
			}
		});
	}

	protected void exportApplicationMemoryStatistics() {
		final IMemoryAnalysisService analysisService = new MemoryAnalysisService(
				monitoredDatastore, monitoredManager, monitoredDatastore.getEpochManager(), TEMP_DIRECTORY);
		statisticsPath = analysisService.exportMostRecentVersion("memoryStats");
	}

	protected IDatastore createAnalysisDatastore() {
		final IDatastoreSchemaDescription desc = new MemoryAnalysisDatastoreDescription();
		return resources.create(StartBuilding.datastore().setSchemaDescription(desc)::build);
	}

	protected Collection<IMemoryStatistic> loadMemoryStatistic(final Path path) throws IOException {
		return Files.list(path)
				.map(file -> {
					try {
						return MemoryStatisticSerializerUtil.readStatisticFile(file.toFile());
					} catch (IOException exception) {
						throw new ActiveViamRuntimeException(exception);
					}
				})
				.collect(Collectors.toList());
	}

	protected void loadStatisticsIntoDatastore(
			final Collection<? extends IMemoryStatistic> statistics, final IDatastore analysisDatastore) {
		analysisDatastore.edit(transactionManager ->
				statistics.forEach(statistic ->
						statistic.accept(
								new FeedVisitor(analysisDatastore.getSchemaMetadata(), transactionManager,
										"test"))));
	}

	@Test
	public void testDictionaryIsShared() {
		final ISchemaDictionaryProvider dictionaries = monitoredDatastore.getDictionaries();
		Assertions.assertThat(dictionaries.getDictionary("Forex", "currency"))
				.isSameAs(dictionaries.getDictionary("Forex", "targetCurrency"));
	}

	@Test
	public void testDictionaryHasAtLeastOneChunk() {
		final long dictionaryId = monitoredDatastore.getDictionaries()
				.getDictionary("Forex", "currency")
				.getDictionaryId();

		final Set<Long> chunkIdsForDictionary = extractChunkIdsForDictionary(dictionaryId);
		Assertions.assertThat(chunkIdsForDictionary)
				.isNotEmpty();
	}

	@Test
	public void testDictionaryChunkFields() {
		final long dictionaryId = monitoredDatastore.getDictionaries()
				.getDictionary("Forex", "currency")
				.getDictionaryId();

		final Set<Long> chunkIdsForDictionary = extractChunkIdsForDictionary(dictionaryId);
		final Map<Long, Multimap<String, String>> fieldsPerChunk =
				extractOwnerAndFieldsPerChunkId(chunkIdsForDictionary);

		SoftAssertions.assertSoftly(assertions -> {
			for (final Multimap<String, String> fieldsForChunk : fieldsPerChunk.values()) {
				assertions.assertThat(fieldsForChunk.keySet())
						.containsOnly("Currency", "Forex");

				assertions.assertThat(fieldsForChunk.get("Currency"))
						.containsExactlyInAnyOrder("currency");
				assertions.assertThat(fieldsForChunk.get("Forex"))
						.containsExactlyInAnyOrder("currency", "targetCurrency");
			}
		});
	}

	protected Set<Long> extractChunkIdsForDictionary(final long dictionaryId) {
		final ICursor cursor = monitoringDatastore.getHead().getQueryRunner()
				.forStore(DatastoreConstants.CHUNK_STORE)
				.withCondition(
						BaseConditions.Equal(DatastoreConstants.CHUNK__PARENT_DICO_ID, dictionaryId))
				.selecting(DatastoreConstants.CHUNK_ID)
				.onCurrentThread()
				.run();

		return StreamSupport.stream(cursor.spliterator(), false)
				.map(reader -> reader.readLong(0))
				.collect(Collectors.toSet());
	}

	protected Map<Long, Multimap<String, String>> extractOwnerAndFieldsPerChunkId(
			final Collection<Long> chunkIdSubset) {
		final ICursor cursor = monitoringDatastore.getHead().getQueryRunner()
				.forStore(DatastoreConstants.OWNER_STORE)
				.withCondition(BaseConditions.And(
						BaseConditions.Equal(DatastoreConstants.OWNER__COMPONENT, ParentType.DICTIONARY),
						BaseConditions.In(DatastoreConstants.OWNER__CHUNK_ID, chunkIdSubset.toArray())))
				.selecting(DatastoreConstants.OWNER__CHUNK_ID, DatastoreConstants.OWNER__OWNER,
						DatastoreConstants.OWNER__FIELD)
				.onCurrentThread()
				.run();

		final Map<Long, Multimap<String, String>> fieldsPerChunk = new HashMap<>();
		for (final IRecordReader reader : cursor) {
			final long chunkId = reader.readLong(0);
			fieldsPerChunk.putIfAbsent(chunkId, HashMultimap.create());
			fieldsPerChunk.get(chunkId).put(
					((ChunkOwner) reader.read(1)).getName(),
					(String) reader.read(2));
		}

		return fieldsPerChunk;
	}
}
