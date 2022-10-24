/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.scenarios;

import com.activeviam.builders.StartBuilding;
import com.activeviam.database.api.query.AliasedField;
import com.activeviam.database.api.query.ListQuery;
import com.activeviam.database.api.schema.FieldPath;
import com.activeviam.fwk.ActiveViamRuntimeException;
import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.mac.statistic.memory.ATestMemoryStatistic;
import com.activeviam.pivot.utils.ApplicationInTests;
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
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotManagerDescription;
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

  protected static final Path TEMP_DIRECTORY =
      QfsFileTestUtils.createTempDirectory(TestMultipleFieldsDictionary.class);
  @RegisterExtension protected LocalResourcesExtension resources = new LocalResourcesExtension();
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
    final IActivePivotManagerDescription managerDescription = new ActivePivotManagerDescription();

    final ApplicationInTests<IDatastore> application =
        ApplicationInTests.builder()
            .withDatastore(datastoreSchemaDescription)
            .withManager(managerDescription)
            .build();

    this.monitoredDatastore = application.getDatabase();

    this.monitoredManager = application.getManager();

    application.start();
    this.resources.register(this.monitoredManager::stop);
    this.resources.register(this.monitoredDatastore);

    fillApplication();
    performGC();
    exportApplicationMemoryStatistics();

    this.monitoringDatastore = createAnalysisDatastore();
    loadStatisticsIntoDatastore(loadMemoryStatistic(this.statisticsPath), this.monitoringDatastore);
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
                .build())
        .withReference(
            StartBuilding.reference()
                .fromStore("Forex")
                .toStore("Currency")
                .withName("currencyReference")
                .withMapping("currency", "currency")
                .build())
        .withReference(
            StartBuilding.reference()
                .fromStore("Forex")
                .toStore("Currency")
                .withName("targetCurrencyReference")
                .withMapping("targetCurrency", "currency")
                .build())
        .build();
  }

  protected void fillApplication() {
    final String[] currencies = new String[] {"EUR", "GBP", "USD"};

    this.monitoredDatastore.edit(
        transactionManager -> {
          for (final String currency : currencies) {
            transactionManager.add("Currency", currency);
            for (final String targetCurrency : currencies) {
              transactionManager.add("Forex", currency, targetCurrency);
            }
          }
        });
  }

  protected void exportApplicationMemoryStatistics() {
    final IMemoryAnalysisService analysisService =
        new MemoryAnalysisService(this.monitoredDatastore, this.monitoredManager, TEMP_DIRECTORY);
    this.statisticsPath = analysisService.exportMostRecentVersion("memoryStats");
  }

  protected IDatastore createAnalysisDatastore() {
    final IDatastoreSchemaDescription desc =
        new MemoryAnalysisDatastoreDescriptionConfig().datastoreSchemaDescription();
    final IActivePivotManagerDescription manager =
        new ManagerDescriptionConfig().managerDescription();
    return (IDatastore)
        ApplicationInTests.builder().withDatastore(desc).withManager(manager).build().getDatabase();
  }

  protected Collection<IMemoryStatistic> loadMemoryStatistic(final Path path) throws IOException {
    return Files.list(path)
        .map(
            file -> {
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
    ATestMemoryStatistic.feedMonitoringApplication(analysisDatastore, statistics, "test");
  }

  @Test
  public void testDictionaryIsShared() {
    final ISchemaDictionaryProvider dictionaries =
        this.monitoredDatastore.getQueryMetadata().getDictionaries();
    Assertions.assertThat(dictionaries.getDictionary("Forex", "currency"))
        .isSameAs(dictionaries.getDictionary("Forex", "targetCurrency"));
  }

  @Test
  public void testDictionaryHasAtLeastOneChunk() {
    final long dictionaryId =
        this.monitoredDatastore
            .getQueryMetadata()
            .getDictionaries()
            .getDictionary("Forex", "currency")
            .getDictionaryId();

    final Set<Long> chunkIdsForDictionary = extractChunkIdsForDictionary(dictionaryId);
    Assertions.assertThat(chunkIdsForDictionary).isNotEmpty();
  }

  @Test
  public void testDictionaryChunkFields() {
    final long dictionaryId =
        this.monitoredDatastore
            .getQueryMetadata()
            .getDictionaries()
            .getDictionary("Forex", "currency")
            .getDictionaryId();

    final Set<Long> chunkIdsForDictionary = extractChunkIdsForDictionary(dictionaryId);
    final Map<Long, Multimap<String, String>> ownersAndFieldsPerChunk =
        extractOwnersAndFieldsPerChunkId(chunkIdsForDictionary);

    SoftAssertions.assertSoftly(
        assertions -> {
          for (final Multimap<String, String> ownersAndFieldsForChunk :
              ownersAndFieldsPerChunk.values()) {
            assertions
                .assertThat(ownersAndFieldsForChunk.keySet())
                .containsOnly("Currency", "Forex");

            assertions
                .assertThat(ownersAndFieldsForChunk.get("Currency"))
                .containsExactlyInAnyOrder("currency");
            assertions
                .assertThat(ownersAndFieldsForChunk.get("Forex"))
                .containsExactlyInAnyOrder("currency", "targetCurrency");
          }
        });
  }

  protected Set<Long> extractChunkIdsForDictionary(final long dictionaryId) {
    final ListQuery query =
        this.monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(
                    FieldPath.of(DatastoreConstants.CHUNK__PARENT_DICO_ID), dictionaryId))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {

      return StreamSupport.stream(cursor.spliterator(), false)
          .map(reader -> reader.readLong(0))
          .collect(Collectors.toSet());
    }
  }

  protected Map<Long, Multimap<String, String>> extractOwnersAndFieldsPerChunkId(
      final Collection<Long> chunkIdSubset) {
    final ListQuery query =
        this.monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.and(
                    BaseConditions.equal(
                        FieldPath.of(DatastoreConstants.OWNER__COMPONENT), ParentType.DICTIONARY),
                    BaseConditions.in(
                        FieldPath.of(DatastoreConstants.OWNER__CHUNK_ID), chunkIdSubset.toArray())))
            .withAliasedFields(
                AliasedField.fromFieldName(DatastoreConstants.OWNER__CHUNK_ID),
                AliasedField.fromFieldName(DatastoreConstants.OWNER__OWNER),
                AliasedField.fromFieldName(DatastoreConstants.OWNER__FIELD))
            .toQuery();
    try (final ICursor cursor =
        this.monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {

      final Map<Long, Multimap<String, String>> fieldsPerChunk = new HashMap<>();
      for (final IRecordReader reader : cursor) {
        final long chunkId = reader.readLong(0);
        fieldsPerChunk.putIfAbsent(chunkId, HashMultimap.create());
        fieldsPerChunk
            .get(chunkId)
            .put(((ChunkOwner) reader.read(1)).getName(), (String) reader.read(2));
      }

      return fieldsPerChunk;
    }
  }
}
