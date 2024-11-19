/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.activeviam.activepivot.server.intf.api.observability.IMemoryAnalysisService;
import com.activeviam.database.api.conditions.BaseConditions;
import com.activeviam.database.api.query.AliasedField;
import com.activeviam.database.api.query.ListQuery;
import com.activeviam.database.api.schema.FieldPath;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescriptionConfig.ParentType;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.records.api.ICursor;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMemoryStatisticLoadingWithReverseOrder extends ATestMemoryStatistic {

  @Test
  public void testLoadDatastoreStats() {
    createApplication(
        (monitoredDatastore, monitoredManager) -> {
          fillApplication(monitoredDatastore);

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
          final List<? extends AMemoryStatistic> storeStats =
              new ArrayList<>(loadMemoryStatFromFolder(exportPath).getChildren());

          Collections.shuffle(storeStats);

          Assertions.assertThat(storeStats).isNotEmpty();
          assertLoadsCorrectly(storeStats, getClass());
        });
  }

  @Test
  public void testLoadDatastoreStatsWithVectors() {
    createApplicationWithVector(
        true,
        (monitoredDatastore, monitoredManager) -> {
          commitDataInDatastoreWithVectors(monitoredDatastore, false);

          final IMemoryAnalysisService analysisService =
              createService(monitoredDatastore, monitoredManager);
          final Path exportPath = analysisService.exportMostRecentVersion("testLoadDatastoreStats");
          final List<? extends AMemoryStatistic> storeStats =
              new ArrayList<>(loadMemoryStatFromFolder(exportPath).getChildren());

          Collections.shuffle(storeStats);

          Assertions.assertThat(storeStats).isNotEmpty();
          final IDatastore monitoringDatastore = assertLoadsCorrectly(storeStats, getClass());

          final Set<Long> vectorBlocks = extractVectorBlocks(monitoringDatastore);
          assertVectorBlockAttributesArePresent(monitoringDatastore, vectorBlocks);
        });
  }

  protected void assertVectorBlockAttributesArePresent(
      final IDatastore monitoringDatastore, final Collection<Long> chunkIdSubset) {
    ListQuery query =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.and(
                    BaseConditions.in(
                        FieldPath.of(DatastoreConstants.CHUNK_ID), chunkIdSubset.toArray()),
                    BaseConditions.equal(
                        FieldPath.of(DatastoreConstants.CHUNK__VECTOR_BLOCK_LENGTH), null)))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();

    try (final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {
      Assertions.assertThat(StreamSupport.stream(cursor.spliterator(), false).count()).isZero();
    }
  }

  protected Set<Long> extractVectorBlocks(final IDatastore monitoringDatastore) {
    ListQuery query =
        monitoringDatastore
            .getQueryManager()
            .listQuery()
            .forTable(DatastoreConstants.CHUNK_STORE)
            .withCondition(
                BaseConditions.equal(
                    FieldPath.of(DatastoreConstants.OWNER__COMPONENT), ParentType.VECTOR_BLOCK))
            .withAliasedFields(AliasedField.fromFieldName(DatastoreConstants.CHUNK_ID))
            .toQuery();
    try (final ICursor cursor =
        monitoringDatastore.getHead("master").getQueryRunner().listQuery(query).run()) {

      return StreamSupport.stream(cursor.spliterator(), false)
          .map(reader -> reader.readLong(0))
          .collect(Collectors.toSet());
    }
  }
}
