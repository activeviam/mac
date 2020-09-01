package com.activeviam.mac.statistic.memory;

import static com.activeviam.mac.memory.DatastoreConstants.CHUNK_STORE;

import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.SharedOwner;
import com.activeviam.mac.memory.DatastoreConstants;
import com.activeviam.mac.statistic.memory.descriptions.MicroApplication;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.store.IDatastore;
import com.qfs.store.query.IDictionaryCursor;
import com.quartetfs.biz.pivot.IActivePivotManager;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSharedMemory extends ASingleAppMonitoringTest {
  @Override
  protected IDatastoreSchemaDescription datastoreSchema() {
    return MicroApplication.datastoreDescription();
  }

  @Override
  protected IActivePivotManagerDescription managerDescription(
      IDatastoreSchemaDescription datastoreSchema) {
    return MicroApplication.managerDescriptionWithLeafBitmap(datastoreSchema);
  }

  @Override
  protected void beforeExport(
      IDatastore datastore, IActivePivotManager manager) {
    datastore.edit(
            tm -> {
              IntStream.range(0, 100)
                  .forEach(
                      i -> {
                        tm.add("A", i * i);
                      });
            });
  }

  /**
   * Tests that importing an application with a store and a pivot containing a level named after a
   * field sets some chunks as "shared" (the Dictionary chunks) as expected
   */
  @Test
  public void testShared() {
    // Query record chunks data :
    final IDictionaryCursor cursor =
        monitoringDatastore
            .getHead()
            .getQueryRunner()
            .forStore(CHUNK_STORE)
            .withCondition(BaseConditions.TRUE)
            .selecting(DatastoreConstants.CHUNK__OWNER)
            .onCurrentThread()
            .run();

    final List<String> list = new ArrayList<>();
    cursor.forEach(
        (record) -> {
          final ChunkOwner owner = (ChunkOwner) record.read(0);
          list.add(owner.getName());
        });

    Assertions.assertThat(list).contains("A", "Cube", SharedOwner.getInstance().getName());
  }
}
