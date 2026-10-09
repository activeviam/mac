package com.activeviam.mac.cfg.impl;

import com.activeviam.activepivot.core.datastore.api.builder.ApplicationWithDatastore;
import com.activeviam.activepivot.core.datastore.api.builder.StartBuilding;
import com.activeviam.activepivot.core.intf.api.cube.IActivePivotManager;
import com.activeviam.activepivot.core.intf.api.description.IActivePivotManagerDescription;
import com.activeviam.database.datastore.api.IDatastore;
import com.activeviam.database.datastore.api.description.IDatastoreSchemaDescription;
import com.activeviam.tech.mvcc.api.policy.KeepLastEpochPolicy;
import com.activeviam.tech.mvcc.api.security.IBranchPermissionsManager;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
@RequiredArgsConstructor
public class ActivePivotWithDatastoreConfig {

  private final IActivePivotManagerDescription managerDescription;

  private final IDatastoreSchemaDescription datastoreSchemaDescription;

  private final IBranchPermissionsManager branchPermissionsManager;

  @Bean
  protected ApplicationWithDatastore applicationWithDatastore() {
    return StartBuilding.application()
        .withDatastore(this.datastoreSchemaDescription)
        .withManager(this.managerDescription)
        .withEpochPolicy(new KeepLastEpochPolicy())
        .withBranchPermissionsManager(this.branchPermissionsManager)
        .build();
  }

  @Bean
  public IActivePivotManager activePivotManager() {
    return applicationWithDatastore().getManager();
  }

  @Bean
  @DependsOn("poolsCleaner")
  public IDatastore database() {
    return applicationWithDatastore().getDatastore();
  }
}
