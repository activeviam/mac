package com.activeviam.mac.cfg.impl;

import com.activeviam.activepivot.core.datastore.api.builder.ApplicationWithDatastore;
import com.activeviam.activepivot.core.datastore.api.builder.StartBuilding;
import com.activeviam.activepivot.core.intf.api.cube.IActivePivotManager;
import com.activeviam.activepivot.server.spring.api.config.IActivePivotBranchPermissionsManagerConfig;
import com.activeviam.activepivot.server.spring.api.config.IActivePivotConfig;
import com.activeviam.activepivot.server.spring.api.config.IActivePivotManagerDescriptionConfig;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreConfig;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.datastore.api.IDatastore;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class ActivePivotWithDatastoreConfig implements IDatastoreConfig, IActivePivotConfig {

  private final IActivePivotManagerDescriptionConfig apManagerConfig;

  private final IDatastoreSchemaDescriptionConfig datastoreDescriptionConfig;

  private final IActivePivotBranchPermissionsManagerConfig branchPermissionsManagerConfig;

  @Bean
  protected ApplicationWithDatastore applicationWithDatastore() {
    return StartBuilding.application()
        .withDatastore(this.datastoreDescriptionConfig.datastoreSchemaDescription())
        .withManager(this.apManagerConfig.managerDescription())
        .withEpochPolicy(this.apManagerConfig.epochManagementPolicy())
        .withBranchPermissionsManager(
            this.branchPermissionsManagerConfig.branchPermissionsManager())
        .build();
  }

  @Bean
  @Override
  public IActivePivotManager activePivotManager() {
    return applicationWithDatastore().getManager();
  }

  @Bean
  @Override
  public IDatastore database() {
    return applicationWithDatastore().getDatastore();
  }
}
