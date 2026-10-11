/*
 * (C) ActiveViam 2026
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import com.activeviam.activepivot.core.datastore.api.foundry.config.IModelConfigurer;
import com.activeviam.tech.mvcc.api.policy.IEpochManagementPolicy;
import com.activeviam.tech.mvcc.api.policy.KeepLastEpochPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration of the data model.
 *
 * <p>The Foundry auto-configuration builds the database and the cube manager from the {@link
 * IModelConfigurer} bean.
 *
 * @author ActiveViam
 */
@Configuration
public class ModelConfig {

  @Bean
  public IModelConfigurer modelConfigurer() {
    return new MacModelConfigurer();
  }

  @Bean
  public IEpochManagementPolicy epochManagementPolicy() {
    return new KeepLastEpochPolicy();
  }
}
