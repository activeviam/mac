/*
 * (C) Quartet FS 2013-2014
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.cfg.impl;

import com.activeviam.mac.memory.MemoryAnalysisDatastoreDescription;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.multiversion.IEpochManagementPolicy;
import com.qfs.multiversion.impl.KeepAllEpochPolicy;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *
 * Spring configuration of the Datastore.
 *
 * @author ActiveViam
 *
 */
@Configuration
public class DatastoreDescriptionConfig implements IDatastoreDescriptionConfig {

	@Override
	@Bean
	public IDatastoreSchemaDescription schemaDescription() {
		return new MemoryAnalysisDatastoreDescription();
	}

	@Override
	public IEpochManagementPolicy epochManagementPolicy() {
		// TODO(ope) discuss this: we want to see the change after a GC, how to do the initial load of many exports
		return new KeepAllEpochPolicy();
	}
}
