/*
 * (C) Quartet FS 2013-2014
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.cfg.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.monitoring.memory.DatastoreMonitoringDescription;
import com.qfs.multiversion.impl.KeepLastEpochPolicy;
import com.qfs.server.cfg.IDatastoreConfig;
import com.qfs.server.cfg.impl.ActivePivotConfig;
import com.qfs.store.IDatastore;
import com.qfs.store.build.impl.DatastoreBuilder;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotDatastorePostProcessor;

/**
 *
 * Spring configuration of the Datastore.
 *
 * @author Quartet FS
 *
 */
@Configuration
public class MonitoringDatastoreConfig implements IDatastoreConfig {

	/** Spring environment, automatically wired */
	@Autowired
	protected Environment env;

	/** {@link ActivePivotConfig} spring configuration */
	@Autowired
	protected ActivePivotConfig apConfig;

	// ////////////////////////////////////////////////
	// Schema & Datastore
	// ////////////////////////////////////////////////

	/**
	 * @return the {@link IDatastoreSchemaDescription} of the datastore
	 * used to greet statistics.
	 */
	@Bean
	public IDatastoreSchemaDescription schemaDescription() {
		return new DatastoreMonitoringDescription();
	}

	@Override
	@Bean
	public IDatastore datastore() {
		return new DatastoreBuilder()
				.setSchemaDescription(schemaDescription())
				.addSchemaDescriptionPostProcessors(ActivePivotDatastorePostProcessor.createFrom(apConfig.activePivotManagerDescription()))
				.setEpochManagementPolicy(new KeepLastEpochPolicy())
				.build();
	}
}
