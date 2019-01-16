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
import com.qfs.monitoring.memory.MemoryAnalysisDatastoreDescription;
import com.qfs.server.cfg.IDatastoreDescriptionConfig;
import com.qfs.server.cfg.impl.ActivePivotConfig;

/**
 *
 * Spring configuration of the Datastore.
 *
 * @author Quartet FS
 *
 */
@Configuration
public class MonitoringDatastoreDescriptionConfig implements IDatastoreDescriptionConfig {

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
	@Override
	@Bean
	public IDatastoreSchemaDescription schemaDescription() {
		return new MemoryAnalysisDatastoreDescription();
	}

}
