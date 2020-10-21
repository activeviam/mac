/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.descriptions;

import com.activeviam.mac.cfg.impl.ManagerDescriptionConfig;
import com.activeviam.mac.statistic.memory.visitor.impl.FeedVisitor;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.store.IDatastore;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import java.util.Collection;
import java.util.Collections;

public class MonitoringApplicationDescription implements ITestApplicationDescription {

	public static final String DEFAULT_DUMP_NAME = "testDump";

	protected final Collection<IMemoryStatistic> statistics;
	protected final String dumpName;
	protected final ManagerDescriptionConfig config = new ManagerDescriptionConfig();

	public MonitoringApplicationDescription(final IMemoryStatistic statistic) {
		this(Collections.singleton(statistic));
	}

	public MonitoringApplicationDescription(final Collection<IMemoryStatistic> statistics) {
		this(statistics, DEFAULT_DUMP_NAME);
	}

	public MonitoringApplicationDescription(
			final Collection<IMemoryStatistic> statistics, final String dumpName) {
		this.statistics = statistics;
		this.dumpName = dumpName;
	}


	@Override
	public IDatastoreSchemaDescription datastoreDescription() {
		return config.schemaDescription();
	}

	@Override
	public IActivePivotManagerDescription managerDescription(
			IDatastoreSchemaDescription schemaDescription) {
		return config.managerDescription();
	}

	@Override
	public void fill(IDatastore datastore) {
		datastore.edit(transactionManager ->
				statistics.forEach(statistic ->
						statistic.accept(
								new FeedVisitor(datastore.getSchemaMetadata(), transactionManager, dumpName))));
	}
}
