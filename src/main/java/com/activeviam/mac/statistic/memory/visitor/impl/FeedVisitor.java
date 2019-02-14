/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.logging.Logger;

import com.activeviam.mac.Loggers;
import com.activeviam.mac.memory.DatastoreConstants;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import com.qfs.multiversion.IEpoch;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.transaction.IOpenedTransaction;

public class FeedVisitor implements IMemoryStatisticVisitor<Void> {

	/** Class logger */
	private static final Logger logger = Logger.getLogger(Loggers.LOADING);

	private final IDatastoreSchemaMetadata storageMetadata;
	private final IOpenedTransaction transaction;
	private final String dumpName;

	public FeedVisitor(
			final IDatastoreSchemaMetadata storageMetadata,
			final IOpenedTransaction tm,
			final String dumpName) {
		this.storageMetadata = storageMetadata;
		this.transaction = tm;
		this.dumpName = dumpName;
	}

	@Override
	public Void visit(DefaultMemoryStatistic stat) {
		switch (stat.getName()) {
				case MemoryStatisticConstants.STAT_NAME_DATASTORE:
				case MemoryStatisticConstants.STAT_NAME_STORE:
					final DatastoreFeederVisitor datastoreFeed = new DatastoreFeederVisitor(
							this.storageMetadata,
							this.transaction,
							this.dumpName);
					stat.accept(datastoreFeed);
					break;
				default :
					logger.warning("Unsupported statistic named " + stat.getName() + ". Ignoring it");
			}

		return null;
	}

	@Override
	public Void visit(ChunkSetStatistic chunkSetStatistic) {
		return null;
	}

	@Override
	public Void visit(ChunkStatistic chunkStatistic) {
		return null;
	}

	@Override
	public Void visit(ReferenceStatistic referenceStatistic) {
		return null;
	}

	@Override
	public Void visit(IndexStatistic indexStatistic) {
		return null;
	}

	@Override
	public Void visit(DictionaryStatistic dictionaryStatistic) {
		return null;
	}

	static void includeApplicationInfoIfAny(
			final IOpenedTransaction tm,
			final Instant date,
			final long epoch,
			final String dumpName,
			final IMemoryStatistic stat) {
		final IStatisticAttribute usedHeap = stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_HEAP_MEMORY);
		final IStatisticAttribute maxHeap = stat.getAttribute(MemoryStatisticConstants.ST$AT_NAME_GLOBAL_MAX_HEAP_MEMORY);
		final IStatisticAttribute usedOffHeap = stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_USED_DIRECT_MEMORY);
		final IStatisticAttribute maxOffHeap = stat.getAttribute(MemoryStatisticConstants.STAT_NAME_GLOBAL_MAX_DIRECT_MEMORY);

		if (usedHeap != null && maxHeap != null && usedOffHeap != null && maxOffHeap != null) {
			tm.add(
					DatastoreConstants.APPLICATION_STORE,
					date,
					epoch,
					dumpName,
					usedHeap.asLong(),
					maxHeap.asLong(),
					usedOffHeap.asLong(),
					maxOffHeap.asLong());
		} else {
			throw new RuntimeException("Missing memory properties in " + stat.getName() + ": " + stat.getAttributes());
		}
	}

}
