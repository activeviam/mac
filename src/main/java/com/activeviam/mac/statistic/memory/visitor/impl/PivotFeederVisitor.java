/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import java.time.Instant;
import java.util.Objects;
import java.util.logging.Logger;

import com.activeviam.mac.Loggers;
import com.activeviam.mac.memory.DatastoreConstants;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.PivotMemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import com.qfs.store.IDatastoreSchemaMetadata;
import com.qfs.store.record.IRecordFormat;
import com.qfs.store.transaction.IOpenedTransaction;

public class PivotFeederVisitor implements IMemoryStatisticVisitor<Void> {

	/** Class logger */
	private static final Logger logger = Logger.getLogger(Loggers.ACTIVEPIVOT_LOADING);

	private final IDatastoreSchemaMetadata storageMetadata;
	private final IOpenedTransaction transaction;
	private final String dumpName;

	/** The export date, found on the first statistics we read */
	protected Instant current = null;
	/** The epoch id we are currently reading statistics for */
	protected Long epochId = null;
	protected String branch = null;

	private String manager;
	private String pivot;
	private Long providerId;
	private Integer partition;
	private Long indexId;
	private Long dictionaryId;
	private ProviderCpnType cpnType;
	private boolean isVersionColumn;

	public PivotFeederVisitor(
			final IDatastoreSchemaMetadata storageMetadata,
			final IOpenedTransaction tm,
			final String dumpName) {
		this.storageMetadata = storageMetadata;
		this.transaction = tm;
		this.dumpName = dumpName;
	}

	public void startFrom(final IMemoryStatistic stat) {
		if (this.current == null) {
			final IStatisticAttribute dateAtt = stat.getAttribute(DatastoreConstants.CHUNK__EXPORT_DATE);
			if (dateAtt == null) {
				throw new IllegalStateException("First level statistic should contain the export date.");
			}
			this.current = Instant.ofEpochSecond(dateAtt.asLong());

			readEpochAndBranchIfAny(stat);
			assert this.epochId != null;

			FeedVisitor.includeApplicationInfoIfAny(
					this.transaction,
					this.current,
					this.epochId,
					this.dumpName,
					stat);

			stat.accept(this);
		} else {
			throw new RuntimeException("Cannot reuse a feed instance");
		}
	}

	@Override
	public Void visit(final DefaultMemoryStatistic stat) {
		final IStatisticAttribute epochAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_EPOCH);
		if (epochAttr != null) {
			this.epochId = epochAttr.asLong();
		}
		final IStatisticAttribute branchAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_BRANCH);
		if (branchAttr != null) {
			this.branch = branchAttr.asText();
		}

		switch (stat.getName()) {
		case PivotMemoryStatisticConstants.STAT_NAME_MANAGER:
			processManager(stat);
			break;
		case PivotMemoryStatisticConstants.STAT_NAME_PIVOT:
			processPivot(stat);
			break;
		case PivotMemoryStatisticConstants.STAT_NAME_PROVIDER:
			processFullProvider(stat);
			break;
		case PivotMemoryStatisticConstants.STAT_NAME_FULL_PROVIDER:
		case PivotMemoryStatisticConstants.STAT_NAME_PARTIAL_PROVIDER:
			processProvider(stat);
			break;
		case PivotMemoryStatisticConstants.STAT_NAME_PROVIDER_PARTITION:
			processPartition(stat);
		default:
			recordStatAndExplore(stat);
		}

		this.epochId = null;
		this.branch = null;

		return null;
	}

	@Override
	public Void visit(final ChunkSetStatistic stat) {
		// FIXME(ope) later work to do here
		throw new UnsupportedOperationException("Not implemented yet");
//		return null;
	}

	@Override
	public Void visit(final ChunkStatistic stat) {
		// FIXME(ope) later work to do here
		throw new UnsupportedOperationException("Not implemented yet");
//		return null;
	}

	@Override
	public Void visit(final ReferenceStatistic stat) {
		throw new UnsupportedOperationException("An ActivePivot cannot contain references. Received: " + stat);
	}

	@Override
	public Void visit(IndexStatistic indexStatistic) {
		// FIXME(ope) later work to do here
		throw new UnsupportedOperationException("Not implemented yet");
//		return null;
	}

	@Override
	public Void visit(DictionaryStatistic stat) {
		final IRecordFormat format = FeedVisitor.getDictionaryFormat(this.storageMetadata);
		final Object[] tuple = FeedVisitor.buildDictionaryTupleFrom(format, stat);

		assert this.providerId != null;
		tuple[format.getFieldIndex(DatastoreConstants.DIC__PROVIDER_ID)] = this.providerId;
		assert this.partition != null;
		tuple[format.getFieldIndex(DatastoreConstants.DIC__PROVIDER_PARTITION_ID)] = this.partition;
		assert this.cpnType != null;
		tuple[format.getFieldIndex(DatastoreConstants.DIC__PROVIDER_COMPONENT_TYPE)] = this.cpnType.name();

		this.transaction.add(DatastoreConstants.DICTIONARY_STORE, tuple);

		this.dictionaryId = (Long) tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)];
		visitChildren(stat);
		this.dictionaryId = null;

		return null;
	}

	private void processManager(final IMemoryStatistic stat) {
		final IStatisticAttribute idAttr = Objects.requireNonNull(
				stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_MANAGER_ID),
				() -> "No manager id in " + stat);
		this.manager =  idAttr.asText();

		visitChildren(stat);

		this.manager = null;
	}

	private void processPivot(final IMemoryStatistic stat) {
		final IStatisticAttribute idAttr = Objects.requireNonNull(
				stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PIVOT_ID),
				() -> "No pivot id in " + stat);
		this.pivot = idAttr.asText();
		final IStatisticAttribute managerAttr = stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_MANAGER_ID);
		if (managerAttr != null) {
			assert this.manager == null;
			this.manager = managerAttr.asText();
		}

		visitChildren(stat);

		this.pivot = null;
		if (managerAttr != null) {
			this.manager = null;
		}
	}

	private void processFullProvider(final IMemoryStatistic stat) {
		// Ignore this provider, only consider its underlying providers
		visitChildren(stat);
	}

	private void processProvider(final IMemoryStatistic stat) {
		final IRecordFormat format = getProviderFormat(this.storageMetadata);
		final Object[] tuple = buildProviderTupleFrom(format, stat);

		assert this.pivot != null;
		tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__PIVOT_ID)] = this.pivot;
		assert this.manager != null;
		tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__MANAGER_ID)] = this.manager;

		this.transaction.add(DatastoreConstants.PROVIDER_STORE, tuple);

		this.providerId = (Long) tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__PROVIDER_ID)];
		visitChildren(stat);
		this.providerId = null;
	}

	private void processPartition(final IMemoryStatistic stat) {
		final IStatisticAttribute idAttr = Objects.requireNonNull(
				stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_PARTITION_ID),
				() -> "No partition id in " + stat);
		assert this.partition == null;
		this.partition = idAttr.asInt();

		visitChildren(stat);

		this.partition = null;
	}

	private void recordStatAndExplore(final DefaultMemoryStatistic stat) {
		final ProviderCpnType type = detectProviderComponent(stat);
		if (type != null) {
			this.cpnType = type;
		}

		isVersionColumn = MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN.equals(stat.getName());
		visitChildren(stat);
		isVersionColumn = false;
		if (type != null) {
			this.cpnType = null;
		}
	}

	// TODO(ope) shared with DatastoreFeederVisitor
	/**
	 * Visits all the children of the given {@link IMemoryStatistic}.
	 *
	 * @param statistic The statistics whose children to visit.
	 */
	protected void visitChildren(final IMemoryStatistic statistic) {
		if (statistic.getChildren() != null) {
			for (final IMemoryStatistic child : statistic.getChildren()) {
				child.accept(this);
			}
		}
	}

	private ProviderCpnType detectProviderComponent(final IMemoryStatistic stat) {
		switch (stat.getName()) {
		case PivotMemoryStatisticConstants.STAT_NAME_POINT_INDEX:
			return ProviderCpnType.POINT_INDEX;
		case PivotMemoryStatisticConstants.STAT_NAME_POINT_MAPPING:
			return ProviderCpnType.POINT_MAPPING;
		case PivotMemoryStatisticConstants.STAT_NAME_AGGREGATE_STORE:
			return ProviderCpnType.AGGREGATE_STORE;
		case PivotMemoryStatisticConstants.STAT_NAME_BITMAP_MATCHER:
			return ProviderCpnType.BITMAP_MATCHER;
		default:
			return null;
		}
	}

	private void readEpochAndBranchIfAny(final IMemoryStatistic stat) {
		final IStatisticAttribute epochAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_EPOCH);
		if (epochAttr != null) {
			final Long epoch = epochAttr.asLong();
			assert this.epochId == null || epoch.equals(this.epochId);
			this.epochId = epoch;
		}
		final IStatisticAttribute branchAttr = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_BRANCH);
		if (branchAttr != null) {
			final String branch = branchAttr.asText();
			assert this.branch == null || this.branch.equals(branch);
			this.branch = branch;
		}
	}

	private static Object[] buildProviderTupleFrom(
			final IRecordFormat format,
			final IMemoryStatistic stat) {
		final Object[] tuple = new Object[format.getFieldCount()];

		tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__PROVIDER_ID)] =
				stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_ID).asLong();

		final IStatisticAttribute indexAttr = stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_INDEX);
		if (indexAttr != null) {
			tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__INDEX)] = indexAttr.asText();
		}
		tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__TYPE)] =
				stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_TYPE).asText();
		tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__CATEGORY)] = getProviderCategory(stat);

		return tuple;
	}

	private static String getProviderCategory(final IMemoryStatistic stat) {
		switch (stat.getName()) {
		case PivotMemoryStatisticConstants.STAT_NAME_FULL_PROVIDER:
			return "Full";
		case PivotMemoryStatisticConstants.STAT_NAME_PARTIAL_PROVIDER:
			return "Partial";
		case PivotMemoryStatisticConstants.STAT_NAME_PROVIDER:
			return "Unique";
		default:
			throw new IllegalArgumentException("Unsupported provider type " + stat);
		}
	}

	private static IRecordFormat getProviderFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.PROVIDER_STORE);
	}

}
