/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import java.time.Instant;
import java.util.Arrays;
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
import com.quartetfs.biz.pivot.cube.hierarchy.IHierarchy;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IMeasureHierarchy;
import com.quartetfs.fwk.QuartetRuntimeException;

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
	private Long chunkSetId;
	private String hierarchy;
	private String level;
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
			if (this.epochId == null && stat.getName().equals(PivotMemoryStatisticConstants.STAT_NAME_MANAGER)) {
				// Look amongst the children to find the epoch
				for (final IMemoryStatistic child : stat.getChildren()) {
					readEpochAndBranchIfAny(child);
					if (this.epochId != null) {
						break;
					}
				}
			}

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
		readEpochAndBranchIfAny(stat);

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
			break;
		case PivotMemoryStatisticConstants.STAT_NAME_HIERARCHY:
			processHierarchy(stat);
			break;
		case PivotMemoryStatisticConstants.STAT_NAME_HIERARCHY_LEVEL:
			processLevel(stat);
			break;
		default:
			recordStatAndExplore(stat);
		}

		return null;
	}

	@Override
	public Void visit(final ChunkSetStatistic stat) {
		final IRecordFormat format = FeedVisitor.getChunksetFormat(this.storageMetadata);
		final Object[] tuple = FeedVisitor.buildChunksetTupleFrom(format, stat);

		assert this.epochId != null;
		tuple[format.getFieldIndex(DatastoreConstants.EPOCH_ID)] = this.epochId;
		final String type;
		if (this.cpnType != null) {
			assert this.providerId != null;
			tuple[format.getFieldIndex(DatastoreConstants.CHUNK__PROVIDER_ID)] = this.providerId;
			assert this.partition != null;
			tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET__PARTITION)] = this.partition;
			tuple[format.getFieldIndex(DatastoreConstants.CHUNK__PROVIDER_COMPONENT_TYPE)] = this.cpnType;
			type = this.cpnType.toString();
		} else if (this.dictionaryId != null) {
			tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET__DICTIONARY_ID)] = this.dictionaryId;
			type = FeedVisitor.TYPE_DICTIONARY;
		} else {
			throw new RuntimeException("Cannot process this stat. A chunkset is not attached to a dictionary or a provider component. Faulty stat: " + stat);
		}
		tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET__TYPE)] = type;

		this.transaction.add(DatastoreConstants.CHUNKSET_STORE, tuple);

		this.chunkSetId = (Long) tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET_ID)];
		visitChildren(stat);
		this.chunkSetId = null;

		return null;
	}

	@Override
	public Void visit(final ChunkStatistic stat) {
		final IRecordFormat format = getChunkFormat(this.storageMetadata);
		final Object[] tuple = FeedVisitor.buildChunkTupleFrom(format, stat);
		tuple[format.getFieldIndex(DatastoreConstants.EPOCH_ID)] = this.epochId;
		tuple[format.getFieldIndex(DatastoreConstants.CHUNK__DUMP_NAME)] = this.dumpName;
		tuple[format.getFieldIndex(DatastoreConstants.CHUNK__EXPORT_DATE)] = this.current;

		final String type;
		if (this.chunkSetId != null) {
			tuple[format.getFieldIndex(DatastoreConstants.CHUNKSET_ID)] = chunkSetId;
			type = FeedVisitor.TYPE_RECORD;
		} else if (this.indexId != null) {
			tuple[format.getFieldIndex(DatastoreConstants.INDEX_ID)] = indexId;
			type = FeedVisitor.TYPE_INDEX;
		} else if (this.dictionaryId != null) {
			tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)] = dictionaryId;
			type = FeedVisitor.TYPE_DICTIONARY;
		} else if (isVersionColumn) {
			type = FeedVisitor.TYPE_VERSION_COLUMN;
		} else if (this.cpnType != null) {
			type = this.cpnType.toString();
		} else {
			type = null;
		}
		tuple[format.getFieldIndex(DatastoreConstants.CHUNK__TYPE)] = type;

		this.transaction.add(DatastoreConstants.CHUNK_STORE, tuple);

		visitChildren(stat);

		return null;
	}

	@Override
	public Void visit(final ReferenceStatistic stat) {
		throw new UnsupportedOperationException("An ActivePivot cannot contain references. Received: " + stat);
	}

	@Override
	public Void visit(IndexStatistic stat) {
		throw new UnsupportedOperationException("An ActivePivot cannot contain references. Received: " + stat);
	}

	@Override
	public Void visit(DictionaryStatistic stat) {
		final IRecordFormat format = FeedVisitor.getDictionaryFormat(this.storageMetadata);
		final Object[] tuple = FeedVisitor.buildDictionaryTupleFrom(format, stat);

		if (this.providerId != null) {
			// We are process a dictionary from a provider
			tuple[format.getFieldIndex(DatastoreConstants.DIC__PROVIDER_ID)] = this.providerId;
			assert this.partition != null;
			tuple[format.getFieldIndex(DatastoreConstants.DIC__PROVIDER_PARTITION_ID)] = this.partition;
			assert this.cpnType != null;

			final ProviderCpnType type = detectProviderComponent(stat);
			if (type != null) {
				assert this.cpnType == null;
				this.cpnType = type;
			}
			tuple[format.getFieldIndex(DatastoreConstants.DIC__PROVIDER_COMPONENT_TYPE)] = this.cpnType;

			this.transaction.add(DatastoreConstants.DICTIONARY_STORE, tuple);

			this.dictionaryId = (Long) tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)];
			visitChildren(stat);
			this.dictionaryId = null;
			if (type != null) {
				this.cpnType = null;
			}
		} else if (this.hierarchy != null) {
			// We are processing a hierarchy
			// TODO(ope) complete the level with the dictionary it's using

			this.transaction.add(DatastoreConstants.DICTIONARY_STORE, tuple);

			this.dictionaryId = (Long) tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)];
			visitChildren(stat);
			this.dictionaryId = null;
		} else {
			throw new QuartetRuntimeException("Unexpected stat on dictionary: " + stat);
		}

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

	private void processHierarchy(final IMemoryStatistic stat) {
		this.hierarchy = stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_HIERARCHY_ID).asText();

		visitChildren(stat);

		this.hierarchy = null;
	}

	private void processLevel(final IMemoryStatistic stat) {
		final IRecordFormat format = getLevelFormat(this.storageMetadata);
		final Object[] tuple = buildLevelTupleFrom(format, stat);

		tuple[format.getFieldIndex(DatastoreConstants.LEVEL__MANAGER_ID)] = this.manager;
		tuple[format.getFieldIndex(DatastoreConstants.LEVEL__PIVOT_ID)] = this.pivot;

		// TODO(ope) we don't have any information about the dictionary

		assert stat.getChildren() == null || stat.getChildren().isEmpty();
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

	private static Object[] buildLevelTupleFrom(
			final IRecordFormat format,
			final IMemoryStatistic stat) {
		final Object[] tuple = new Object[format.getFieldCount()];
		final String levelId = stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_LEVEL_ID).asText();

		final String[] parts;
		if (levelId.equals(IMeasureHierarchy.MEASURES_LEVEL_NAME)) {
			parts = new String[3];
			Arrays.fill(parts, IHierarchy.MEASURES);
		} else {
			parts = levelId.split("\\\\");
			assert parts.length == 3;
		}

		tuple[format.getFieldIndex(DatastoreConstants.LEVEL__DIMENSION)] = parts[0];
		tuple[format.getFieldIndex(DatastoreConstants.LEVEL__HIERARCHY)] = parts[1];
		tuple[format.getFieldIndex(DatastoreConstants.LEVEL__LEVEL)] = parts[2];

		tuple[format.getFieldIndex(DatastoreConstants.LEVEL__ON_HEAP_SIZE)] = stat.getShallowOnHeap();
		tuple[format.getFieldIndex(DatastoreConstants.LEVEL__OFF_HEAP_SIZE)] = stat.getShallowOffHeap();
		tuple[format.getFieldIndex(DatastoreConstants.LEVEL__MEMBER_COUNT)] =
			stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_LEVEL_MEMBER_COUNT).asInt();

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

	private static IRecordFormat getChunkFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.CHUNK_STORE);
	}

	private static IRecordFormat getProviderFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.PROVIDER_STORE);
	}

	private static IRecordFormat getLevelFormat(IDatastoreSchemaMetadata storageMetadata) {
		return FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.LEVEL_STORE);
	}

}
