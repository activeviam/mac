/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.copper.HierarchyCoordinate;
import com.activeviam.copper.LevelCoordinate;
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
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IMeasureHierarchy;
import com.quartetfs.fwk.QuartetRuntimeException;

import java.time.Instant;
import java.util.Objects;
import java.util.logging.Logger;

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
	private Long dictionaryId;
	private Long chunkSetId;
	private String dimension;
	private String hierarchy;
	private String level;
	private Integer levelMemberCount;
	private ProviderCpnType providerCpnType;

	protected StatisticTreePrinter printer;

	public PivotFeederVisitor(
			final IDatastoreSchemaMetadata storageMetadata,
			final IOpenedTransaction tm,
			final String dumpName) {
		this.storageMetadata = storageMetadata;
		this.transaction = tm;
		this.dumpName = dumpName;
	}

	public void startFrom(final IMemoryStatistic stat) {
		this.printer = DebugVisitor.createDebugPrinter(stat);
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

			try {
				stat.accept(this);
			} finally {
				this.printer.print();
			}
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
		case PivotMemoryStatisticConstants.STAT_NAME_LEVEL:
			processLevel(stat);
			break;
		case PivotMemoryStatisticConstants.STAT_NAME_LEVEL_MEMBERS:
			processLevelMember(stat);
			break;
		default:
			visitChildren(stat);
		}

		return null;
	}

	@Override
	public Void visit(final ChunkSetStatistic stat) {
		final IRecordFormat format = FeedVisitor.getChunksetFormat(this.storageMetadata);
		final Object[] tuple = FeedVisitor.buildChunksetTupleFrom(format, stat);

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
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__DUMP_NAME, this.dumpName);
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__EXPORT_DATE, this.current);

		if (this.providerId != null) {
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__PROVIDER_ID, this.providerId);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__PARTITION_ID, this.partition);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__TYPE, this.providerCpnType.toString());

			switch (this.providerCpnType) {
				case POINT_INDEX:
					if (this.chunkSetId != null) {
						FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNKSET_ID, this.chunkSetId);
					} else {
						FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.DICTIONARY_ID, this.dictionaryId);
					}
					break;
				case POINT_MAPPING:
					break;
				case AGGREGATE_STORE:
					FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNKSET_ID, this.chunkSetId);
					break;
				case BITMAP_MATCHER:
					break;
			}
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__GROUP, FeedVisitor.GROUP_AGGREGATE_PROVIDER);
		} else if (this.level != null){
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__GROUP, FeedVisitor.GROUP_HIERARCHY);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.DICTIONARY_ID, this.dictionaryId);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__TYPE, FeedVisitor.TYPE_DICTIONARY);
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.CHUNK__FIELD, this.level);
		} else {
			throw new RuntimeException("Unexpected statistic " + stat);
		}

		this.transaction.add(DatastoreConstants.CHUNK_STORE, tuple);

		visitChildren(stat);

		return null;
	}

	@Override
	public Void visit(DictionaryStatistic stat) {
		final IRecordFormat format = FeedVisitor.getDictionaryFormat(this.storageMetadata);
		final Object[] tuple = FeedVisitor.buildDictionaryTupleFrom(format, stat);

		if (this.providerId != null) {
			this.transaction.add(DatastoreConstants.DICTIONARY_STORE, tuple);

			this.dictionaryId = (Long) tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)];
			visitChildren(stat);
			this.dictionaryId = null;
		} else if (this.level != null) {
			// We are processing a hierarchy/level
			tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_IS_LEVEL)] = true;
			this.transaction.add(DatastoreConstants.DICTIONARY_STORE, tuple);

			this.dictionaryId = (Long) tuple[format.getFieldIndex(DatastoreConstants.DICTIONARY_ID)];
			visitChildren(stat);
			// Do not nullify dictionaryId. It is done after visiting the whole level
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

		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.PROVIDER__PIVOT_ID, this.pivot);
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.PROVIDER__MANAGER_ID, this.manager);

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
		String hierarchyDescription = stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_HIERARCHY_ID).asText();
		HierarchyCoordinate hc = HierarchyCoordinate.fromDescription(hierarchyDescription);
		this.dimension = hc.dimension;
		this.hierarchy = hc.hierarchy;

		visitChildren(stat);

		this.hierarchy = null;
	}

	private void processLevel(final IMemoryStatistic stat) {
		final IRecordFormat format = getLevelFormat(this.storageMetadata);
		final Object[] tuple = buildLevelTupleFrom(format, stat);

		String levelDescription = stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_LEVEL_ID).asText();
		LevelCoordinate lc = LevelCoordinate.fromDescription(levelDescription);
		this.level = lc.level;

		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__MANAGER_ID, this.manager);
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__PIVOT_ID, this.pivot);
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__DIMENSION, this.dimension);
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__HIERARCHY, this.hierarchy);
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__LEVEL, this.level);

		visitChildren(stat);

		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__MEMBER_COUNT, this.levelMemberCount);
		if (!this.hierarchy.equals(IMeasureHierarchy.MEASURE_HIERARCHY) && !this.level.equals(ILevelInfo.ClassificationType.ALL.name())) {
			FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.LEVEL__DICTIONARY_ID, this.dictionaryId);
		}

		this.transaction.add(DatastoreConstants.LEVEL_STORE, tuple);

		this.dictionaryId = null;
		this.level = null;
		this.levelMemberCount = null;
	}

	private void processLevelMember(final IMemoryStatistic stat) {
		this.levelMemberCount = stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_LEVEL_MEMBER_COUNT).asInt();
	}

		/**
		 * Visits all the children of the given {@link IMemoryStatistic}.
		 *
		 * @param statistic The statistics whose children to visit.
		 */
	protected void visitChildren(final IMemoryStatistic statistic) {
		if (statistic.getChildren() != null) {
			final ProviderCpnType type = detectProviderComponent(statistic);
			if (type != null) {
				// A provider component is visited.
				this.providerCpnType = type;
				processProviderComponent(statistic);
			}

			for (final IMemoryStatistic child : statistic.getChildren()) {
				child.accept(this);
			}

			if (type != null) {
				// This statistic corresponds to a provider component, Once the children have been visited,
				// nullify the type to proceed the visit.
				this.providerCpnType = null;
			}
		}
	}

	protected void processProviderComponent(IMemoryStatistic statistic) {
		IRecordFormat format = FeedVisitor.getRecordFormat(storageMetadata, DatastoreConstants.PROVIDER_COMPONENT_STORE);
		Object[] tuple = buildProviderComponentTupleFrom(format, statistic);

		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.PROVIDER_COMPONENT__TYPE, this.providerCpnType.toString());
		FeedVisitor.setTupleElement(tuple, format, DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID, this.providerId);

		FeedVisitor.checkTuple(tuple, format);
		this.transaction.add(DatastoreConstants.PROVIDER_COMPONENT_STORE, tuple);
	}

	/**
	 * Returns the provider component that corresponds to the given statistic or null.
	 *
	 * @param stat the statistic
	 * @return the provider component
	 */
	protected ProviderCpnType detectProviderComponent(final IMemoryStatistic stat) {
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

		final IStatisticAttribute indexAttr = stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_INDEX);
		if (indexAttr != null) {
			tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__INDEX)] = indexAttr.asText();
		}
		tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__PROVIDER_ID)] = stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_ID).asLong();
		tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__TYPE)] = stat.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_TYPE).asText(); // JIT, BITMAP, LEAF
		tuple[format.getFieldIndex(DatastoreConstants.PROVIDER__CATEGORY)] = getProviderCategory(stat);

		return tuple;
	}

	private static Object[] buildProviderComponentTupleFrom(
			final IRecordFormat format,
			final IMemoryStatistic stat) {
		final Object[] tuple = new Object[format.getFieldCount()];

		tuple[format.getFieldIndex(DatastoreConstants.PROVIDER_COMPONENT__CLASS)] = stat.getAttribute(MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS).asText();

		return tuple;
	}

	private static Object[] buildLevelTupleFrom(
			final IRecordFormat format,
			final IMemoryStatistic stat) {
		final Object[] tuple = new Object[format.getFieldCount()];

		tuple[format.getFieldIndex(DatastoreConstants.LEVEL__ON_HEAP_SIZE)] = stat.getShallowOnHeap();
		tuple[format.getFieldIndex(DatastoreConstants.LEVEL__OFF_HEAP_SIZE)] = stat.getShallowOffHeap();

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

	@Override
	public Void visit(final ReferenceStatistic stat) {
		throw new UnsupportedOperationException("An ActivePivot cannot contain references. Received: " + stat);
	}

	@Override
	public Void visit(IndexStatistic stat) {
		throw new UnsupportedOperationException("An ActivePivot cannot contain references. Received: " + stat);
	}

}
