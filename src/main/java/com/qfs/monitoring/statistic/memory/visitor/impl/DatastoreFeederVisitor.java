/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.statistic.memory.visitor.impl;

import com.qfs.monitoring.memory.MemoryAnalysisDatastoreDescription;
import com.qfs.monitoring.memory.MemoryAnalysisDatastoreDescription.StringArrayObject;
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
import com.qfs.store.IDatastore;
import com.qfs.store.IRecordSet;
import com.qfs.store.IStoreFormat;
import com.qfs.store.impl.ChunkSet;
import com.qfs.store.record.IByteRecordFormat;
import com.qfs.store.transaction.ITransactionManager;

import java.time.Instant;

import static com.qfs.monitoring.memory.DatastoreConstants.CHUNKSET_ID;
import static com.qfs.monitoring.memory.DatastoreConstants.CHUNK_CLASS;
import static com.qfs.monitoring.memory.DatastoreConstants.CHUNK_ID;
import static com.qfs.monitoring.memory.DatastoreConstants.CHUNK_OFF_HEAP_SIZE;
import static com.qfs.monitoring.memory.DatastoreConstants.CHUNK_ON_HEAP_SIZE;
import static com.qfs.monitoring.memory.DatastoreConstants.CHUNK_SET_CLASS;
import static com.qfs.monitoring.memory.DatastoreConstants.CHUNK_SET_FREE_ROWS;
import static com.qfs.monitoring.memory.DatastoreConstants.CHUNK_SET_PHYSICAL_CHUNK_SIZE;
import static com.qfs.monitoring.memory.DatastoreConstants.CHUNK_SET_STORE;
import static com.qfs.monitoring.memory.DatastoreConstants.CHUNK_STORE;
import static com.qfs.monitoring.memory.DatastoreConstants.CHUNK_TYPE;
import static com.qfs.monitoring.memory.DatastoreConstants.DICTIONARY_CLASS;
import static com.qfs.monitoring.memory.DatastoreConstants.DICTIONARY_ID;
import static com.qfs.monitoring.memory.DatastoreConstants.DICTIONARY_ORDER;
import static com.qfs.monitoring.memory.DatastoreConstants.DICTIONARY_SIZE;
import static com.qfs.monitoring.memory.DatastoreConstants.DICTIONARY_STORE;
import static com.qfs.monitoring.memory.DatastoreConstants.DUMP_NAME;
import static com.qfs.monitoring.memory.DatastoreConstants.EPOCH_ID;
import static com.qfs.monitoring.memory.DatastoreConstants.EXPORT_DATE;
import static com.qfs.monitoring.memory.DatastoreConstants.FIELDS;
import static com.qfs.monitoring.memory.DatastoreConstants.INDEX_CLASS;
import static com.qfs.monitoring.memory.DatastoreConstants.INDEX_ID;
import static com.qfs.monitoring.memory.DatastoreConstants.INDEX_STORE;
import static com.qfs.monitoring.memory.DatastoreConstants.REFERENCE_CLASS;
import static com.qfs.monitoring.memory.DatastoreConstants.REFERENCE_FROM_STORE;
import static com.qfs.monitoring.memory.DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID;
import static com.qfs.monitoring.memory.DatastoreConstants.REFERENCE_ID;
import static com.qfs.monitoring.memory.DatastoreConstants.REFERENCE_NAME;
import static com.qfs.monitoring.memory.DatastoreConstants.REFERENCE_STORE;
import static com.qfs.monitoring.memory.DatastoreConstants.REFERENCE_TO_STORE;
import static com.qfs.monitoring.memory.DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID;
import static com.qfs.monitoring.memory.DatastoreConstants.STORE_AND_PARTITION_PARTITION_ID;
import static com.qfs.monitoring.memory.DatastoreConstants.STORE_AND_PARTITION_STORE_NAME;
import static com.qfs.monitoring.statistic.memory.MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS;
import static com.qfs.monitoring.statistic.memory.MemoryStatisticConstants.ATTR_NAME_DICTIONARY_ID;

/**
 * This visitor is not reusable.
 * <p>
 * This visits memory statistics and commits them into the given datastore. The datastore must have
 * the schema defined by {@link MemoryAnalysisDatastoreDescription}.
 *
 * @author Quartet FS
 */
public class DatastoreFeederVisitor implements IMemoryStatisticVisitor<Void> {

	/** Type name if the memory usage comes from an {@link IRecordSet} */
	public static final String TYPE_RECORD = "record";
	/** Type name if the memory usage comes from references */
	public static final String TYPE_REFERENCE = "reference";
	/** Type name if the memory usage comes from indexes */
	public static final String TYPE_INDEX = "index";
	/** Type name if the memory usage comes from dictionaries */
	public static final String TYPE_DICTIONARY = "dictionary";
	/** Type name if the memory usage comes from the version chunks */
	public static final String TYPE_QFS_VERSION = MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN;

	/**
	 * A boolean that if true tells us that the currently visited component is responsible for
	 * storing versioning data.
	 */
	protected boolean isQfsVersion = false; // FIXME find a cleaner way to do that. (specific stat for instance).

	/** The datastore to which tuple can be added. */
	protected final IDatastore datastore;
	/** The transaction manager to which tuple can be added. */
	protected final ITransactionManager tm;
	/** The record format of the store that stores the chunks. */
	protected final IByteRecordFormat chunkRecordFormat;
	/** The name of the off-heap dump. Can be null */
	protected final String dumpName;

	/** The current record, that will be added to the transaction manager when visiting a leaf. */
	protected Object[] currentChunkRecord;
	/** The export date, found on the first statistics we read */
	protected Instant current = null;
	/** The epoch id we are currently reading statistics for */
	protected Long epochId = null;
	/** ID of the current {@link ChunkSet}. */
	protected Long chunkSetId = null;
	/** ID of the current reference. */
	protected Long referenceId = null;
	/** ID of the current index. */
	protected Long indexId = null;
	/** ID of the current dictionary. */
	protected Long dictionaryId = null;

	/**
	 * Constructor.
	 *
	 * @param datastore the datastore to fill with the off-heap dump data
	 * @param dumpName The name of the off-heap dump. Can be null.
	 */
	public DatastoreFeederVisitor(final IDatastore datastore, final String dumpName) {
		this.datastore = datastore;
		this.tm = datastore.getTransactionManager();
		this.chunkRecordFormat = datastore.getSchemaMetadata()
				.getStoreMetadata(CHUNK_STORE)
				.getStoreFormat()
				.getRecordFormat();
		this.dumpName = dumpName;
	}

	@Override
	public Void visit(final ChunkStatistic chunkStatistic) {
		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNK_ID)] = chunkStatistic.getChunkId();

		String type = null;
		if (chunkSetId!=null) {
			currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNKSET_ID)] = chunkSetId;
			type = TYPE_RECORD;
		} else if (referenceId!=null) {
			currentChunkRecord[chunkRecordFormat.getFieldIndex(REFERENCE_ID)] = referenceId;
			type = TYPE_REFERENCE;
		} else if (indexId!=null) {
			currentChunkRecord[chunkRecordFormat.getFieldIndex(INDEX_ID)] = indexId;
			type = TYPE_INDEX;
		} else if (dictionaryId!=null) {
			currentChunkRecord[chunkRecordFormat.getFieldIndex(DICTIONARY_ID)] = dictionaryId;
			type = TYPE_DICTIONARY;
		} else if (isQfsVersion) {
			type = TYPE_QFS_VERSION;
		}

		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNK_TYPE)] = type;
		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNK_CLASS)] = chunkStatistic.getAttributes()
				.get(ATTR_NAME_CREATOR_CLASS)
				.asText();
		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNK_OFF_HEAP_SIZE)] = chunkStatistic
				.getShallowOffHeap();
		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNK_ON_HEAP_SIZE)] = chunkStatistic
				.getShallowOnHeap();
		currentChunkRecord[chunkRecordFormat.getFieldIndex(DUMP_NAME)] = dumpName;

		tm.add(CHUNK_STORE, currentChunkRecord);

		visitChildren(chunkStatistic);

		// Reset
		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNK_ID)] = null;
		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNKSET_ID)] = null;
		currentChunkRecord[chunkRecordFormat.getFieldIndex(REFERENCE_ID)] = null;
		currentChunkRecord[chunkRecordFormat.getFieldIndex(INDEX_ID)] = null;
		currentChunkRecord[chunkRecordFormat.getFieldIndex(DICTIONARY_ID)] = null;
		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNK_TYPE)] = null;
		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNK_CLASS)] = null;
		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNK_OFF_HEAP_SIZE)] = null;
		currentChunkRecord[chunkRecordFormat.getFieldIndex(CHUNK_ON_HEAP_SIZE)] = null;

		return null;
	}

	@Override
	public Void visit(final DefaultMemoryStatistic defaultMemoryStatistic) {
		// The first Statistic should contain the export date.
		// We assume the first visited statistic is a Composite.
		if (current == null) {
			final IStatisticAttribute dateAtt = defaultMemoryStatistic.getAttributes().get(EXPORT_DATE);
			if (dateAtt==null){
				throw new IllegalStateException("First level statistic should contain the export date.");
			}
			current = Instant.ofEpochSecond(dateAtt.asLong());
		}
		if (defaultMemoryStatistic.getAttributes().containsKey(MemoryStatisticConstants.ATTR_NAME_EPOCH)) {
			epochId = defaultMemoryStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_EPOCH).asLong();
		}

		if (defaultMemoryStatistic.getAttributes().containsKey(STORE_AND_PARTITION_STORE_NAME)) {
			currentChunkRecord = new Object[chunkRecordFormat.getFieldCount()];
			// Set the store name
			currentChunkRecord[chunkRecordFormat
					.getFieldIndex(STORE_AND_PARTITION_STORE_NAME)] = defaultMemoryStatistic.getAttributes()
							.get(STORE_AND_PARTITION_STORE_NAME)
							.asText();
			currentChunkRecord[chunkRecordFormat.getFieldIndex(EXPORT_DATE)] = current;
		} else if (defaultMemoryStatistic.getAttributes().containsKey(STORE_AND_PARTITION_PARTITION_ID)) {
			// Set the partition id
			currentChunkRecord[chunkRecordFormat
					.getFieldIndex(STORE_AND_PARTITION_PARTITION_ID)] = defaultMemoryStatistic.getAttributes()
							.get(STORE_AND_PARTITION_PARTITION_ID)
							.asInt();
		}

		isQfsVersion = MemoryStatisticConstants.STAT_NAME_VERSIONS_COLUMN.equals(defaultMemoryStatistic.getName());

		visitChildren(defaultMemoryStatistic);

		isQfsVersion = false;

		// Reset
		if (defaultMemoryStatistic.getAttributes().containsKey(STORE_AND_PARTITION_STORE_NAME)) {
			currentChunkRecord = null;
		} else if (defaultMemoryStatistic.getAttributes().containsKey(STORE_AND_PARTITION_PARTITION_ID)) {
			currentChunkRecord[chunkRecordFormat.getFieldIndex(STORE_AND_PARTITION_PARTITION_ID)] = null;
		}

		return null;
	}

	@Override
	public Void visit(final ChunkSetStatistic chunkSetStatistic) {
		IStatisticAttribute chunkSetIdAttr = chunkSetStatistic.getAttributes().get(MemoryStatisticConstants.ATTR_NAME_CHUNKSET_ID);
		chunkSetId=chunkSetIdAttr.asLong();

		tm.add(
				CHUNK_SET_STORE,
				chunkSetId,
				epochId,
				chunkSetStatistic.getAttributes().get(CHUNK_SET_CLASS).asText(),
				chunkSetStatistic.getAttributes().get(CHUNK_SET_PHYSICAL_CHUNK_SIZE).asInt(),
				chunkSetStatistic.getAttributes().get(CHUNK_SET_FREE_ROWS).asInt());

		if (chunkSetStatistic.getChildren() != null) {
			for (final IMemoryStatistic child : chunkSetStatistic.getChildren()) {
				// Could be the rowMapping if SparseChunkSet
				final IStatisticAttribute keyAtt = child.getAttributes().get(MemoryStatisticConstants.ATTR_NAME_FIELD);
				if (keyAtt != null) {
					currentChunkRecord[chunkRecordFormat.getFieldIndex(FIELDS)] =
							new StringArrayObject(keyAtt.asText());
				} else {
					currentChunkRecord[chunkRecordFormat.getFieldIndex(FIELDS)] = null;
				}
				child.accept(this);
			}
		}

		// Reset
		currentChunkRecord[chunkRecordFormat.getFieldIndex(FIELDS)] = null;
		chunkSetId=null;

		return null;
	}

	/**
	 * Build an object array that represents a reference.
	 *
	 * @param referenceStatistic the {@link ReferenceStatistic} from which the array must be built.
	 * @return the object array.
	 */
	protected Object[] buildPartOfReferenceStatisticTuple(final ReferenceStatistic referenceStatistic) {
		final IStoreFormat refStoreFormat = datastore.getSchemaMetadata()
				.getStoreMetadata(REFERENCE_STORE)
				.getStoreFormat();
		final Object[] tuple = new Object[refStoreFormat.getRecordFormat().getFieldCount()];
		tuple[refStoreFormat.getRecordFormat().getFieldIndex(
				REFERENCE_FROM_STORE)] = referenceStatistic.getAttribute(REFERENCE_FROM_STORE).asText();
		tuple[refStoreFormat.getRecordFormat().getFieldIndex(REFERENCE_FROM_STORE_PARTITION_ID)] =
				referenceStatistic.getAttribute(REFERENCE_FROM_STORE_PARTITION_ID).asInt();
		tuple[refStoreFormat.getRecordFormat()
				.getFieldIndex(REFERENCE_TO_STORE)] = referenceStatistic.getAttribute(REFERENCE_TO_STORE).asText();
		tuple[refStoreFormat.getRecordFormat().getFieldIndex(
				REFERENCE_TO_STORE_PARTITION_ID)] = referenceStatistic.getAttribute(REFERENCE_TO_STORE_PARTITION_ID)
						.asInt();
		return tuple;
	}

	@Override
	public Void visit(final ReferenceStatistic referenceStatistic) {
		final Object[] tuple = buildPartOfReferenceStatisticTuple(referenceStatistic);
		final IStoreFormat refStoreFormat = datastore.getSchemaMetadata()
				.getStoreMetadata(REFERENCE_STORE)
				.getStoreFormat();

		// fill out the tuple
		referenceId = referenceStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_REFERENCE_ID).asLong();
		tuple[refStoreFormat.getRecordFormat().getFieldIndex(REFERENCE_ID)] = referenceId;
		tuple[refStoreFormat.getRecordFormat().getFieldIndex(REFERENCE_NAME)] =
				referenceStatistic.getAttribute(REFERENCE_NAME).asText();
		tuple[refStoreFormat.getRecordFormat().getFieldIndex(REFERENCE_CLASS)] =
				referenceStatistic.getAttribute(REFERENCE_CLASS).asText();
		tuple[refStoreFormat.getRecordFormat()
				.getFieldIndex(EPOCH_ID)] = epochId;
		tm.add(REFERENCE_STORE, tuple);


		visitChildren(referenceStatistic);

		//Reset
		referenceId = null;

		return null;
	}

	@Override
	public Void visit(final IndexStatistic indexStatistic) {
	indexId = indexStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_INDEX_ID).asLong();
		tm.add(
				INDEX_STORE,
				indexId,
				epochId,
				null, //FIXME
				indexStatistic.getAttributes().get(INDEX_CLASS).asText());

		final String[] fieldNames = indexStatistic.getAttribute(FIELDS)
				.asStringArray();

		assert fieldNames != null && fieldNames.length > 0 : "Cannot find fields in the attributes of "
				+ indexStatistic;

		currentChunkRecord[chunkRecordFormat.getFieldIndex(FIELDS)] = new StringArrayObject(fieldNames);

		visitChildren(indexStatistic);

		// Reset
		currentChunkRecord[chunkRecordFormat.getFieldIndex(FIELDS)] = null;
		indexId = null;

		return null;
	}

	@Override
	public Void visit(final DictionaryStatistic dictionaryStatistic) {
		if (dictionaryStatistic.getName() != MemoryStatisticConstants.STAT_NAME_DICTIONARY) {
			// The stat was constructed as a dictionary stat, but in fact belongs to another structure, like the keys of an index
			visitChildren(dictionaryStatistic);
			return null;
		}
		final IStatisticAttribute dicIdAttr = dictionaryStatistic.getAttribute(ATTR_NAME_DICTIONARY_ID);
		if (dicIdAttr == null) {
			System.err.println("No dictionary ID for " + dictionaryStatistic);
		}
		dictionaryId = dicIdAttr.asLong();
		// Init. chunk record
		currentChunkRecord = new Object[chunkRecordFormat.getFieldCount()];

		currentChunkRecord[chunkRecordFormat.getFieldIndex(EPOCH_ID)] = epochId;
		final IStatisticAttribute fieldNamesAttr = dictionaryStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_FIELDS);
		if (fieldNamesAttr!=null) {
			currentChunkRecord[chunkRecordFormat.getFieldIndex(FIELDS)] = new StringArrayObject(
					fieldNamesAttr.asStringArray());
		}

		final IStatisticAttribute dicClassAttr = dictionaryStatistic.getAttribute(DICTIONARY_CLASS);
		final String dictionaryClass;
		if (dicClassAttr!=null){
			dictionaryClass=dicClassAttr.asText();
		} else{
			System.err.println("Dictionary does not state its class "+dictionaryStatistic);
			dictionaryClass = dictionaryStatistic.getAttribute(ATTR_NAME_CREATOR_CLASS).asText();
		}
		tm.add(
				DICTIONARY_STORE,
				dictionaryId,
				epochId,
				dictionaryStatistic.getAttribute(DICTIONARY_SIZE).asInt(),
				dictionaryStatistic.getAttribute(DICTIONARY_ORDER).asInt(),
				dictionaryClass);

		visitChildren(dictionaryStatistic);

		// Reset
		currentChunkRecord = null;
		dictionaryId = null;
		return null;
	}

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

}
