/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.memory;

import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;

/**
 * List of constants used by stores storing the off-heap memory analysis results.
 * @author ActiveViam
 */
public class DatastoreConstants {

	// Default values
	public static final long LONG_IF_NOT_EXIST = -1L;

	// ALL STORES
	public static final String CHUNK_STORE = MemoryStatisticConstants.STAT_NAME_CHUNK;
	public static final String CHUNK_SET_STORE = MemoryStatisticConstants.STAT_NAME_CHUNKSET;
	public static final String REFERENCE_STORE = MemoryStatisticConstants.STAT_NAME_REFERENCE;
	public static final String INDEX_STORE = MemoryStatisticConstants.STAT_NAME_INDEX;
	public static final String DICTIONARY_STORE = MemoryStatisticConstants.STAT_NAME_DICTIONARY;

	// Field names

	public static final String CHUNK__EXPORT_DATE = MemoryStatisticConstants.ATTR_NAME_DATE;
	public static final String DATE_PATTERN = MemoryStatisticConstants.DATE_PATTERN;

	// Ids of different stores used as key.
	public static final String EPOCH_ID = MemoryStatisticConstants.ATTR_NAME_EPOCH;
	public static final String CHUNK_ID = MemoryStatisticConstants.ATTR_NAME_CHUNK_ID;
	public static final String CHUNKSET_ID = MemoryStatisticConstants.ATTR_NAME_CHUNKSET_ID;
	public static final String REFERENCE_ID = "referenceId";
	public static final String INDEX_ID = "indexId";
	public static final String FIELDS = "fields";
	public static final String DICTIONARY_ID = "dicId";
	public static final String CHUNK__TYPE = "chunkType"; // is it an index, ref, record, dic...

	// store and partition info.
	public static final String CHUNK__STORE_NAME = MemoryStatisticConstants.ATTR_NAME_STORE_NAME;
	public static final String CHUNK__PARTITION_ID = MemoryStatisticConstants.ATTR_NAME_PARTITION_ID;

	// ## CHUNK_STORE ## Field names of the chunk store
	public static final String CHUNK__CLASS = "class";
	public static final String CHUNK__OFF_HEAP_SIZE = "offHeapMemorySize";
	public static final String CHUNK__ON_HEAP_SIZE = "onHeapMemorySize";
	public static final String CHUNK__DUMP_NAME = "dumpName"; // The name of the off-heap dump

	// ## CHUNK_SET_STORE ## Field names of the ChunkSet store
	public static final String CHUNK_SET_CLASS = MemoryStatisticConstants.ATTR_NAME_CLASS;
	public static final String CHUNK_SET_FREE_ROWS = MemoryStatisticConstants.ATTR_NAME_FREED_ROWS;
	public static final String CHUNK_SET_PHYSICAL_CHUNK_SIZE = MemoryStatisticConstants.ATTR_NAME_LENGTH;

	// ## REFERENCES_STORE ## Field names of the Reference store
	public static final String REFERENCE_NAME = MemoryStatisticConstants.ATTR_NAME_REFERENCE_NAME;
	public static final String REFERENCE_FROM_STORE = MemoryStatisticConstants.ATTR_NAME_FROM_STORE;
	public static final String REFERENCE_FROM_STORE_PARTITION_ID = MemoryStatisticConstants.ATTR_NAME_FROM_STORE_PARTITION_ID;
	public static final String REFERENCE_TO_STORE = MemoryStatisticConstants.ATTR_NAME_TO_STORE;
	public static final String REFERENCE_TO_STORE_PARTITION_ID = MemoryStatisticConstants.ATTR_NAME_TO_STORE_PARTITION_ID;
	public static final String REFERENCE_CLASS = MemoryStatisticConstants.ATTR_NAME_CLASS;

	// ## INDEX_STORE ## Field names of the Index store
	public static final String INDEX_TYPE = "type";
	public static final String INDEX_CLASS = MemoryStatisticConstants.ATTR_NAME_CLASS;

	// ## DICTIONARY_STORE ## Field names of the Dictionary store
	public static final String DICTIONARY_SIZE = MemoryStatisticConstants.ATTR_NAME_LENGTH;
	public static final String DICTIONARY_ORDER = "order";
	public static final String DICTIONARY_CLASS = MemoryStatisticConstants.ATTR_NAME_CLASS;

	private DatastoreConstants() {}

}
