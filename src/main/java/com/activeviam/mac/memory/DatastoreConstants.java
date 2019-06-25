/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.memory;

import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.PivotMemoryStatisticConstants;
import com.quartetfs.biz.pivot.impl.ActivePivotManager;

/**
 * List of constants used by stores storing the off-heap memory analysis results.
 * @author ActiveViam
 */
public class DatastoreConstants {

	// Default values
	/**
	 * Default long value
	 */
	public static final long LONG_IF_NOT_EXIST = -1L;
	/**
	 * Default int value
	 */
	public static final int INT_IF_NOT_EXIST = -1;

	// ALL STORES

	/*  */
	public static final String CHUNK_STORE = MemoryStatisticConstants.STAT_NAME_CHUNK;
	
	/*  */
	public static final String REFERENCE_STORE = MemoryStatisticConstants.STAT_NAME_REFERENCE;

	/*  */
	public static final String INDEX_STORE = MemoryStatisticConstants.STAT_NAME_INDEX;

	/*  */
	public static final String DICTIONARY_STORE = MemoryStatisticConstants.STAT_NAME_DICTIONARY;

	/*  */
	public static final String LEVEL_STORE = "Levels";

	/*  */
	public static final String PROVIDER_COMPONENT_STORE = "ProviderComponent";

	/*  */
	public static final String PROVIDER_STORE = "Provider";

	/*  */
	public static final String PIVOT_STORE = "ActivePivot";

	/*  */
	public static final String APPLICATION_STORE = "Application";

	// Field names


	/*  */
	public static final String CHUNK__DEBUG_TREE = "chunkDebugTree";

	/*  */
	public static final String DATE_PATTERN = MemoryStatisticConstants.DATE_PATTERN;

	// Ids of different stores used as key.

	/*  */
	public static final String CHUNK_ID = MemoryStatisticConstants.ATTR_NAME_CHUNK_ID;

	/*  */
	public static final String REFERENCE_ID = "referenceId";

	/*  */
	public static final String INDEX_ID = "indexId";

	/*  */
	public static final String FIELDS = "fields";

	/*  */
	public static final String DICTIONARY_ID = "dicId";


	/*  */
	public static final String CHUNK__OWNER = "owner";

	/*  */
	public static final String CHUNK__COMPONENT = "component";

	/*  */
	public static final String CHUNK__PARENT_ID = "parentId";

	/*  */
	public static final String CHUNK__PARENT_TYPE = "parentType";

	/*  */
	public static final String CHUNK__SIZE = "size";

	/*  */
	public static final String CHUNK__NON_WRITTEN_ROWS = "nonWrittenRows";

	/*  */
	public static final String CHUNK__FREE_ROWS = "freeRows";


	/*  */
	public static final String PROVIDER__PROVIDER_ID = PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_ID;

	/*  */
	public static final String PROVIDER__PIVOT_ID = "pivotId_prov";

	/*  */
	public static final String PROVIDER__MANAGER_ID = "managerId_prov";

	/*  */
	public static final String PROVIDER__INDEX = "index_prov";

	/*  */
	public static final String PROVIDER__TYPE = "type";

	/*  */
	public static final String PROVIDER__CATEGORY = "category";


	/*  */
	public static final String FIELD_SUFFIX = "Fld";

	/*  */
	public static final String INDEX_SUFFIX = "Idx";

	/*  */
	public static final String DICO_SUFFIX = "Dic";

	/*  */
	public static final String REFERENCE_SUFFIX = "Ref";

	/*  */
	public static final String LEVEL_SUFFIX = "Lvl";

	/*  */
	public static final String CHUNK_TO_FIELD_STORE = "ChunkToField";

	/*  */
	public static final String CHUNK_TO_FIELD__PARENT_ID = "parentId_"+ FIELD_SUFFIX;

	/*  */
	public static final String CHUNK_TO_FIELD__PARENT_TYPE = "parentType_"+ FIELD_SUFFIX;

	/*  */
	public static final String CHUNK_TO_FIELD__FIELD = "fieldName";

	/*  */
	public static final String CHUNK_TO_FIELD__STORE = "storeName";


	/*  */
	public static final String CHUNK_TO_INDEX_STORE = "ChunkToIndex";

	/*  */
	public static final String CHUNK_TO_INDEX__PARENT_ID = "parentId_"+ INDEX_SUFFIX;

	/*  */
	public static final String CHUNK_TO_INDEX__PARENT_TYPE = "parentType_"+ INDEX_SUFFIX;

	/*  */
	public static final String CHUNK_TO_INDEX__INDEX_ID = "indexId";


	/*  */
	public static final String CHUNK_TO_DICO_STORE = "ChunkToDico";

	/*  */
	public static final String CHUNK_TO_DICO__PARENT_ID = "parentId_"+ DICO_SUFFIX;

	/*  */
	public static final String CHUNK_TO_DICO__PARENT_TYPE = "parentType_"+DICO_SUFFIX;

	/*  */
	public static final String CHUNK_TO_DICO__DICO_ID = "dictionaryId";


	/*  */
	public static final String CHUNK_TO_REF_STORE = "ChunkToReference";

	/*  */
	public static final String CHUNK_TO_REF__PARENT_ID = "parentId_" + REFERENCE_SUFFIX;

	/*  */
	public static final String CHUNK_TO_REF__PARENT_TYPE = "parentType_"+ REFERENCE_SUFFIX;

	/*  */
	public static final String CHUNK_TO_REF__REF_ID = "refId";

	// store and partition info.

	/*  */
	public static final String CHUNK__PARTITION_ID = MemoryStatisticConstants.ATTR_NAME_PARTITION_ID;

	/*  */
	public static final String CHUNK__PROVIDER_ID = PROVIDER__PROVIDER_ID;

	/*  */
	public static final String CHUNK__PROVIDER_COMPONENT_TYPE = "providerCpnType";

	// ## CHUNK_STORE ## Field names of the chunk store

	/*  */
	public static final String CHUNK__CLASS = "class";

	/*  */
	public static final String CHUNK__OFF_HEAP_SIZE = "offHeapMemorySize";

	/*  */
	public static final String CHUNK__ON_HEAP_SIZE = "onHeapMemorySize";

	/*  */
	public static final String CHUNK__DUMP_NAME = "dumpName"; // The name of the off-heap dump

	// ## REFERENCES_STORE ## Field names of the Reference store

	/*  */
	public static final String REFERENCE_NAME = MemoryStatisticConstants.ATTR_NAME_REFERENCE_NAME;

	/*  */
	public static final String REFERENCE_FROM_STORE = MemoryStatisticConstants.ATTR_NAME_FROM_STORE;

	/*  */
	public static final String REFERENCE_FROM_STORE_PARTITION_ID = MemoryStatisticConstants.ATTR_NAME_FROM_STORE_PARTITION_ID;

	/*  */
	public static final String REFERENCE_TO_STORE = MemoryStatisticConstants.ATTR_NAME_TO_STORE;

	/*  */
	public static final String REFERENCE_TO_STORE_PARTITION_ID = MemoryStatisticConstants.ATTR_NAME_TO_STORE_PARTITION_ID;

	/*  */
	public static final String REFERENCE_CLASS = MemoryStatisticConstants.ATTR_NAME_CLASS;

	// ## INDEX_STORE ## Field names of the Index store

	/*  */
	public static final String INDEX_TYPE = "type";

	/*  */
	public static final String INDEX_CLASS = MemoryStatisticConstants.ATTR_NAME_CLASS;

	/*  */
	public static final String INDEX__FIELDS = "fields";

	// ## DICTIONARY_STORE ## Field names of the Dictionary store

	/*  */
	public static final String DICTIONARY_SIZE = MemoryStatisticConstants.ATTR_NAME_LENGTH;

	/*  */
	public static final String DICTIONARY_ORDER = "order";

	/*  */
	public static final String DICTIONARY_CLASS = MemoryStatisticConstants.ATTR_NAME_CLASS;


	/*  */
	public static final String LEVEL__MANAGER_ID = "managerId";

	/*  */
	public static final String LEVEL__PIVOT_ID = "pivotId";

	/*  */
	public static final String LEVEL__DIMENSION = "dimension";

	/*  */
	public static final String LEVEL__HIERARCHY = "hierarchy";

	/*  */
	public static final String LEVEL__LEVEL = "level";

	/*  */
	public static final String LEVEL__ON_HEAP_SIZE = "onHeap";

	/*  */
	public static final String LEVEL__OFF_HEAP_SIZE = "offHeap";

	/*  */
	public static final String LEVEL__MEMBER_COUNT = "memberCount";


	/*  */
	public static final String CHUNK_TO_LEVEL_STORE = "ChunkToLevel";

	/*  */
	public static final String CHUNK_TO_LEVEL__MANAGER_ID = "managerId";

	/*  */
	public static final String CHUNK_TO_LEVEL__PIVOT_ID = "pivotId" ;

	/*  */
	public static final String CHUNK_TO_LEVEL__DIMENSION = "dimension";

	/*  */
	public static final String CHUNK_TO_LEVEL__HIERARCHY = "hierarchy";

	/*  */
	public static final String CHUNK_TO_LEVEL__LEVEL = "level";

	/*  */
	public static final String CHUNK_TO_LEVEL__PARENT_ID = "parentId"+LEVEL_SUFFIX;

	/** Parent type in the chunk to level store*/
	public static final String CHUNK_TO_LEVEL__PARENT_TYPE = "parentType"+ LEVEL_SUFFIX;


	/** Id of the aggragate provider in the provider component store */
	public static final String PROVIDER_COMPONENT__PROVIDER_ID = PROVIDER__PROVIDER_ID;

	/** Class of the aggregate provider compoent */
	public static final String PROVIDER_COMPONENT__CLASS = "class";

	/** Type of aggregate provider component  */
	public static final String PROVIDER_COMPONENT__TYPE = "providerComponentType";


	/** Id of the Pivot */
	public static final String PIVOT__PIVOT_ID = "pivotId";

	/** Id of the {@link ActivePivotManager} */
	public static final String PIVOT__MANAGER_ID = "managerId";


	/** Date */
	public static final String APPLICATION__DATE = "date";

	/** Name of the import */
	public static final String APPLICATION__DUMP_NAME = "dumpName";

	/** Used on heap memory */
	public static final String APPLICATION__USED_ON_HEAP = "usedOnHeap";

	/** Maximum Applicaiton on heap memory */
	public static final String APPLICATION__MAX_ON_HEAP = "maxOnHeap";

	/** used direct momory */
	public static final String APPLICATION__USED_OFF_HEAP = "usedOffHeap";

	/** Maximum Application direct memory */
	public static final String APPLICATION__MAX_OFF_HEAP = "maxOffHeap";

	private DatastoreConstants() {}

}
