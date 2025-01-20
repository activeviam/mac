/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.activeviam.activepivot.core.impl.internal.impl.ActivePivotManager;
import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;

/**
 * List of constants used by stores storing the off-heap memory analysis results.
 *
 * @author ActiveViam
 */
public class DatastoreConstants {

  // Default values
  /** Default long value. */
  public static final long LONG_IF_NOT_EXIST = -1L;

  /** Default int value. */
  public static final int INT_IF_NOT_EXIST = -1;

  // ALL STORES

  /** Name of the Store containing chunk-related facts. */
  public static final String CHUNK_STORE = MemoryStatisticConstants.STAT_NAME_CHUNK;

  /** Name of the store containing reference related facts. */
  public static final String REFERENCE_STORE = MemoryStatisticConstants.STAT_NAME_REFERENCE;

  /** Name of the store containing index-related facts. */
  public static final String INDEX_STORE = MemoryStatisticConstants.STAT_NAME_INDEX;

  /** Name of the store containing dictionary related facts. */
  public static final String DICTIONARY_STORE = MemoryStatisticConstants.STAT_NAME_DICTIONARY;

  /** Name of the store containing level related facts. */
  public static final String LEVEL_STORE = "Levels";

  /** Name of the store containing facts related to the aggregate provider. */
  public static final String PROVIDER_STORE = "Provider";

  /** Name of the store containing pivot -related facts. */
  public static final String PIVOT_STORE = "ActivePivot";

  /** Name of the store related to the application. */
  public static final String APPLICATION_STORE = "Application";

  /** Name of the store related to the application. */
  public static final String VERSION_STORE = "Version";

  // Field names

  /** Field containing debug data for the memory statistics. */
  public static final String CHUNK__DEBUG_TREE = "chunkDebugTree";

  /** Formatting pattern of the date field. */
  public static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSZ";

  // Ids of different stores used as key.

  /** Chunk ID field. */
  public static final String CHUNK_ID = MemoryStatisticConstants.ATTR_NAME_CHUNK_ID;

  /** Reference id field. */
  public static final String REFERENCE_ID = "referenceId";

  /** Index id field. */
  public static final String INDEX_ID = "indexId";

  /** Dictionary id field. */
  public static final String DICTIONARY_ID = "dicId";

  /** Field for the Id of the direct parent of the chunk. */
  public static final String CHUNK__PARENT_ID = "parentId";

  /** Field for the structure type of the direct parent of the chunk. */
  public static final String CHUNK__CLOSEST_PARENT_TYPE = "parentType";

  /** Field for the id of the closest parent dictionary of the chunk. */
  public static final String CHUNK__PARENT_DICO_ID = "parentDicoId";

  /** Field for the id of the closest parent index of the chunk. */
  public static final String CHUNK__PARENT_INDEX_ID = "parentIndexId";

  /** Field for the id of the closest parent reference of the chunk. */
  public static final String CHUNK__PARENT_REF_ID = "parentRefId";

  /** Field for the size of the chunk. */
  public static final String CHUNK__SIZE = "size";

  /** Field for the amount of non written rows in the chunk. */
  public static final String CHUNK__NON_WRITTEN_ROWS = "nonWrittenRows";

  /** Field for the amount of freed rows in a chunk. */
  public static final String CHUNK__FREE_ROWS = "freeRows";

  /** Field for the id of the aggregate provider. */
  public static final String PROVIDER__PROVIDER_ID = MemoryStatisticConstants.ATTR_NAME_PROVIDER_ID;

  /** Field for the id of the pivot. */
  public static final String PROVIDER__PIVOT_ID = "pivotId_prov";

  /** Field for the id of the {@link ActivePivotManager}. */
  public static final String PROVIDER__MANAGER_ID = "managerId_prov";

  /** Field for the index of the aggregate provider. */
  public static final String PROVIDER__INDEX = "index_prov";

  /** Field for the type of the aggregate provider. */
  public static final String PROVIDER__TYPE = "type";

  /** Field for the category of aggregate provider. */
  public static final String PROVIDER__CATEGORY = "category";

  /** Partition id field. */
  public static final String CHUNK__PARTITION_ID = MemoryStatisticConstants.ATTR_NAME_PARTITION_ID;

  // store and partition info.
  /** Aggregate provider id field. */
  public static final String CHUNK__PROVIDER_ID = PROVIDER__PROVIDER_ID;

  /** Chunk class field. */
  public static final String CHUNK__CLASS = "class";

  // ## CHUNK_STORE ## Field names of the chunk store
  /** Used by version field. */
  public static final String CHUNK__USED_BY_VERSION = "usedByVersion";

  /** Direct memory footprint size field. */
  public static final String CHUNK__OFF_HEAP_SIZE = "offHeapMemorySize";

  /** On heap memory footprint size field. */
  public static final String CHUNK__ON_HEAP_SIZE = "onHeapMemorySize";

  /** Field for the vector block length of the chunk, if relevant. */
  public static final String CHUNK__VECTOR_BLOCK_LENGTH = "vectorBlockLength";

  /** Field for the vector block reference count of the chunk, if relevant. */
  public static final String CHUNK__VECTOR_BLOCK_REF_COUNT = "vectorBlockRefCount";

  /** Import name field. */
  public static final String CHUNK__DUMP_NAME = "dumpName"; // The name of the off-heap dump

  /** The epoch corresponding to the chunk. */
  public static final String VERSION__EPOCH_ID = "epochId";

  /** Reference name field. */
  public static final String REFERENCE_NAME = MemoryStatisticConstants.ATTR_NAME_REFERENCE_NAME;

  // ## REFERENCES_STORE ## Field names of the Reference store
  /** Field for the name of the base field of the reference. */
  public static final String REFERENCE_FROM_STORE = MemoryStatisticConstants.ATTR_NAME_FROM_STORE;

  /** Field for the partition id in the base of the reference. */
  public static final String REFERENCE_FROM_STORE_PARTITION_ID =
      MemoryStatisticConstants.ATTR_NAME_FROM_STORE_PARTITION_ID;

  /** Field for the name of the target fields of the reference. */
  public static final String REFERENCE_TO_STORE = MemoryStatisticConstants.ATTR_NAME_TO_STORE;

  /** Field for the partition id of the target store. */
  public static final String REFERENCE_TO_STORE_PARTITION_ID =
      MemoryStatisticConstants.ATTR_NAME_TO_STORE_PARTITION_ID;

  /** Reference class field. */
  public static final String REFERENCE_CLASS = MemoryStatisticConstants.ATTR_NAME_CLASS;

  /** Index type field. */
  public static final String INDEX_TYPE = "type";

  // ## INDEX_STORE ## Field names of the Index store
  /** Index class field. */
  public static final String INDEX_CLASS = MemoryStatisticConstants.ATTR_NAME_CLASS;

  /** Field for the name of the indexed fields (by an index). */
  public static final String INDEX__FIELDS = "fields";

  /** Field for the size of a dictionary. */
  public static final String DICTIONARY_SIZE = MemoryStatisticConstants.ATTR_NAME_LENGTH;

  // ## DICTIONARY_STORE ## Field names of the Dictionary store
  /** Dictionary size field. */
  public static final String DICTIONARY_ORDER = "order";

  /** Dictionary class field. */
  public static final String DICTIONARY_CLASS = MemoryStatisticConstants.ATTR_NAME_CLASS;

  /** Field for the {@link ActivePivotManager} name. */
  public static final String LEVEL__MANAGER_ID = "managerId";

  /** Field for the Pivot name. */
  public static final String LEVEL__PIVOT_ID = "pivotId";

  /** Field for the dimension name. */
  public static final String LEVEL__DIMENSION = "dimension";

  /** Field for the hierarchy name. */
  public static final String LEVEL__HIERARCHY = "hierarchy";

  /** Field for the level name. */
  public static final String LEVEL__LEVEL = "level";

  /** Field for the on-heap footprint of levels. */
  public static final String LEVEL__ON_HEAP_SIZE = "onHeap";

  /** Field for the direct memory footprint of levels. */
  public static final String LEVEL__OFF_HEAP_SIZE = "offHeap";

  /** member count field. */
  public static final String LEVEL__MEMBER_COUNT = "memberCount";

  /** Name of the store for joining chunk and level data. */
  public static final String CHUNK_TO_LEVEL_STORE = "ChunkToLevel";

  /** {@link ActivePivotManager} id field. */
  public static final String CHUNK_TO_LEVEL__MANAGER_ID = "managerId";

  /** Pivot id field. */
  public static final String CHUNK_TO_LEVEL__PIVOT_ID = "pivotId";

  /** Dimension field. */
  public static final String CHUNK_TO_LEVEL__DIMENSION = "dimension";

  /** Hierarchy field. */
  public static final String CHUNK_TO_LEVEL__HIERARCHY = "hierarchy";

  /** level field. */
  public static final String CHUNK_TO_LEVEL__LEVEL = "level";

  /** The chunk ID field in the store of chunk owners. */
  public static final String OWNER__CHUNK_ID = CHUNK_ID;

  /** The owner field in the store of chunk owners. */
  public static final String OWNER__OWNER = "owner";

  /** The component field in the store of chunk owners. */
  public static final String OWNER__COMPONENT = "component";

  /** The field name field in the store of chunk owners. */
  public static final String OWNER__FIELD = "field";

  /** Class of the aggregate provider compoent. */
  public static final String PROVIDER_COMPONENT__CLASS = "class";

  /** Id of the Pivot. */
  public static final String PIVOT__PIVOT_ID = "pivotId";

  /** Id of the {@link ActivePivotManager}. */
  public static final String PIVOT__MANAGER_ID = "managerId";

  /** Date. */
  public static final String APPLICATION__DATE = "date";

  /** Name of the import. */
  public static final String APPLICATION__DUMP_NAME = "dumpName";

  /** Used on heap memory. */
  public static final String APPLICATION__USED_ON_HEAP = "usedOnHeap";

  /** Maximum Application on heap memory. */
  public static final String APPLICATION__MAX_ON_HEAP = "maxOnHeap";

  /** Used direct memory. */
  public static final String APPLICATION__USED_OFF_HEAP = "usedOffHeap";

  /** Maximum Application direct memory. */
  public static final String APPLICATION__MAX_OFF_HEAP = "maxOffHeap";

  /** The dump name field in the branch store. */
  public static final String VERSION__DUMP_NAME = "dumpName";

  /** The branch name field in the branch store. */
  public static final String VERSION__BRANCH_NAME = "branch";

  /** The name of the epoch view store. */
  public static final String EPOCH_VIEW_STORE = "EpochView";

  /** The base epoch id field in the epoch view store. */
  public static final String EPOCH_VIEW__BASE_EPOCH_ID = "baseEpochId";

  /** The owner field in the epoch view store. */
  public static final String EPOCH_VIEW__OWNER = "owner";

  /** The view epoch id field in the epoch view store. */
  public static final String EPOCH_VIEW__VIEW_EPOCH_ID = "viewEpochId";

  private static final String LEVEL_SUFFIX = "Lvl";

  /** parent id field. */
  public static final String CHUNK_TO_LEVEL__PARENT_ID = "parentId" + LEVEL_SUFFIX;

  /** Parent type in the chunk to level store. */
  public static final String CHUNK_TO_LEVEL__PARENT_TYPE = "parentType" + LEVEL_SUFFIX;

  private DatastoreConstants() {}
}
