/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.activeviam.activepivot.core.datastore.api.builder.StartBuilding;
import com.activeviam.activepivot.server.spring.api.config.IDatastoreSchemaDescriptionConfig;
import com.activeviam.database.api.schema.StoreField;
import com.activeviam.database.api.types.ILiteralType;
import com.activeviam.database.datastore.api.description.IDatastoreSchemaDescription;
import com.activeviam.database.datastore.api.description.IReferenceDescription;
import com.activeviam.database.datastore.api.description.IStoreDescription;
import com.activeviam.database.datastore.api.description.impl.DatastoreSchemaDescription;
import com.activeviam.database.datastore.api.description.impl.DuplicateKeyHandlers;
import com.activeviam.database.datastore.api.description.impl.StoreDescription;
import com.activeviam.tech.chunks.internal.IChunk;
import com.activeviam.tech.chunks.internal.pool.impl.AtotiPools;
import com.activeviam.tech.core.api.format.IParser;
import com.activeviam.tech.core.internal.util.ArrayUtil;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.context.annotation.Configuration;

/**
 * Contains all stores used to analyze a memory analysis dump.
 *
 * @author ActiveViam
 */
@Configuration
public class MemoryAnalysisDatastoreDescriptionConfig implements IDatastoreSchemaDescriptionConfig {

  /** Constant enabling the creation of Debug tree for each info, stored in the Datastore. */
  public static final boolean ADD_DEBUG_TREE = false;

  /** Name of the chunk <-> provider linking store. */
  public static final String CHUNK_TO_PROVIDER = "chunkToProvider";

  /** Name of the chunk -> application linking store. */
  public static final String CHUNK_TO_APP = "chunkToApp";

  /** Name of the chunk -> branch reference. */
  public static final String CHUNK_TO_VERSION = "epochViewToVersion";

  /** Default value for component-specific ids. */
  public static final Long DEFAULT_COMPONENT_ID_VALUE = -1L;

  /** Partition value for chunks held by no partitions. */
  public static final int NO_PARTITION = -3;

  /** Partition value for chunks held by multiple partitions. */
  public static final int MANY_PARTITIONS = -2;

  /**
   * Returns the value with which to do modulo partitioning on the chunk store.
   *
   * @return the modulo partitioning value
   */
  private static int partitioningModulo() {
    return AtotiPools.getMixedWorkloadThreadCount();
  }

  @Override
  public IDatastoreSchemaDescription datastoreSchemaDescription() {
    return new DatastoreSchemaDescription(
        getStoreDescriptions(), getReferenceDescriptions(), getDictionaryGroups());
  }

  /**
   * Description of the chunk store.
   *
   * @return description of {@link DatastoreConstants#CHUNK_STORE} (main store)
   */
  protected IStoreDescription chunkStore() {
    return StoreDescription.builder()
        .withStoreName(DatastoreConstants.CHUNK_STORE)
        // key
        .withField(DatastoreConstants.CHUNK_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK__DUMP_NAME, ILiteralType.STRING)
        .asKeyField()
        .withField(DatastoreConstants.VERSION__EPOCH_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.OWNER__OWNER, ILiteralType.OBJECT)
        .asKeyField()
        .withField(DatastoreConstants.OWNER__FIELD, ILiteralType.STRING)
        .asKeyField()
        .withField(DatastoreConstants.OWNER__COMPONENT, ILiteralType.OBJECT)
        .asKeyField()

        /* Foreign keys */
        .withField(DatastoreConstants.CHUNK__PARTITION_ID, ILiteralType.INT, NO_PARTITION)
        .dictionarized()
        .withField(DatastoreConstants.CHUNK__PARENT_ID)
        .withField(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, ILiteralType.OBJECT)
        // Add 5 fields corresponding to the closest parent Id for a given type of parent
        .withField(
            DatastoreConstants.CHUNK__PARENT_DICO_ID, ILiteralType.LONG, DEFAULT_COMPONENT_ID_VALUE)
        .dictionarized()
        .withField(
            DatastoreConstants.CHUNK__PARENT_INDEX_ID,
            ILiteralType.LONG,
            DEFAULT_COMPONENT_ID_VALUE)
        .dictionarized()
        .withField(
            DatastoreConstants.CHUNK__PARENT_REF_ID, ILiteralType.LONG, DEFAULT_COMPONENT_ID_VALUE)
        .dictionarized()
        .withField(
            DatastoreConstants.CHUNK__PROVIDER_ID,
            ILiteralType.LONG,
            DatastoreConstants.LONG_IF_NOT_EXIST)
        .withField(DatastoreConstants.CHUNK__CLASS)
        .withField(DatastoreConstants.CHUNK__OFF_HEAP_SIZE, ILiteralType.LONG)
        .withField(DatastoreConstants.CHUNK__ON_HEAP_SIZE, ILiteralType.LONG)
        .withField(DatastoreConstants.CHUNK__SIZE, ILiteralType.LONG)
        .withField(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS, ILiteralType.LONG)
        .withField(DatastoreConstants.CHUNK__FREE_ROWS, ILiteralType.LONG)
        .withField(
            DatastoreConstants.CHUNK__USED_BY_VERSION, ILiteralType.OBJECT, UsedByVersion.UNKNOWN)
        .dictionarized()
        .withNullableField(DatastoreConstants.CHUNK__VECTOR_BLOCK_LENGTH, ILiteralType.LONG)
        .withNullableField(DatastoreConstants.CHUNK__VECTOR_BLOCK_REF_COUNT, ILiteralType.LONG)
        .withNullableField(DatastoreConstants.CHUNK__DEBUG_TREE, ILiteralType.STRING)
        .withModuloPartitioning(
            partitioningModulo(),
            DatastoreConstants.CHUNK_ID,
            DatastoreConstants.CHUNK__DUMP_NAME,
            DatastoreConstants.VERSION__EPOCH_ID)
        .withDuplicateKeyHandler(new ChunkRecordHandler())
        .build();
  }

  /**
   * Description of the epoch view store.
   *
   * @return description of {@link DatastoreConstants#EPOCH_VIEW_STORE}
   */
  protected IStoreDescription epochViewStore() {
    return StartBuilding.store()
        .withStoreName(DatastoreConstants.EPOCH_VIEW_STORE)
        .withField(DatastoreConstants.EPOCH_VIEW__OWNER, ILiteralType.OBJECT)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK__DUMP_NAME)
        .asKeyField()
        .withField(DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.EPOCH_VIEW__VIEW_EPOCH_ID, ILiteralType.OBJECT)
        .asKeyField()
        .build();
  }

  /**
   * Description of the reference store.
   *
   * @return description of {@link DatastoreConstants#REFERENCE_STORE}
   */
  protected IStoreDescription referenceStore() {
    return StoreDescription.builder()
        .withStoreName(DatastoreConstants.REFERENCE_STORE)
        .withField(DatastoreConstants.REFERENCE_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
        .asKeyField()
        .withField(DatastoreConstants.VERSION__EPOCH_ID, ILiteralType.LONG)
        .asKeyField()

        /* Foreign keys */
        .withField(DatastoreConstants.REFERENCE_FROM_STORE)
        .withField(
            DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID,
            ILiteralType.INT,
            DatastoreConstants.INT_IF_NOT_EXIST)
        .withField(DatastoreConstants.REFERENCE_TO_STORE)
        .withField(
            DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID,
            ILiteralType.INT,
            DatastoreConstants.INT_IF_NOT_EXIST)
        /* Attributes */
        .withField(DatastoreConstants.REFERENCE_NAME)
        .withField(DatastoreConstants.REFERENCE_CLASS)
        .build();
  }

  /**
   * Description of the index store.
   *
   * @return description of {@link DatastoreConstants#INDEX_STORE}
   */
  protected IStoreDescription indexStore() {
    return StoreDescription.builder()
        .withStoreName(DatastoreConstants.INDEX_STORE)
        .withField(DatastoreConstants.INDEX_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
        .asKeyField()
        .withField(DatastoreConstants.VERSION__EPOCH_ID, ILiteralType.LONG)
        .asKeyField()

        /* Attributes */
        .withField(
            DatastoreConstants.INDEX_TYPE, ILiteralType.OBJECT) // FIXME(ope) primary, secondary,
        // key
        .withField(DatastoreConstants.INDEX_CLASS)
        .withField(DatastoreConstants.INDEX__FIELDS, ILiteralType.OBJECT)
        .build();
  }

  /**
   * Description of the dictionary store.
   *
   * @return description of {@link DatastoreConstants#DICTIONARY_STORE}
   */
  protected IStoreDescription dictionaryStore() {
    return StoreDescription.builder()
        .withStoreName(DatastoreConstants.DICTIONARY_STORE)
        .withField(DatastoreConstants.DICTIONARY_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
        .asKeyField()
        .withField(DatastoreConstants.VERSION__EPOCH_ID, ILiteralType.LONG)
        .asKeyField()

        /* Attributes */
        .withField(DatastoreConstants.DICTIONARY_SIZE, ILiteralType.LONG)
        .withField(DatastoreConstants.DICTIONARY_ORDER, ILiteralType.INT)
        .withField(DatastoreConstants.DICTIONARY_CLASS)
        .build();
  }

  /**
   * Returns the description of {@link DatastoreConstants#LEVEL_STORE}.
   *
   * @return description of {@link DatastoreConstants#LEVEL_STORE}
   */
  protected IStoreDescription levelStore() {
    return StartBuilding.store()
        .withStoreName(DatastoreConstants.LEVEL_STORE)
        .withField(DatastoreConstants.LEVEL__MANAGER_ID)
        .asKeyField()
        .withField(DatastoreConstants.LEVEL__PIVOT_ID)
        .asKeyField()
        .withField(DatastoreConstants.LEVEL__DIMENSION)
        .asKeyField()
        .withField(DatastoreConstants.LEVEL__HIERARCHY)
        .asKeyField()
        .withField(DatastoreConstants.LEVEL__LEVEL)
        .asKeyField()
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
        .asKeyField()
        .withField(DatastoreConstants.VERSION__EPOCH_ID, ILiteralType.LONG)
        .asKeyField()

        /* Attributes */
        .withField(DatastoreConstants.LEVEL__ON_HEAP_SIZE, ILiteralType.LONG)
        // TODO(ope) will be empty, but how to consider this in the cube
        .withField(DatastoreConstants.LEVEL__OFF_HEAP_SIZE, ILiteralType.LONG)
        .withField(DatastoreConstants.LEVEL__MEMBER_COUNT, ILiteralType.LONG)
        .build();
  }

  /**
   * Returns the description of {@link DatastoreConstants#CHUNK_TO_LEVEL_STORE}.
   *
   * @return description of {@link DatastoreConstants#CHUNK_TO_LEVEL_STORE}
   */
  protected IStoreDescription chunkTolevelStore() {
    return StartBuilding.store()
        .withStoreName(DatastoreConstants.CHUNK_TO_LEVEL_STORE)
        .withField(DatastoreConstants.CHUNK_TO_LEVEL__MANAGER_ID)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK_TO_LEVEL__PIVOT_ID)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK_TO_LEVEL__DIMENSION)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK_TO_LEVEL__HIERARCHY)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK_TO_LEVEL__LEVEL)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK_TO_LEVEL__PARENT_ID)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK_TO_LEVEL__PARENT_TYPE, ILiteralType.OBJECT)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK__DUMP_NAME, ILiteralType.STRING)
        .asKeyField()
        .build();
  }

  /**
   * Returns the description of {@link DatastoreConstants#VERSION_STORE}.
   *
   * @return description of {@link DatastoreConstants#VERSION_STORE}
   */
  protected IStoreDescription versionStore() {
    return StartBuilding.store()
        .withStoreName(DatastoreConstants.VERSION_STORE)
        .withField(DatastoreConstants.VERSION__DUMP_NAME, ILiteralType.STRING)
        .asKeyField()
        .withField(DatastoreConstants.VERSION__EPOCH_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.VERSION__BRANCH_NAME, ILiteralType.STRING)
        .build();
  }

  /**
   * Returns the description of {@link DatastoreConstants#PROVIDER_STORE}.
   *
   * @return description of {@link DatastoreConstants#PROVIDER_STORE}
   */
  protected IStoreDescription providerStore() {
    return StartBuilding.store()
        .withStoreName(DatastoreConstants.PROVIDER_STORE)
        .withField(DatastoreConstants.PROVIDER__PROVIDER_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
        .asKeyField()

        /* Foreign keys */
        .withField(DatastoreConstants.PROVIDER__PIVOT_ID)
        .withField(DatastoreConstants.PROVIDER__MANAGER_ID)
        /* Attributes */
        .withField(DatastoreConstants.PROVIDER__INDEX, ILiteralType.STRING, "<None>")
        .withField(DatastoreConstants.PROVIDER__TYPE)
        .withField(DatastoreConstants.PROVIDER__CATEGORY)
        .build();
  }

  /**
   * Returns the description of {@link DatastoreConstants#PIVOT_STORE}.
   *
   * @return description of {@link DatastoreConstants#PIVOT_STORE}
   */
  protected IStoreDescription pivotStore() {
    return StartBuilding.store()
        .withStoreName(DatastoreConstants.PIVOT_STORE)
        .withField(DatastoreConstants.PIVOT__PIVOT_ID)
        .asKeyField()
        .withField(DatastoreConstants.PIVOT__MANAGER_ID)
        .asKeyField()
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
        .asKeyField()
        .build();
  }

  /**
   * Returns the description of {@link DatastoreConstants#APPLICATION_STORE}.
   *
   * @return description of {@link DatastoreConstants#APPLICATION_STORE}
   */
  protected IStoreDescription applicationStore() {
    return StartBuilding.store()
        .withStoreName(DatastoreConstants.APPLICATION_STORE)
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
        .asKeyField()
        .withField(
            DatastoreConstants.APPLICATION__DATE,
            IParser.DATE + "[" + DatastoreConstants.DATE_PATTERN + "]")
        .withField(DatastoreConstants.APPLICATION__USED_ON_HEAP, ILiteralType.LONG)
        .withField(DatastoreConstants.APPLICATION__MAX_ON_HEAP, ILiteralType.LONG)
        .withField(DatastoreConstants.APPLICATION__USED_OFF_HEAP, ILiteralType.LONG)
        .withField(DatastoreConstants.APPLICATION__MAX_OFF_HEAP, ILiteralType.LONG)
        .withDuplicateKeyHandler(DuplicateKeyHandlers.ALWAYS_UPDATE)
        .build();
  }

  public List<? extends IStoreDescription> getStoreDescriptions() {
    return Arrays.asList(
        chunkStore(),
        referenceStore(),
        indexStore(),
        dictionaryStore(),
        levelStore(),
        providerStore(),
        pivotStore(),
        chunkTolevelStore(),
        epochViewStore(),
        versionStore(),
        applicationStore());
  }

  public Collection<? extends IReferenceDescription> getReferenceDescriptions() {
    return Stream.of(getChunkReferences(), getPivotAndProviderReferences())
        .flatMap(Function.identity())
        .collect(Collectors.toList());
  }

  public Collection<Set<StoreField>> getDictionaryGroups() {
    return List.of(
        Set.of(
            new StoreField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__DUMP_NAME),
            new StoreField(
                DatastoreConstants.EPOCH_VIEW_STORE, DatastoreConstants.CHUNK__DUMP_NAME),
            new StoreField(
                DatastoreConstants.REFERENCE_STORE, DatastoreConstants.APPLICATION__DUMP_NAME),
            new StoreField(
                DatastoreConstants.INDEX_STORE, DatastoreConstants.APPLICATION__DUMP_NAME),
            new StoreField(
                DatastoreConstants.DICTIONARY_STORE, DatastoreConstants.APPLICATION__DUMP_NAME),
            new StoreField(
                DatastoreConstants.LEVEL_STORE, DatastoreConstants.APPLICATION__DUMP_NAME),
            new StoreField(
                DatastoreConstants.CHUNK_TO_LEVEL_STORE, DatastoreConstants.CHUNK__DUMP_NAME),
            new StoreField(DatastoreConstants.VERSION_STORE, DatastoreConstants.VERSION__DUMP_NAME),
            new StoreField(
                DatastoreConstants.PROVIDER_STORE, DatastoreConstants.APPLICATION__DUMP_NAME),
            new StoreField(
                DatastoreConstants.PIVOT_STORE, DatastoreConstants.APPLICATION__DUMP_NAME),
            new StoreField(
                DatastoreConstants.APPLICATION_STORE, DatastoreConstants.APPLICATION__DUMP_NAME)),
        Set.of(
            new StoreField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.VERSION__EPOCH_ID),
            new StoreField(
                DatastoreConstants.EPOCH_VIEW_STORE, DatastoreConstants.EPOCH_VIEW__BASE_EPOCH_ID),
            new StoreField(
                DatastoreConstants.REFERENCE_STORE, DatastoreConstants.VERSION__EPOCH_ID),
            new StoreField(DatastoreConstants.INDEX_STORE, DatastoreConstants.VERSION__EPOCH_ID),
            new StoreField(
                DatastoreConstants.DICTIONARY_STORE, DatastoreConstants.VERSION__EPOCH_ID),
            new StoreField(DatastoreConstants.LEVEL_STORE, DatastoreConstants.VERSION__EPOCH_ID),
            new StoreField(DatastoreConstants.VERSION_STORE, DatastoreConstants.VERSION__EPOCH_ID)),
        Set.of(
            new StoreField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.OWNER__OWNER),
            new StoreField(
                DatastoreConstants.EPOCH_VIEW_STORE, DatastoreConstants.EPOCH_VIEW__OWNER)),
        Set.of(
            new StoreField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__PROVIDER_ID),
            new StoreField(
                DatastoreConstants.PROVIDER_STORE, DatastoreConstants.PROVIDER__PROVIDER_ID)),
        Set.of(
            new StoreField(
                DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__PARENT_DICO_ID),
            new StoreField(DatastoreConstants.DICTIONARY_STORE, DatastoreConstants.DICTIONARY_ID)),
        Set.of(
            new StoreField(
                DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__PARENT_INDEX_ID),
            new StoreField(DatastoreConstants.INDEX_STORE, DatastoreConstants.INDEX_ID)),
        Set.of(
            new StoreField(DatastoreConstants.CHUNK_STORE, DatastoreConstants.CHUNK__PARENT_REF_ID),
            new StoreField(DatastoreConstants.REFERENCE_STORE, DatastoreConstants.REFERENCE_ID)));
  }

  private Stream<IReferenceDescription> getChunkReferences() {
    return Stream.of(
        StartBuilding.reference()
            .fromStore(DatastoreConstants.CHUNK_STORE)
            .toStore(DatastoreConstants.PROVIDER_STORE)
            .withName(CHUNK_TO_PROVIDER)
            .withMapping(
                DatastoreConstants.CHUNK__PROVIDER_ID, DatastoreConstants.PROVIDER__PROVIDER_ID)
            .withMapping(
                DatastoreConstants.APPLICATION__DUMP_NAME,
                DatastoreConstants.APPLICATION__DUMP_NAME)
            .build(),
        StartBuilding.reference()
            .fromStore(DatastoreConstants.CHUNK_STORE)
            .toStore(DatastoreConstants.APPLICATION_STORE)
            .withName(CHUNK_TO_APP)
            .withMapping(
                DatastoreConstants.CHUNK__DUMP_NAME, DatastoreConstants.APPLICATION__DUMP_NAME)
            .build(),
        StartBuilding.reference()
            .fromStore(DatastoreConstants.CHUNK_STORE)
            .toStore(DatastoreConstants.VERSION_STORE)
            .withName(CHUNK_TO_VERSION)
            .withMapping(DatastoreConstants.CHUNK__DUMP_NAME, DatastoreConstants.VERSION__DUMP_NAME)
            .withMapping(DatastoreConstants.VERSION__EPOCH_ID, DatastoreConstants.VERSION__EPOCH_ID)
            .build());
  }

  private Stream<IReferenceDescription> getPivotAndProviderReferences() {
    return Stream.of(
        // Level refs
        StartBuilding.reference()
            .fromStore(DatastoreConstants.CHUNK_TO_LEVEL_STORE)
            .toStore(DatastoreConstants.LEVEL_STORE)
            .withName("LevelInfo")
            .withMapping(
                DatastoreConstants.CHUNK_TO_LEVEL__MANAGER_ID, DatastoreConstants.LEVEL__MANAGER_ID)
            .withMapping(
                DatastoreConstants.CHUNK_TO_LEVEL__PIVOT_ID, DatastoreConstants.LEVEL__PIVOT_ID)
            .withMapping(
                DatastoreConstants.CHUNK_TO_LEVEL__DIMENSION, DatastoreConstants.LEVEL__DIMENSION)
            .withMapping(
                DatastoreConstants.CHUNK_TO_LEVEL__HIERARCHY, DatastoreConstants.LEVEL__HIERARCHY)
            .withMapping(DatastoreConstants.CHUNK_TO_LEVEL__LEVEL, DatastoreConstants.LEVEL__LEVEL)
            .build(),
        // Provider partitions refs
        StartBuilding.reference()
            .fromStore(DatastoreConstants.PROVIDER_STORE)
            .toStore(DatastoreConstants.PIVOT_STORE)
            .withName("ProviderToPivot")
            .withMapping(DatastoreConstants.PROVIDER__PIVOT_ID, DatastoreConstants.PIVOT__PIVOT_ID)
            .withMapping(
                DatastoreConstants.PROVIDER__MANAGER_ID, DatastoreConstants.PIVOT__MANAGER_ID)
            .withMapping(
                DatastoreConstants.APPLICATION__DUMP_NAME,
                DatastoreConstants.APPLICATION__DUMP_NAME)
            .build());
  }

  /** Enum listing the types of parent structures that hold the {@link IChunk chunks}. */
  public enum ParentType {
    /** Records structure. */
    RECORDS,
    /** Vector block structure. */
    VECTOR_BLOCK,
    /** Dictionary structure. */
    DICTIONARY,
    /** Reference structure. */
    REFERENCE,
    /** Index structure. */
    INDEX,
    /** Point mapping structure. */
    POINT_MAPPING,
    /** Point index structure. */
    POINT_INDEX,
    /** Aggregate store structure. */
    AGGREGATE_STORE,
    /** Bitmap Matcher structure. */
    BITMAP_MATCHER,
    /** Level structure. */
    LEVEL,
    /** No owning structure. */
    NO_COMPONENT
  }

  /** Characterizes whether or not a chunk is used by the version it appears in. */
  public enum UsedByVersion {
    /** Not used by the version. */
    FALSE,
    /** Used by the version. */
    TRUE,
    /** Cannot tell if the chunk belongs to the version. */
    UNKNOWN
  }

  /**
   * Wrapper class around String[] (for equals, hashcode and toString).
   *
   * @author ActiveViam
   */
  public static class StringArrayObject {

    /** Underlying array. */
    protected final String[] fieldNames;

    /**
     * Default constructor.
     *
     * @param fieldNames the Underlying array to wrap
     */
    public StringArrayObject(String... fieldNames) {
      this.fieldNames = fieldNames;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(this.fieldNames);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      StringArrayObject other = (StringArrayObject) obj;
      return Arrays.equals(this.fieldNames, other.fieldNames);
    }

    @Override
    public String toString() {
      return ArrayUtil.join(", ", this.fieldNames).toString();
    }
  }
}
