/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.memory;

import com.activeviam.builders.StartBuilding;
import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.NoOwner;
import com.activeviam.mac.entities.SharedOwner;
import com.qfs.chunk.IChunk;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IReferenceDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.DuplicateKeyHandlers;
import com.qfs.desc.impl.StoreDescriptionBuilder;
import com.qfs.literal.ILiteralType;
import com.qfs.store.record.IRecordFormat;
import com.qfs.util.impl.QfsArrays;
import com.quartetfs.fwk.format.IParser;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contains all stores used to analyze a memory analysis dump.
 *
 * @author ActiveViam
 */
public class MemoryAnalysisDatastoreDescription implements IDatastoreSchemaDescription {

  /** Constant enabling the creation of Debug tree for each info, stored in the Datastore. */
  public static final boolean ADD_DEBUG_TREE = false;

  /** Name of the chunk <-> provider linking store. */
  public static final String CHUNK_TO_PROVIDER = "chunkToProvider";
  /** Name of the provider component <-> provider store. */
  public static final String PROVIDER_COMPONENT_TO_PROVIDER = "providerComponentToProvider";

  /** Name of the /** Name of the chunk <-> application linking store. */
  public static final String CHUNK_TO_APP = "ChunkToApp";

  /** Default value for store and field - fields. */
  public static final String DATASTORE_SHARED = "Shared";

  /** Default value for store and field - fields. */
  public static final String DEFAULT_DATASTORE = "Unknown";

  /** Default value for component-specific ids. */
  public static final Long DEFAULT_COMPONENT_ID_VALUE = -1L;
  /** Owner value for a chunk held by multiple components. */
  public static final ChunkOwner SHARED_OWNER = SharedOwner.getInstance();
  /** Component value for a chunk held by multiple components. */
  public static final String SHARED_COMPONENT = "shared";

  /** Partition value for chunks held by no partitions. */
  public static final int NO_PARTITION = -3;
  /** Partition value for chunks held by multiple partitions. */
  public static final int MANY_PARTITIONS = -2;

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

  /**
   * Description of the chunk store.
   *
   * @return description of {@link DatastoreConstants#CHUNK_STORE} (main store)
   */
  protected IStoreDescription chunkStore() {
    return new StoreDescriptionBuilder()
        .withStoreName(DatastoreConstants.CHUNK_STORE)
        // key
        .withField(DatastoreConstants.CHUNK_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK__DUMP_NAME, ILiteralType.STRING)
        .asKeyField()

        /* Foreign keys */
        .withField(DatastoreConstants.CHUNK__OWNER, ILiteralType.OBJECT, NoOwner.getInstance())
        .dictionarized()
        .withField(DatastoreConstants.CHUNK__COMPONENT, ILiteralType.OBJECT)
        .dictionarized()
        .withField(DatastoreConstants.CHUNK__PARTITION_ID, ILiteralType.INT, NO_PARTITION)
        .dictionarized()
        .withField(DatastoreConstants.CHUNK__PARENT_ID)
        .withField(DatastoreConstants.CHUNK__CLOSEST_PARENT_TYPE, ILiteralType.OBJECT)
        // Add 5 fields corresponding to the closest parent Id for a given type of parent
        .withField(
            DatastoreConstants.CHUNK__PARENT_DICO_ID, ILiteralType.LONG, DEFAULT_COMPONENT_ID_VALUE)
        .dictionarized()
        .withField(
            DatastoreConstants.CHUNK__PARENT_FIELD_NAME, ILiteralType.STRING, DEFAULT_DATASTORE)
        .dictionarized()
        .withField(
            DatastoreConstants.CHUNK__PARENT_STORE_NAME, ILiteralType.STRING, DEFAULT_DATASTORE)
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
        .withField(DatastoreConstants.CHUNK__PROVIDER_COMPONENT_TYPE)
        .withField(DatastoreConstants.CHUNK__CLASS)
        .withField(DatastoreConstants.CHUNK__OFF_HEAP_SIZE, ILiteralType.LONG)
        .withField(DatastoreConstants.CHUNK__ON_HEAP_SIZE, ILiteralType.LONG)
        .withField(DatastoreConstants.CHUNK__SIZE, ILiteralType.LONG)
        .withField(DatastoreConstants.CHUNK__NON_WRITTEN_ROWS, ILiteralType.LONG)
        .withField(DatastoreConstants.CHUNK__FREE_ROWS, ILiteralType.LONG)
        .withNullableField(DatastoreConstants.CHUNK__DEBUG_TREE, ILiteralType.STRING)
        .withDuplicateKeyHandler(new ChunkRecordHandler())
        .build();
  }

  /**
   * Description of the chunk store.
   *
   * @return description of {@link DatastoreConstants#CHUNK_TO_OWNER_STORE}
   */
  protected IStoreDescription chunkToOwnerStore() {
    return new StoreDescriptionBuilder()
        .withStoreName(DatastoreConstants.CHUNK_TO_OWNER_STORE)
        .withField(DatastoreConstants.OWNER__CHUNK_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.OWNER__OWNER, ILiteralType.OBJECT)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK__DUMP_NAME)
        .asKeyField()
        .build();
  }

  /**
   * Description of the chunk store.
   *
   * @return description of {@link DatastoreConstants#CHUNK_TO_COMPONENT_STORE}
   */
  protected IStoreDescription chunkToComponentStore() {
    return new StoreDescriptionBuilder()
        .withStoreName(DatastoreConstants.CHUNK_TO_COMPONENT_STORE)
        .withField(DatastoreConstants.COMPONENT__CHUNK_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.COMPONENT__COMPONENT, ILiteralType.OBJECT)
        .asKeyField()
        .withField(DatastoreConstants.CHUNK__DUMP_NAME)
        .asKeyField()
        .build();
  }

  /**
   * Description of the reference store.
   *
   * @return description of {@link DatastoreConstants#REFERENCE_STORE}
   */
  protected IStoreDescription referenceStore() {
    return new StoreDescriptionBuilder()
        .withStoreName(DatastoreConstants.REFERENCE_STORE)
        .withField(DatastoreConstants.REFERENCE_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
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
    return new StoreDescriptionBuilder()
        .withStoreName(DatastoreConstants.INDEX_STORE)
        .withField(DatastoreConstants.INDEX_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
        .asKeyField()

        /* Attributes */
        .withField(
            DatastoreConstants.INDEX_TYPE,
            ILiteralType.OBJECT) // FIXME(ope) primary, secondary, key
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
    return new StoreDescriptionBuilder()
        .withStoreName(DatastoreConstants.DICTIONARY_STORE)
        .withField(DatastoreConstants.DICTIONARY_ID, ILiteralType.LONG)
        .asKeyField()
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
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

        /* Attributes */
        .withField(DatastoreConstants.LEVEL__ON_HEAP_SIZE, ILiteralType.LONG)
        .withField(
            DatastoreConstants.LEVEL__OFF_HEAP_SIZE,
            ILiteralType.LONG) // TODO(ope) will be empty, but how to consider this in the cube
        .withField(DatastoreConstants.LEVEL__MEMBER_COUNT, ILiteralType.LONG)
        // TODO(ope) this is a base unit, introduce some versioning with the dump
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
   * Returns the description of {@link DatastoreConstants#PROVIDER_COMPONENT_STORE}.
   *
   * @return description of {@link DatastoreConstants#PROVIDER_COMPONENT_STORE}
   */
  protected IStoreDescription providerComponentStore() {
    return StartBuilding.store()
        .withStoreName(DatastoreConstants.PROVIDER_COMPONENT_STORE)
        .withField(DatastoreConstants.APPLICATION__DUMP_NAME)
        .asKeyField()
        .withField(DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID, ILiteralType.LONG)
        .asKeyField()
        /* Attributes */
        .withField(DatastoreConstants.PROVIDER_COMPONENT__TYPE)
        .asKeyField()
        .withField(DatastoreConstants.PROVIDER_COMPONENT__CLASS)
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

  // TODO(ope) add another store for global info. It shall be linked to a dump and a date, possibly
  // with the base entry

  @Override
  public Collection<? extends IStoreDescription> getStoreDescriptions() {
    return Arrays.asList(
        chunkStore(),
        chunkToOwnerStore(),
        chunkToComponentStore(),
        referenceStore(),
        indexStore(),
        dictionaryStore(),
        levelStore(),
        providerComponentStore(),
        providerStore(),
        pivotStore(),
        chunkTolevelStore(),
        applicationStore());
  }

  @Override
  public Collection<? extends IReferenceDescription> getReferenceDescriptions() {
    return Stream.of(getChunkReferences(), getPivotAndProviderReferences())
        .flatMap(Function.identity())
        .collect(Collectors.toList());
  }

  private Stream<IReferenceDescription> getChunkReferences() {
    return Stream.of(
        StartBuilding.reference()
            .fromStore(DatastoreConstants.CHUNK_STORE)
            .toStore(DatastoreConstants.PROVIDER_COMPONENT_STORE)
            .withName(CHUNK_TO_PROVIDER)
            .withMapping(
                DatastoreConstants.APPLICATION__DUMP_NAME,
                DatastoreConstants.APPLICATION__DUMP_NAME)
            .withMapping(
                DatastoreConstants.CHUNK__PROVIDER_ID,
                DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID)
            .withMapping(
                DatastoreConstants.CHUNK__PROVIDER_COMPONENT_TYPE,
                DatastoreConstants.PROVIDER_COMPONENT__TYPE)
            .build(),
        StartBuilding.reference()
            .fromStore(DatastoreConstants.CHUNK_STORE)
            .toStore(DatastoreConstants.APPLICATION_STORE)
            .withName(CHUNK_TO_APP)
            .withMapping(
                DatastoreConstants.CHUNK__DUMP_NAME, DatastoreConstants.APPLICATION__DUMP_NAME)
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
        // Provider component refs
        StartBuilding.reference()
            .fromStore(DatastoreConstants.PROVIDER_COMPONENT_STORE)
            .toStore(DatastoreConstants.PROVIDER_STORE)
            .withName(PROVIDER_COMPONENT_TO_PROVIDER)
            .withMapping(
                DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID,
                DatastoreConstants.PROVIDER__PROVIDER_ID)
            .withMapping(
                DatastoreConstants.APPLICATION__DUMP_NAME,
                DatastoreConstants.APPLICATION__DUMP_NAME)
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

  @Override
  public Collection<? extends IReferenceDescription> getSameDictionaryDescriptions() {
    // TODO(ope) report same fields, as some are shared
    return Collections.emptyList();
  }

  /**
   * Wrapper class around String[] (for equals, hashcode and toString).
   *
   * @author ActiveViam
   */
  public static class StringArrayObject {

    /** Default value for the list of fields. */
    protected static final StringArrayObject DEFAULT_VALUE =
        new StringArrayObject(IRecordFormat.GLOBAL_DEFAULT_STRING);

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
      result = prime * result + Arrays.hashCode(fieldNames);
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
      if (!Arrays.equals(fieldNames, other.fieldNames)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return QfsArrays.join(", ", fieldNames).toString();
    }
  }
}
