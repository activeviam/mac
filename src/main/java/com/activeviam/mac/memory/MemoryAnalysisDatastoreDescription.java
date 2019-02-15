/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.memory;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.activeviam.builders.StartBuilding;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IReferenceDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.DuplicateKeyHandlers;
import com.qfs.desc.impl.StoreDescriptionBuilder;
import com.qfs.literal.ILiteralType;
import com.qfs.store.record.IRecordFormat;
import com.qfs.util.impl.QfsArrays;
import com.quartetfs.fwk.format.IParser;

/**
 * Contains all stores used to analyze a memory analysis dump.
 *
 * @author Quartet FS
 */
public class MemoryAnalysisDatastoreDescription implements IDatastoreSchemaDescription {

	public static final String CHUNK_TO_REF = "chunkToReferences";
	public static final String CHUNK_TO_INDICES = "chunkToIndices";
	public static final String CHUNK_TO_SETS = "chunkToChunkSet";
	public static final String CHUNK_TO_DICS = "chunkToDic";

	/**
	 * @return description of {@link DatastoreConstants#CHUNK_STORE} (main store)
	 */
	protected IStoreDescription chunkStore() {
		return new StoreDescriptionBuilder()
				.withStoreName(DatastoreConstants.CHUNK_STORE)
				// key
				.withField(DatastoreConstants.CHUNK_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.EPOCH_ID, ILiteralType.LONG).asKeyField()
				/* Foreign keys */
				.withField(DatastoreConstants.CHUNKSET_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.withField(DatastoreConstants.REFERENCE_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.withField(DatastoreConstants.INDEX_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.withField(DatastoreConstants.DICTIONARY_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.withField(DatastoreConstants.CHUNK__PROVIDER_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.withField(DatastoreConstants.CHUNK__PROVIDER_PARTITION_ID, ILiteralType.INT, DatastoreConstants.INT_IF_NOT_EXIST)
				.withField(DatastoreConstants.CHUNK__PROVIDER_COMPONENT_TYPE, ILiteralType.OBJECT)
				/* Attributes */
//				.withField(DatastoreConstants.CHUNK__STORE_NAME)
//				.withField(DatastoreConstants.CHUNK__PARTITION_ID, ILiteralType.INT)
//				.withField(DatastoreConstants.FIELDS, ILiteralType.OBJECT, StringArrayObject.DEFAULT_VALUE)
				.withField(DatastoreConstants.CHUNK__FIELD)
				.withField(DatastoreConstants.CHUNK__TYPE)
				.withField(DatastoreConstants.CHUNK__CLASS)
				.withField(DatastoreConstants.CHUNK__OFF_HEAP_SIZE, ILiteralType.LONG)
				.withField(DatastoreConstants.CHUNK__ON_HEAP_SIZE, ILiteralType.LONG)
				.withField(DatastoreConstants.CHUNK__DUMP_NAME, ILiteralType.STRING)
				.withField(DatastoreConstants.CHUNK__EXPORT_DATE, IParser.DATE + "[" + DatastoreConstants.DATE_PATTERN + "]")
				.build();

	}

	/**
	 * @return description of {@link DatastoreConstants#CHUNKSET_STORE}
	 */
	protected IStoreDescription chunkSetStore() {
		return new StoreDescriptionBuilder()
				.withStoreName(DatastoreConstants.CHUNKSET_STORE)
				.withField(DatastoreConstants.CHUNKSET_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.EPOCH_ID, ILiteralType.LONG).asKeyField()
				/* Foreign keys */
				.withField(DatastoreConstants.CHUNKSET__PARTITION, ILiteralType.INT)
				.withField(DatastoreConstants.CHUNKSET__STORE_NAME)
				.withField(DatastoreConstants.CHUNKSET__PROVIDER_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.withField(DatastoreConstants.CHUNKSET__PROVIDER_COMPONENT_TYPE, ILiteralType.OBJECT)
				.withField(DatastoreConstants.CHUNKSET__DICTIONARY_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				/* Attributes */
				.withField(DatastoreConstants.CHUNK_SET_CLASS)
				.withField(DatastoreConstants.CHUNKSET__TYPE)
				.withField(DatastoreConstants.CHUNK_SET_PHYSICAL_CHUNK_SIZE, ILiteralType.INT)
				.withField(DatastoreConstants.CHUNK_SET_FREE_ROWS, ILiteralType.INT)
				.withField(DatastoreConstants.CHUNKSET__FREED_ROWS, ILiteralType.INT)
				.build();

	}

	/**
	 * @return description of {@link DatastoreConstants#REFERENCE_STORE}
	 */
	protected IStoreDescription referenceStore() {
		return new StoreDescriptionBuilder()
				.withStoreName(DatastoreConstants.REFERENCE_STORE)
				.withField(DatastoreConstants.REFERENCE_ID, ILiteralType.LONG).asKeyField()
				/* Foreign keys */
				.withField(DatastoreConstants.REFERENCE_FROM_STORE)
				.withField(DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID, ILiteralType.INT)
				.withField(DatastoreConstants.REFERENCE_TO_STORE)
				.withField(DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID, ILiteralType.INT)
				/* Attributes */
				.withField(DatastoreConstants.REFERENCE_NAME)
				.withField(DatastoreConstants.REFERENCE_CLASS)
//				.withField(DatastoreConstants.REFERENCE__FIELDS, ILiteralType.OBJECT, StringArrayObject.DEFAULT_VALUE)
				.build();
	}

	/**
	 * @return description of {@link DatastoreConstants#INDEX_STORE}
	 */
	protected IStoreDescription indexStore() {
		return new StoreDescriptionBuilder()
				.withStoreName(DatastoreConstants.INDEX_STORE)
				.withField(DatastoreConstants.INDEX_ID, ILiteralType.LONG).asKeyField()
				/* Foreign keys */
				.withField(DatastoreConstants.INDEX__STORE_NAME)
				.withField(DatastoreConstants.INDEX__STORE_PARTITION, ILiteralType.INT, DatastoreConstants.INT_IF_NOT_EXIST)
				/* Attributes */
				.withField(DatastoreConstants.INDEX_TYPE, ILiteralType.OBJECT)//FIXME(ope) primary, secondary, key
				.withField(DatastoreConstants.INDEX_CLASS)
				.withField(DatastoreConstants.INDEX__FIELDS, ILiteralType.OBJECT)
				.build();
	}

	/**
	 * @return description of {@link DatastoreConstants#DICTIONARY_STORE}
	 */
	protected IStoreDescription dictionaryStore() {
		return new StoreDescriptionBuilder()
				.withStoreName(DatastoreConstants.DICTIONARY_STORE)
				.withField(DatastoreConstants.DICTIONARY_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.EPOCH_ID, ILiteralType.LONG).asKeyField()
				/* Foreign keys */
				.withField(DatastoreConstants.DIC__PROVIDER_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.withField(DatastoreConstants.DIC__PROVIDER_PARTITION_ID, ILiteralType.INT, DatastoreConstants.INT_IF_NOT_EXIST)
				.withField(DatastoreConstants.DIC__PROVIDER_COMPONENT_TYPE, ILiteralType.OBJECT)
				.withField(DatastoreConstants.DIC__INDEX_ID, ILiteralType.LONG)
				/* Attributes */
				.withField(DatastoreConstants.DICTIONARY_SIZE, ILiteralType.INT)
				.withField(DatastoreConstants.DICTIONARY_ORDER, ILiteralType.INT)
				.withField(DatastoreConstants.DICTIONARY_CLASS)
				.build();
	}

	protected IStoreDescription storePartitionStore() {
		return StartBuilding.store().withStoreName(DatastoreConstants.STORE_PARTITION_STORE)
				.withField(DatastoreConstants.STORE_PARTITION__STORE_NAME).asKeyField()
				.withField(DatastoreConstants.STORE_PARTIION__PARTITION_ID, ILiteralType.INT).asKeyField()
				// TODO(ope) add the epoch start and end of the partition
				.build();
	}

	protected IStoreDescription storeFieldStore() {
		return StartBuilding.store().withStoreName(DatastoreConstants.STORE_FIELD_STORE)
				// TODO(ope) generate an id for the datastore
				.withField(DatastoreConstants.STORE_FIELD__STORE_NAME).asKeyField()
				.withField(DatastoreConstants.STORE_FIELD__FIELD).asKeyField()
				.withField(DatastoreConstants.STORE_FIELD__DICTIONARY_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.build();
	}

	protected IStoreDescription storeStore() {
		return StartBuilding.store().withStoreName(DatastoreConstants.STORE_STORE)
				// TODO(ope) generate an id for the datastore
				.withField(DatastoreConstants.STORE__STORE_NAME).asKeyField()
				.build();
	}

	protected IStoreDescription levelStore() {
		return StartBuilding.store().withStoreName(DatastoreConstants.LEVEL_STORE)
				.withField(DatastoreConstants.LEVEL__MANAGER_ID).asKeyField()
				.withField(DatastoreConstants.LEVEL__PIVOT_ID).asKeyField()
				.withField(DatastoreConstants.LEVEL__DIMENSION).asKeyField()
				.withField(DatastoreConstants.LEVEL__HIERARCHY).asKeyField()
				.withField(DatastoreConstants.LEVEL__LEVEL).asKeyField()
				.withField(DatastoreConstants.LEVEL__EPOCH_ID, ILiteralType.LONG).asKeyField()
				/* Foreign keys */
				.withField(DatastoreConstants.LEVEL__DICTIONARY_ID).asKeyField()
				/* Attributes */
				.withField(DatastoreConstants.LEVEL__ON_HEAP_SIZE, ILiteralType.LONG)
				.withField(DatastoreConstants.LEVEL__OFF_HEAP_SIZE, ILiteralType.LONG) // TODO(ope) will be empty, but how to consider this in the cube
				.withField(DatastoreConstants.LEVEL__MEMBER_COUNT, ILiteralType.LONG)
				// TODO(ope) this is a base unit, introduce some versioning with the dump
				.build();
	}

	protected IStoreDescription providerComponentStore() {
		return StartBuilding.store().withStoreName(DatastoreConstants.PROVIDER_COMPONENT_STORE)
				.withField(DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.PROVIDER_COMPONENT__PARTITION_ID, ILiteralType.INT).asKeyField()
				.withField(DatastoreConstants.PROVIDER_COMPONENT__TYPE, ILiteralType.OBJECT).asKeyField()
				.withField(DatastoreConstants.PROVIDER_COMPONENT__EPOCH_ID, ILiteralType.LONG).asKeyField()
				/* Attributes */
				.withField(DatastoreConstants.PROVIDER_COMPONENT__CLASS)
				.withField(DatastoreConstants.PROVIDER_COMPONENT__ON_HEAP_SIZE, ILiteralType.LONG)
				.withField(DatastoreConstants.PROVIDER_COMPONENT__OFF_HEAP_SIZE, ILiteralType.LONG) // TODO(ope) will be empty, but how to consider this in the cube
				// TODO(ope) this is a base unit, introduce some versioning with the dump
				.build();
	}

	protected IStoreDescription providerStore() {
		return StartBuilding.store().withStoreName(DatastoreConstants.PROVIDER_STORE)
				.withField(DatastoreConstants.PROVIDER__PROVIDER_ID, ILiteralType.LONG).asKeyField()
				/* Foreign keys */
				.withField(DatastoreConstants.PROVIDER__PIVOT_ID)
				.withField(DatastoreConstants.PROVIDER__MANAGER_ID)
				/* Attributes */
				.withField(DatastoreConstants.PROVIDER__INDEX, ILiteralType.STRING, "<None>")
				.withField(DatastoreConstants.PROVIDER__TYPE)
				.withField(DatastoreConstants.PROVIDER__CATEGORY)
				.build();
	}

	protected IStoreDescription pivotStore() {
		return StartBuilding.store().withStoreName(DatastoreConstants.PIVOT_STORE)
				.withField(DatastoreConstants.PIVOT__PIVOT_ID).asKeyField()
				.withField(DatastoreConstants.PIVOT__MANAGER_ID).asKeyField()
				.build();
	}

	protected IStoreDescription applicationStore() {
		return StartBuilding.store().withStoreName(DatastoreConstants.APPLICATION_STORE)
				.withField(DatastoreConstants.APPLICATION__DATE, ILiteralType.LOCAL_DATE_TIME).asKeyField()
				.withField(DatastoreConstants.APPLICATION__EPOCH, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.APPLICATION__DUMP_NAME).asKeyField()
				.withField(DatastoreConstants.APPLICATION__USED_ON_HEAP, ILiteralType.LONG)
				.withField(DatastoreConstants.APPLICATION__MAX_ON_HEAP, ILiteralType.LONG)
				.withField(DatastoreConstants.APPLICATION__USED_OFF_HEAP, ILiteralType.LONG)
				.withField(DatastoreConstants.APPLICATION__MAX_OFF_HEAP, ILiteralType.LONG)
				.withDuplicateKeyHandler(DuplicateKeyHandlers.THROW_WITHIN_TRANSACTION)
				.build();
	}

	// TODO(ope) add another store for global info. It shall be linked to a dump and a date, possibly with the base entry

	@Override
	public Collection<? extends IStoreDescription> getStoreDescriptions() {
		return Arrays.asList(
				chunkStore(),
				chunkSetStore(),
				referenceStore(),
				indexStore(),
				dictionaryStore(),
				storePartitionStore(),
				storeFieldStore(),
				storeStore(),
				levelStore(),
				providerComponentStore(),
				providerStore(),
				pivotStore(),
				applicationStore());
	}

	@Override
	public Collection<? extends IReferenceDescription> getReferenceDescriptions() {
		return Stream.of(
				getChunkReferences(),
				getChunksetReferences(),
				Arrays.asList(
						// Index refs
						StartBuilding.reference()
								.fromStore(DatastoreConstants.INDEX_STORE).toStore(DatastoreConstants.STORE_PARTITION_STORE)
								.withName("IndexToStore")
								.withMapping(DatastoreConstants.INDEX__STORE_NAME, DatastoreConstants.STORE_PARTITION__STORE_NAME)
								.withMapping(DatastoreConstants.INDEX__STORE_PARTITION, DatastoreConstants.STORE_PARTIION__PARTITION_ID)
								.build(),
						// Reference refs
						StartBuilding.reference()
								.fromStore(DatastoreConstants.REFERENCE_STORE).toStore(DatastoreConstants.STORE_PARTITION_STORE)
								.withName("RefToStore")
								.withMapping(DatastoreConstants.REFERENCE_FROM_STORE, DatastoreConstants.STORE_PARTITION__STORE_NAME)
								.withMapping(DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID, DatastoreConstants.STORE_PARTIION__PARTITION_ID)
								.build(),
						// Partitions to store
						StartBuilding.reference()
								.fromStore(DatastoreConstants.STORE_PARTITION_STORE).toStore(DatastoreConstants.STORE_STORE)
								.withName("PartitionToStore")
								.withMapping(DatastoreConstants.STORE_PARTITION__STORE_NAME, DatastoreConstants.STORE__STORE_NAME)
								.build(),
						// Level refs
						StartBuilding.reference()
								.fromStore(DatastoreConstants.LEVEL_STORE).toStore(DatastoreConstants.PIVOT_STORE)
								.withName("LevelToPivot")
								.withMapping(DatastoreConstants.LEVEL__PIVOT_ID, DatastoreConstants.PIVOT__PIVOT_ID)
								.withMapping(DatastoreConstants.LEVEL__MANAGER_ID, DatastoreConstants.PIVOT__MANAGER_ID)
								.build(),
						// Provider component refs
						StartBuilding.reference()
								.fromStore(DatastoreConstants.PROVIDER_COMPONENT_STORE).toStore(DatastoreConstants.PROVIDER_STORE)
								.withName("ProviderComponentToPartition")
								.withMapping(DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID, DatastoreConstants.PROVIDER__PROVIDER_ID)
								.build(),
						// Provider partitions refs
						StartBuilding.reference()
								.fromStore(DatastoreConstants.PROVIDER_STORE).toStore(DatastoreConstants.PIVOT_STORE)
								.withName("ProviderToPivot")
								.withMapping(DatastoreConstants.PROVIDER__PIVOT_ID, DatastoreConstants.PIVOT__PIVOT_ID)
								.withMapping(DatastoreConstants.PROVIDER__MANAGER_ID, DatastoreConstants.PIVOT__MANAGER_ID)
								.build()))
				.flatMap(Collection::stream)
				.collect(Collectors.toList());
	}

	private Collection<IReferenceDescription> getChunkReferences() {
		return Arrays.asList(
				StartBuilding.reference()
						.fromStore(DatastoreConstants.CHUNK_STORE)
						.toStore(DatastoreConstants.REFERENCE_STORE)
						.withName(CHUNK_TO_REF)
						.withMapping(DatastoreConstants.REFERENCE_ID, DatastoreConstants.REFERENCE_ID)
						.build(),
				StartBuilding.reference()
						.fromStore(DatastoreConstants.CHUNK_STORE)
						.toStore(DatastoreConstants.INDEX_STORE)
						.withName(CHUNK_TO_INDICES)
						.withMapping(DatastoreConstants.INDEX_ID, DatastoreConstants.INDEX_ID)
						.build(),
				StartBuilding.reference()
						.fromStore(DatastoreConstants.CHUNK_STORE)
						.toStore(DatastoreConstants.CHUNKSET_STORE)
						.withName(CHUNK_TO_SETS)
						.withMapping(DatastoreConstants.CHUNKSET_ID, DatastoreConstants.CHUNKSET_ID)
						.withMapping(DatastoreConstants.EPOCH_ID, DatastoreConstants.EPOCH_ID)
						.build(),
				StartBuilding.reference()
						.fromStore(DatastoreConstants.CHUNK_STORE)
						.toStore(DatastoreConstants.DICTIONARY_STORE)
						.withName(CHUNK_TO_DICS)
						.withMapping(DatastoreConstants.DICTIONARY_ID, DatastoreConstants.DICTIONARY_ID)
						.withMapping(DatastoreConstants.EPOCH_ID, DatastoreConstants.EPOCH_ID)
						.build(),
				StartBuilding.reference()
						.fromStore(DatastoreConstants.CHUNK_STORE).toStore(DatastoreConstants.PROVIDER_COMPONENT_STORE)
						.withName("ChunkToProvider")
						.withMapping(DatastoreConstants.CHUNK__PROVIDER_ID, DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID)
						.withMapping(DatastoreConstants.CHUNK__PROVIDER_PARTITION_ID, DatastoreConstants.PROVIDER_COMPONENT__PARTITION_ID)
						.withMapping(DatastoreConstants.CHUNK__PROVIDER_COMPONENT_TYPE, DatastoreConstants.PROVIDER_COMPONENT__TYPE)
						.build());
	}

	protected Collection<IReferenceDescription> getChunksetReferences() {
		return Arrays.asList(
				StartBuilding.reference()
						.fromStore(DatastoreConstants.CHUNKSET_STORE).toStore(DatastoreConstants.STORE_PARTITION_STORE)
						.withName("ChunksetToStore")
						.withMapping(DatastoreConstants.CHUNKSET__STORE_NAME, DatastoreConstants.STORE_PARTITION__STORE_NAME)
						.withMapping(DatastoreConstants.CHUNKSET__PARTITION, DatastoreConstants.STORE_PARTIION__PARTITION_ID)
						.build(),
				StartBuilding.reference()
						.fromStore(DatastoreConstants.CHUNKSET_STORE).toStore(DatastoreConstants.PROVIDER_COMPONENT_STORE)
						.withName("ChunksetToProvider")
						.withMapping(DatastoreConstants.CHUNKSET__PROVIDER_ID, DatastoreConstants.PROVIDER_COMPONENT__PROVIDER_ID)
						.withMapping(DatastoreConstants.CHUNKSET__PARTITION, DatastoreConstants.PROVIDER_COMPONENT__PARTITION_ID)
						.withMapping(DatastoreConstants.CHUNKSET__PROVIDER_COMPONENT_TYPE, DatastoreConstants.PROVIDER_COMPONENT__TYPE)
						.withMapping(DatastoreConstants.EPOCH_ID, DatastoreConstants.PROVIDER_COMPONENT__EPOCH_ID)
						.build(),
				StartBuilding.reference()
						.fromStore(DatastoreConstants.CHUNKSET_STORE).toStore(DatastoreConstants.DICTIONARY_STORE)
						.withName("ChunksetToDictionary")
						.withMapping(DatastoreConstants.CHUNKSET__DICTIONARY_ID, DatastoreConstants.DICTIONARY_ID)
						.withMapping(DatastoreConstants.EPOCH_ID, DatastoreConstants.EPOCH_ID)
						.build());
	}

	/**
	 * Wrapper class around String[] (for equals, hashcode and toStrin).
	 * @author Quartet FS
	 */
	public static class StringArrayObject {

		/** Default value for the list of fields. */
		protected static final StringArrayObject DEFAULT_VALUE = new StringArrayObject(IRecordFormat.GLOBAL_DEFAULT_STRING);

		/** Underlying array */
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
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			StringArrayObject other = (StringArrayObject) obj;
			if (!Arrays.equals(fieldNames, other.fieldNames))
				return false;
			return true;
		}

		@Override
		public String toString() {
			return QfsArrays.join(", ", fieldNames).toString();
		}
	}
}
