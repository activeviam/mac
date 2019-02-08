/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.monitoring.memory;

import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IReferenceDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.ReferenceDescription;
import com.qfs.desc.impl.StoreDescriptionBuilder;
import com.qfs.literal.ILiteralType;
import com.qfs.store.record.IRecordFormat;
import com.qfs.util.impl.QfsArrays;

import java.util.Arrays;
import java.util.Collection;

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
import static com.qfs.monitoring.memory.DatastoreConstants.DATE_PATTERN;
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
import static com.qfs.monitoring.memory.DatastoreConstants.INDEX_TYPE;
import static com.qfs.monitoring.memory.DatastoreConstants.LONG_IF_NOT_EXIST;
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

/**
 * Contains all stores used to analyze a memory analysis dump.
 *
 * @author Quartet FS
 */
public class MemoryAnalysisDatastoreDescription implements IDatastoreSchemaDescription {

	/**
	 * @return description of {@link DatastoreConstants#CHUNK_STORE} (main store)
	 */
	protected IStoreDescription chunkStoreDescription() {
		return new StoreDescriptionBuilder()
				.withStoreName(CHUNK_STORE)
				// key
				.withField(CHUNK_ID, ILiteralType.LONG).asKeyField()
				.withField(EPOCH_ID, ILiteralType.LONG).asKeyField()
				// references to secondary stores
				.withField(CHUNKSET_ID, ILiteralType.LONG, LONG_IF_NOT_EXIST)
				.withField(REFERENCE_ID, ILiteralType.LONG, LONG_IF_NOT_EXIST)
				.withField(INDEX_ID, ILiteralType.LONG, LONG_IF_NOT_EXIST)
				.withField(FIELDS, ILiteralType.OBJECT, StringArrayObject.DEFAULT_VALUE)
				.withField(DICTIONARY_ID, ILiteralType.LONG, LONG_IF_NOT_EXIST)
				// other fields
				.withField(STORE_AND_PARTITION_STORE_NAME)
				.withField(STORE_AND_PARTITION_PARTITION_ID, ILiteralType.INT)
				.withField(CHUNK_TYPE)
				.withField(CHUNK_CLASS)
				.withField(CHUNK_OFF_HEAP_SIZE, ILiteralType.LONG)
				.withField(CHUNK_ON_HEAP_SIZE, ILiteralType.LONG)
				.withField(DUMP_NAME, ILiteralType.STRING)
				.withField(EXPORT_DATE, "date[" + DATE_PATTERN + "]")
				.build();

	}

	/**
	 * @return description of {@link DatastoreConstants#CHUNK_SET_STORE}
	 */
	protected IStoreDescription chunkSetStoreDescription() {
		return new StoreDescriptionBuilder()
				.withStoreName(CHUNK_SET_STORE)
				.withField(CHUNKSET_ID, ILiteralType.LONG).asKeyField()
				.withField(EPOCH_ID, ILiteralType.LONG).asKeyField()
				.withField(CHUNK_SET_CLASS)
				.withField(CHUNK_SET_PHYSICAL_CHUNK_SIZE, ILiteralType.INT)
				.withField(CHUNK_SET_FREE_ROWS, ILiteralType.INT)
				.build();

	}

	/**
	 * @return description of {@link DatastoreConstants#REFERENCE_STORE}
	 */
	protected IStoreDescription referenceStoreDescription() {
		return new StoreDescriptionBuilder()
				.withStoreName(REFERENCE_STORE)
				.withField(REFERENCE_ID, ILiteralType.LONG).asKeyField()
				.withField(EPOCH_ID, ILiteralType.LONG).asKeyField()
				.withField(REFERENCE_NAME)
				.withField(REFERENCE_FROM_STORE)
				.withField(REFERENCE_FROM_STORE_PARTITION_ID, ILiteralType.INT)
				.withField(REFERENCE_TO_STORE)
				.withField(REFERENCE_TO_STORE_PARTITION_ID, ILiteralType.INT)
				.withField(REFERENCE_CLASS)
				.build();
	}

	/**
	 * @return description of {@link DatastoreConstants#INDEX_STORE}
	 */
	protected IStoreDescription indexStoreDescription() {
		return new StoreDescriptionBuilder()
				.withStoreName(INDEX_STORE)
				.withField(INDEX_ID, ILiteralType.LONG).asKeyField()
				.withField(EPOCH_ID, ILiteralType.LONG).asKeyField()
				.withField(INDEX_TYPE, ILiteralType.OBJECT)//FIXME primary, secondary, key
				.withField(INDEX_CLASS)
				.build();
	}

	/**
	 * @return description of {@link DatastoreConstants#DICTIONARY_STORE}
	 */
	protected IStoreDescription dictionaryStoreDescription() {
		return new StoreDescriptionBuilder()
				.withStoreName(DICTIONARY_STORE)
				.withField(DICTIONARY_ID, ILiteralType.LONG).asKeyField()
				.withField(EPOCH_ID, ILiteralType.LONG).asKeyField()
				.withField(DICTIONARY_SIZE, ILiteralType.INT)
				.withField(DICTIONARY_ORDER, ILiteralType.INT)
				.withField(DICTIONARY_CLASS)
				.build();
	}

	@Override
	public Collection<? extends IStoreDescription> getStoreDescriptions() {
		return Arrays.asList(
				chunkStoreDescription(),
				chunkSetStoreDescription(),
				referenceStoreDescription(),
				indexStoreDescription(),
				dictionaryStoreDescription());
	}

	@Override
	public Collection<? extends IReferenceDescription> getReferenceDescriptions() {
		return Arrays.asList(
				ReferenceDescription.builder()
						.fromStore(CHUNK_STORE)
						.toStore(REFERENCE_STORE)
						.withName("chunkToReferences")
						.withMapping(REFERENCE_ID, REFERENCE_ID)
						.withMapping(EPOCH_ID, EPOCH_ID)
						.build(),
				ReferenceDescription.builder()
						.fromStore(CHUNK_STORE)
						.toStore(INDEX_STORE)
						.withName("chunkToIndices")
						.withMapping(INDEX_ID, INDEX_ID)
						.withMapping(EPOCH_ID, EPOCH_ID)
						.build(),
				ReferenceDescription.builder()
						.fromStore(CHUNK_STORE)
						.toStore(CHUNK_SET_STORE)
						.withName("chunkToChunkSet")
						.withMapping(CHUNKSET_ID, CHUNKSET_ID)
						.withMapping(EPOCH_ID, EPOCH_ID)
						.build(),
				ReferenceDescription.builder().
						fromStore(CHUNK_STORE)
						.toStore(DICTIONARY_STORE)
						.withName("chunkToDic")
						.withMapping(DICTIONARY_ID, DICTIONARY_ID)
						.withMapping(EPOCH_ID, EPOCH_ID)
						.build()
				);
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
