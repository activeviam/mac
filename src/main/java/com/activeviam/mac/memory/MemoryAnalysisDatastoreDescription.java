/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.memory;

import java.util.Arrays;
import java.util.Collection;

import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IReferenceDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.ReferenceDescription;
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
	protected IStoreDescription chunkStoreDescription() {
		return new StoreDescriptionBuilder()
				.withStoreName(DatastoreConstants.CHUNK_STORE)
				// key
				.withField(DatastoreConstants.CHUNK_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.EPOCH_ID, ILiteralType.LONG).asKeyField()
				// references to secondary stores
				.withField(DatastoreConstants.CHUNKSET_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.withField(DatastoreConstants.REFERENCE_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.withField(DatastoreConstants.INDEX_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				.withField(DatastoreConstants.FIELDS, ILiteralType.OBJECT, StringArrayObject.DEFAULT_VALUE)
				.withField(DatastoreConstants.DICTIONARY_ID, ILiteralType.LONG, DatastoreConstants.LONG_IF_NOT_EXIST)
				// other fields
				.withField(DatastoreConstants.CHUNK__STORE_NAME)
				.withField(DatastoreConstants.CHUNK__PARTITION_ID, ILiteralType.INT)
				.withField(DatastoreConstants.CHUNK__TYPE)
				.withField(DatastoreConstants.CHUNK__CLASS)
				.withField(DatastoreConstants.CHUNK__OFF_HEAP_SIZE, ILiteralType.LONG)
				.withField(DatastoreConstants.CHUNK__ON_HEAP_SIZE, ILiteralType.LONG)
				.withField(DatastoreConstants.CHUNK__DUMP_NAME, ILiteralType.STRING)
				.withField(DatastoreConstants.CHUNK__EXPORT_DATE, IParser.DATE + "[" + DatastoreConstants.DATE_PATTERN + "]")
				.build();

	}

	/**
	 * @return description of {@link DatastoreConstants#CHUNK_SET_STORE}
	 */
	protected IStoreDescription chunkSetStoreDescription() {
		return new StoreDescriptionBuilder()
				.withStoreName(DatastoreConstants.CHUNK_SET_STORE)
				.withField(DatastoreConstants.CHUNKSET_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.EPOCH_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.CHUNK_SET_CLASS)
				.withField(DatastoreConstants.CHUNK_SET_PHYSICAL_CHUNK_SIZE, ILiteralType.INT)
				.withField(DatastoreConstants.CHUNK_SET_FREE_ROWS, ILiteralType.INT)
				.build();

	}

	/**
	 * @return description of {@link DatastoreConstants#REFERENCE_STORE}
	 */
	protected IStoreDescription referenceStoreDescription() {
		return new StoreDescriptionBuilder()
				.withStoreName(DatastoreConstants.REFERENCE_STORE)
				.withField(DatastoreConstants.REFERENCE_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.EPOCH_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.REFERENCE_NAME)
				.withField(DatastoreConstants.REFERENCE_FROM_STORE)
				.withField(DatastoreConstants.REFERENCE_FROM_STORE_PARTITION_ID, ILiteralType.INT)
				.withField(DatastoreConstants.REFERENCE_TO_STORE)
				.withField(DatastoreConstants.REFERENCE_TO_STORE_PARTITION_ID, ILiteralType.INT)
				.withField(DatastoreConstants.REFERENCE_CLASS)
				.build();
	}

	/**
	 * @return description of {@link DatastoreConstants#INDEX_STORE}
	 */
	protected IStoreDescription indexStoreDescription() {
		return new StoreDescriptionBuilder()
				.withStoreName(DatastoreConstants.INDEX_STORE)
				.withField(DatastoreConstants.INDEX_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.EPOCH_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.INDEX_TYPE, ILiteralType.OBJECT)//FIXME(ope) primary, secondary, key
				.withField(DatastoreConstants.INDEX_CLASS)
				.build();
	}

	/**
	 * @return description of {@link DatastoreConstants#DICTIONARY_STORE}
	 */
	protected IStoreDescription dictionaryStoreDescription() {
		return new StoreDescriptionBuilder()
				.withStoreName(DatastoreConstants.DICTIONARY_STORE)
				.withField(DatastoreConstants.DICTIONARY_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.EPOCH_ID, ILiteralType.LONG).asKeyField()
				.withField(DatastoreConstants.DICTIONARY_SIZE, ILiteralType.INT)
				.withField(DatastoreConstants.DICTIONARY_ORDER, ILiteralType.INT)
				.withField(DatastoreConstants.DICTIONARY_CLASS)
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
						.fromStore(DatastoreConstants.CHUNK_STORE)
						.toStore(DatastoreConstants.REFERENCE_STORE)
						.withName(CHUNK_TO_REF)
						.withMapping(DatastoreConstants.REFERENCE_ID, DatastoreConstants.REFERENCE_ID)
						.withMapping(DatastoreConstants.EPOCH_ID, DatastoreConstants.EPOCH_ID)
						.build(),
				ReferenceDescription.builder()
						.fromStore(DatastoreConstants.CHUNK_STORE)
						.toStore(DatastoreConstants.INDEX_STORE)
						.withName(CHUNK_TO_INDICES)
						.withMapping(DatastoreConstants.INDEX_ID, DatastoreConstants.INDEX_ID)
						.withMapping(DatastoreConstants.EPOCH_ID, DatastoreConstants.EPOCH_ID)
						.build(),
				ReferenceDescription.builder()
						.fromStore(DatastoreConstants.CHUNK_STORE)
						.toStore(DatastoreConstants.CHUNK_SET_STORE)
						.withName(CHUNK_TO_SETS)
						.withMapping(DatastoreConstants.CHUNKSET_ID, DatastoreConstants.CHUNKSET_ID)
						.withMapping(DatastoreConstants.EPOCH_ID, DatastoreConstants.EPOCH_ID)
						.build(),
				ReferenceDescription.builder().
						fromStore(DatastoreConstants.CHUNK_STORE)
						.toStore(DatastoreConstants.DICTIONARY_STORE)
						.withName(CHUNK_TO_DICS)
						.withMapping(DatastoreConstants.DICTIONARY_ID, DatastoreConstants.DICTIONARY_ID)
						.withMapping(DatastoreConstants.EPOCH_ID, DatastoreConstants.EPOCH_ID)
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
