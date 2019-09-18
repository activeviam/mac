/*
 * (C) ActiveViam 2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.pivot.monitoring.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.qfs.fwk.services.InternalServiceException;
import com.qfs.jackson.impl.JacksonSerializer;
import com.qfs.monitoring.statistic.IMonitoringStatistic;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link JsonDeserializer} for {@link IMonitoringStatistic}.
 *
 * @author ActiveViam
 * @param <T> Type of {@link IMonitoringStatistic} to deserialize
 */
public class MonitoringStatisticDeserializer<T extends IMonitoringStatistic> extends JsonDeserializer<T> {

	protected final Class<T> klass;

	/**
	 * {@link ObjectMapper} of this deserializer for the field
	 * "attributes". It can be use to change/customize the
	 * deserialization process.
	 */
	protected final ObjectMapper attributesObjectMapper;

	/**
	 * Default constructor.
	 */
	public MonitoringStatisticDeserializer(final Class<T> klass) {
		this.klass = klass;
		this.attributesObjectMapper = JacksonSerializer.getObjectMapper().copy();
		SimpleModule deserializeModule = new SimpleModule();
		deserializeModule.addDeserializer(
				Map.class,
				new MonitoringStatisticMapAttributesDeserializer(attributesObjectMapper));
		attributesObjectMapper.registerModule(deserializeModule);
	}

	@Override
	public T deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException {
		if (!JsonToken.START_OBJECT.equals(parser.currentToken())) {
			throw new IllegalArgumentException("Should be called at the start of an object");
		}

		parser.nextToken();
		final String name;
		if (Objects.equals(readFieldName(parser), MemoryStatisticAdapter.NAME_ATTR)) {
			readAndCheckFieldName(parser, MemoryStatisticAdapter.NAME_ATTR);
			name = readStringField(parser);
			parser.nextToken();
		} else {
			name = null;
		}

		final long onHeap;
		if (Objects.equals(readFieldName(parser), MemoryStatisticAdapter.ON_HEAP_ATTR)) {
			onHeap = readLongField(parser);

			parser.nextToken();
		} else {
			onHeap = -1;
		}

		final long offHeap;
		if (Objects.equals(readFieldName(parser), MemoryStatisticAdapter.OFF_HEAP_ATTR)) {
			offHeap = readLongField(parser);

			// Advance to the next step
			parser.nextToken();
		} else {
			offHeap = -1;
		}

		checkFieldName(MemoryStatisticAdapter.STATISTIC_CLASS_ATTR, readFieldName(parser));
		final String klassName = readStringField(parser);

		parser.nextToken();
		final Map<String, IStatisticAttribute> attributes;
		if (Objects.equals(readFieldName(parser), MemoryStatisticAdapter.ATTRIBUTES_ATTR)) {
			parser.nextToken();
			if (!Objects.equals(parser.currentToken(), JsonToken.START_OBJECT)) {
				throw new IllegalStateException(
						"Parser not pointing at the start of the attribute map. Got " + parser.currentToken());
			}
			final ObjectCodec currentCodec = parser.getCodec();
			parser.setCodec(this.attributesObjectMapper);
			try {
				attributes = parser.readValueAs(new TypeReference<Map<String, IStatisticAttribute>>() {});
			} finally {
				parser.setCodec(currentCodec);
			}
		} else {
			attributes = Collections.emptyMap();
		}

		parser.nextToken();
		final List<T> children;
		if (Objects.equals(readFieldName(parser), MemoryStatisticAdapter.CHILDREN_ATTR)) {
			parser.nextToken();
			if (!Objects.equals(parser.currentToken(), JsonToken.START_ARRAY)) {
				throw new IllegalStateException(
						"Parser not pointing at the start of the children list. Got " + parser.currentToken());
			}

			parser.nextToken(); // Consume the array start
			final Iterator<T> it = parser.readValuesAs(this.klass);
			children = new ArrayList<>();
			it.forEachRemaining(children::add);
			assert Objects.equals(parser.currentToken(), JsonToken.END_ARRAY);
			parser.nextToken(); // Consume the array end
		} else {
			children = Collections.emptyList();
		}

		supportExcessiveAttributes(parser);

		if (!Objects.equals(parser.currentToken(), JsonToken.END_OBJECT)) {
			throw new IllegalStateException(
					"Unexpected additional tokens. First is " + parser.currentToken());
		}

		return createDeserialized(
				klassName,
				name,
				onHeap,
				offHeap,
				attributes,
				children);
	}

	private void supportExcessiveAttributes(JsonParser parser) throws IOException {
		for (JsonToken token = parser.currentToken();
			!Objects.equals(token, JsonToken.END_OBJECT);
			token = parser.nextToken()) {
			if (Objects.equals(token, JsonToken.FIELD_NAME)) {
				final JsonToken value = parser.nextToken();
				if (value.isStructStart()) {
					throw new IllegalStateException("Not handling unexpected structures yet");
				}
			}
		}
	}

	protected String readStringField(final JsonParser parser) throws IOException {
		final String value = parser.nextTextValue();
		if (value != null) {
			return value;
		} else {
			throw new IllegalStateException(
					"Parser is not pointing to a text value. Got " + parser.currentToken());
		}
	}

	protected long readLongField(final JsonParser parser) throws IOException {
		final long value = parser.nextLongValue(0);
		if (value >= 0) {
			return value;
		} else {
			throw new IllegalStateException(
					"Parser is not pointing to a long value. Got " + parser.currentToken());
		}
	}

	protected String readFieldName(final JsonParser parser) throws IOException {
		final JsonToken token = parser.currentToken();
		if (token == null || Objects.equals(token, JsonToken.END_OBJECT)) {
			return null;
		} else if (Objects.equals(token, JsonToken.FIELD_NAME)) {
			return parser.getText();
		} else {
			throw new IllegalStateException("The parser is not pointing to a field. Got " + token);
		}
	}

	protected void readAndCheckFieldName(final JsonParser parser, final String fieldName)
			throws IOException {
		final String readName = readFieldName(parser);
		checkFieldName(fieldName, readName);
	}

	protected void checkFieldName(final String fieldName, final String readName) {
		if (!Objects.equals(fieldName, readName)) {
			throw new IllegalStateException(
					"The parser is not pointing at the correct attribute." +
							" Expecting " + fieldName + " but got " + readName);
		}
	}

	/**
	 * Creates the actual statistic using the parsed attributed.
	 * @param klassName name of the statistic class
	 * @param name name of the statistic (can be null)
	 * @param onHeap name of the statistic
	 * @param offHeap
	 * @param attributes
	 * @param children
	 * @return
	 */
	protected T createDeserialized(
			final String klassName,
			final String name,
			final long onHeap,
			final long offHeap,
			final Map<String, IStatisticAttribute> attributes,
			final List<T> children) {
		final Class<?> klass;
		try {
			klass = Class.forName(klassName);
		} catch (ClassNotFoundException e) {
			throw new InternalServiceException("Cannot find statistic class " + klassName, e);
		}

		final IMonitoringStatistic objDeserialized;
		try {
			objDeserialized = (IMonitoringStatistic) klass.newInstance();
		} catch (final InstantiationException | IllegalAccessException e) {
			throw new InternalServiceException("Cannot create instance of " + klassName, e);
		}

		try {
			klass.getMethod("setAttributes", Map.class).invoke(objDeserialized, attributes);
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new InternalServiceException("Cannot set attributes for class " + klassName, e);
		}
		try {
			klass.getMethod("setChildren", Collection.class).invoke(objDeserialized, children);
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new InternalServiceException("Cannot set children for class " + klassName, e);
		}

		return (T) objDeserialized;
	}

}
