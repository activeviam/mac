/*
 * (C) ActiveViam 2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.pivot.monitoring.impl;

import static com.qfs.pivot.monitoring.impl.MonitoringStatisticMapAttributesSerializer.CLASS_INFO;
import static com.qfs.pivot.monitoring.impl.MonitoringStatisticMapAttributesSerializer.CONTENT_TYPE_INFO;
import static com.qfs.pivot.monitoring.impl.MonitoringStatisticMapAttributesSerializer.PARSER_INFO;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.qfs.monitoring.statistic.IMonitoringStatistic;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.impl.BooleanArrayStatisticAttribute;
import com.qfs.monitoring.statistic.impl.BooleanStatisticAttribute;
import com.qfs.monitoring.statistic.impl.DoubleArrayStatisticAttribute;
import com.qfs.monitoring.statistic.impl.DoubleStatisticAttribute;
import com.qfs.monitoring.statistic.impl.FloatArrayStatisticAttribute;
import com.qfs.monitoring.statistic.impl.FloatStatisticAttribute;
import com.qfs.monitoring.statistic.impl.IntegerArrayStatisticAttribute;
import com.qfs.monitoring.statistic.impl.IntegerStatisticAttribute;
import com.qfs.monitoring.statistic.impl.LongArrayStatisticAttribute;
import com.qfs.monitoring.statistic.impl.LongStatisticAttribute;
import com.qfs.monitoring.statistic.impl.ObjectArrayStatisticAttribute;
import com.qfs.monitoring.statistic.impl.ObjectStatisticAttribute;
import com.qfs.monitoring.statistic.impl.StringArrayStatisticAttribute;
import com.qfs.monitoring.statistic.impl.StringStatisticAttribute;
import com.qfs.store.Types;
import com.quartetfs.fwk.QuartetRuntimeException;
import com.quartetfs.fwk.Registry;
import com.quartetfs.fwk.format.IParser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link JsonDeserializer} for {@link IMonitoringStatistic}.
 *
 * @author ActiveViam
 */
public class MonitoringStatisticMapAttributesDeserializer extends JsonDeserializer<Map<String, IStatisticAttribute>> {

	/**
	 * {@link ObjectMapper} of this deserializer for the field
	 * "attributes". It can be use to change/customize the
	 * deserialization process.
	 */
	protected ObjectMapper attributesObjectMapper;

	/**
	 * Default constructor.
	 *
	 * @param attributesObjectMapper {@link ObjectMapper} to use.
	 */
	public MonitoringStatisticMapAttributesDeserializer(ObjectMapper attributesObjectMapper) {
		this.attributesObjectMapper = attributesObjectMapper;
	}

	@Override
	public Map<String, IStatisticAttribute> deserialize(JsonParser parser, DeserializationContext ctxt)
			throws IOException {
		if (!JsonToken.START_OBJECT.equals(parser.currentToken())) {
			throw new IllegalArgumentException("Should be called at the start of an object");
		}

		final Map<String, IStatisticAttribute> result = new HashMap<>();
		for (
				JsonToken token = parser.nextToken();
				!Objects.equals(token, JsonToken.END_OBJECT);
				token = parser.nextToken()) {
			if (!Objects.equals(token, JsonToken.FIELD_NAME)) {
				throw new IllegalStateException("Expecting a field name but got " + token);
			}
			final String key = parser.getText();

			final String[] keys;
			String parserPlugin = null, klassName = null, type = null;
			if (key.contains(CONTENT_TYPE_INFO)) {
				keys = key.split(CONTENT_TYPE_INFO);
				type = keys[1];
			} else if (key.contains(CLASS_INFO)) {
				keys = key.split(CLASS_INFO);
				klassName = keys[1];
			} else if (key.contains(PARSER_INFO)) {
				keys = key.split(PARSER_INFO);
				parserPlugin = keys[1];
			} else {
				throw new QuartetRuntimeException("Unexpected key '" + key + "'. It should contain either "
						+ CONTENT_TYPE_INFO + " or " + CLASS_INFO + " or " + PARSER_INFO);
			}

			parser.nextToken(); // Move to value
			if (parserPlugin != null || klassName != null) {
				final IStatisticAttribute value = deserializedWithParserOrClassName(
						parser,
						parserPlugin,
						klassName);
				result.put(keys[0], value);
			} else if (type != null) {
				final IStatisticAttribute value = deserializedWithContentType(
						parser,
						Integer.parseInt(type));
				result.put(keys[0], value);
			} else {
				throw new QuartetRuntimeException("Unexpected key '" + key + "'. It should contain either"
						+ CONTENT_TYPE_INFO + " or " + CLASS_INFO + " or " + PARSER_INFO);
			}
		}
		return result;
	}

	/**
	 * @param klassName class name of the object
	 * @param node object's node
	 * @return object
	 * @throws IOException If an I/O error occurs
	 */
	protected Object deserializedWithClassName(
			final String klassName,
			final JsonParser parser) throws IOException {
		final Class<?> klass;
		try {
			klass = Class.forName(klassName);
		} catch (ClassNotFoundException e1) {
			throw new QuartetRuntimeException(e1);
		}
		return parser.readValueAs(klass);
	}

	/**
	 * @param parserPlugin plugin key of the {@link IParser}
	 * @param node object's node
	 * @return object
	 * @throws IOException If an I/O error occurs
	 */
	protected Object deserializedWithParser(
			final String parserPlugin,
			final JsonParser parser) throws IOException {
		final IParser<?> parserValue = Registry.getPlugin(IParser.class).valueOf(parserPlugin);
		return parserValue.parse(parser.getText());
	}

	/**
	 * @param node object's node
	 * @param parserPlugin plugin key of the {@link IParser}
	 * @param klassName class name of the object
	 * @return object
	 * @throws IOException If an I/O error occurs
	 */
	protected IStatisticAttribute deserializedWithParserOrClassName(
			final JsonParser parser,
			final String parserPlugin,
			final String klassName) throws IOException {
		if (Objects.equals(parser.currentToken(), JsonToken.START_ARRAY)) {
			final List<Object> stats = new ArrayList<>();
			for (
					JsonToken token = parser.nextToken();
					!Objects.equals(token, JsonToken.END_ARRAY);
					token = parser.nextToken()) {
				final Object stat = klassName != null
								? deserializedWithClassName(klassName, parser)
								: deserializedWithParser(parserPlugin, parser);
				stats.add(stat);
			}
			return new ObjectArrayStatisticAttribute(stats.toArray());
		} else {
			return new ObjectStatisticAttribute(klassName != null
					? deserializedWithClassName(klassName, parser)
					: deserializedWithParser(parserPlugin, parser));
		}
	}

	/**
	 * @param node object's node
	 * @param contentType the content type
	 * @return object
	 * @throws IOException If an I/O error occurs
	 */
	protected IStatisticAttribute deserializedWithContentType(
			final JsonParser parser,
			final int contentType) throws IOException {
		if (Objects.equals(parser.currentToken(), JsonToken.START_ARRAY)) {
			switch (contentType) {
			case Types.CONTENT_INT: {
				int[] values = new int[16];
				int i = 0;
				int capacity = values.length - 1;
				for (
						JsonToken token = parser.nextToken();
						!Objects.equals(token, JsonToken.END_ARRAY);
						token = parser.nextToken()) {
					final int value = parser.getValueAsInt();
					if (i == capacity) {
						values = Arrays.copyOf(values, values.length * 2);
						capacity = values.length - 1;
					}
					values[i] = value;
					i += 1;
				}
				values = Arrays.copyOf(values, i);
				return new IntegerArrayStatisticAttribute(values);
			}
			case Types.CONTENT_LONG: {
				long[] values = new long[16];
				int i = 0;
				int capacity = values.length - 1;
				for (
						JsonToken token = parser.nextToken();
						!Objects.equals(token, JsonToken.END_ARRAY);
						token = parser.nextToken()) {
					final long value = parser.getValueAsLong();
					if (i == capacity) {
						values = Arrays.copyOf(values, values.length * 2);
						capacity = values.length - 1;
					}
					values[i] = value;
					i += 1;
				}
				values = Arrays.copyOf(values, i);
				return new LongArrayStatisticAttribute(values);
			}
			case Types.CONTENT_FLOAT: {
				float[] values = new float[16];
				int i = 0;
				int capacity = values.length - 1;
				for (
						JsonToken token = parser.nextToken();
						!Objects.equals(token, JsonToken.END_ARRAY);
						token = parser.nextToken()) {
					final float value = parser.getFloatValue();
					if (i == capacity) {
						values = Arrays.copyOf(values, values.length * 2);
						capacity = values.length - 1;
					}
					values[i] = value;
					i += 1;
				}
				values = Arrays.copyOf(values, i);
				return new FloatArrayStatisticAttribute(values);
			}
			case Types.CONTENT_DOUBLE: {
				double[] values = new double[16];
				int i = 0;
				int capacity = values.length - 1;
				for (
						JsonToken token = parser.nextToken();
						!Objects.equals(token, JsonToken.END_ARRAY);
						token = parser.nextToken()) {
					final double value = parser.getDoubleValue();
					if (i == capacity) {
						values = Arrays.copyOf(values, values.length * 2);
						capacity = values.length - 1;
					}
					values[i] = value;
					i += 1;
				}
				values = Arrays.copyOf(values, i);
				return new DoubleArrayStatisticAttribute(values);
			}
			case Types.CONTENT_BOOLEAN: {
				boolean[] values = new boolean[16];
				int i = 0;
				int capacity = values.length - 1;
				for (
						JsonToken token = parser.nextToken();
						!Objects.equals(token, JsonToken.END_ARRAY);
						token = parser.nextToken()) {
					final boolean value = parser.getBooleanValue();
					if (i == capacity) {
						values = Arrays.copyOf(values, values.length * 2);
						capacity = values.length - 1;
					}
					values[i] = value;
					i += 1;
				}
				values = Arrays.copyOf(values, i);
				return new BooleanArrayStatisticAttribute(values);
			}
			case Types.CONTENT_STRING: {
				final List<String> values = new ArrayList<>();
				for (
						JsonToken token = parser.nextToken();
						!Objects.equals(token, JsonToken.END_ARRAY);
						token = parser.nextToken()) {
					final String value = parser.getText();
					values.add(value);
				}
				return new StringArrayStatisticAttribute(values.toArray(new String[0]));
			}
			default:
				throw new QuartetRuntimeException(
						"Unsupported array type " + Types.toString(contentType) + "(" + contentType + ")");
			}
		} else {
			final IStatisticAttribute attribute = createPrimitiveAttribute(parser, contentType);
			return attribute;
		}
	}

	private IStatisticAttribute createPrimitiveAttribute(JsonParser parser, int contentType)
			throws IOException {
		switch (contentType) {

		case Types.CONTENT_INT:
			return new IntegerStatisticAttribute(parser.getIntValue());
		case Types.CONTENT_LONG:
			return new LongStatisticAttribute(parser.getLongValue());
		case Types.CONTENT_DOUBLE:
			return new DoubleStatisticAttribute(parser.getDoubleValue());
		case Types.CONTENT_FLOAT:
			return new FloatStatisticAttribute(parser.getFloatValue());
		case Types.CONTENT_BOOLEAN:
			return new BooleanStatisticAttribute(parser.getBooleanValue());
		case Types.CONTENT_STRING:
			return new StringStatisticAttribute(parser.getText());
		default:
			throw new QuartetRuntimeException("Unsupported type " + contentType);
		}
	}

}
