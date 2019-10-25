/*
 * (C) ActiveViam 2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.qfs.pivot.monitoring.impl;

import com.activeviam.mac.Workaround;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.qfs.jackson.impl.JacksonSerializer;
import com.qfs.monitoring.statistic.IMonitoringStatistic;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Abstract base for {@link JsonDeserializer} for Memory statistics.
 *
 * @param <T> Type of {@link IMonitoringStatistic} to deserialize
 *
 * @author ActiveViam
 */
@Workaround(jira = "PIVOT-4093", solution = "Waiting for the next version with the fix")
public abstract class AStatisticDeserializer<T extends IMonitoringStatistic>
		extends JsonDeserializer<T> {

	/**
	 * {@link ObjectMapper} of this deserializer for the field
	 * "attributes". It can be use to change/customize the
	 * deserialization process.
	 */
	protected final ObjectMapper attributesObjectMapper;

	/**
	 * Full constructor.
	 */
	public AStatisticDeserializer() {
		this.attributesObjectMapper = JacksonSerializer.getObjectMapper().copy();
		SimpleModule deserializeModule = new SimpleModule();
		deserializeModule.addDeserializer(
				Map.class,
				new MonitoringStatisticMapAttributesDeserializer(attributesObjectMapper));
		attributesObjectMapper.registerModule(deserializeModule);
	}

	/**
	 * Parses the attributes of a statistic serialized object.
	 * @param parser JSON parser moved at the start of the attribute list
	 * @param ctx parsing context
	 * @return deserialized attributes
	 * @throws IOException if the parsing fails
	 */
	protected Map<String, IStatisticAttribute> parseAttributes(
			final JsonParser parser,
			final DeserializationContext ctx) throws IOException {
		final Map<String, IStatisticAttribute> attributes;
		if (!Objects.equals(parser.currentToken(), JsonToken.START_OBJECT)) {
			throw new IllegalStateException(
					"Parser not pointing at the start of the attribute map. Got " + parser.currentToken());
		}
		final ObjectCodec currentCodec = parser.getCodec();
		parser.setCodec(this.attributesObjectMapper);
		try {
			attributes = parser.readValueAs(new TypeReference<Map<String, IStatisticAttribute>>() {
			});
		} finally {
			parser.setCodec(currentCodec);
		}
		return attributes;
	}

	/**
	 * Parses the children of a statistic serialized object.
	 * @param parser JSON parser moved at the start of the child list
	 * @param ctx parsing context
	 * @param klass type of the children statistic
	 * @return deserialized children
	 * @throws IOException if the parsing fails
	 */
	protected List<T> parseChildren(
			final JsonParser parser,
			final DeserializationContext ctx,
			final Class<T> klass) throws IOException {
		if (!Objects.equals(parser.currentToken(), JsonToken.START_ARRAY)) {
			throw new IllegalStateException(
					"Parser not pointing at the start of the children list. Got " + parser.currentToken());
		}

		parser.nextToken(); // Consume the array start
		final Iterator<T> it = parser.readValuesAs(klass);
		final List<T> children = new ArrayList<>();
		it.forEachRemaining(children::add);
		assert Objects.equals(parser.currentToken(), JsonToken.END_ARRAY);
		parser.nextToken(); // Consume the array end

		return children;
	}

	/**
	 * Handles extra attributes at the end of the serialized statistics.
	 *
	 * <p>The current behaviour is to ignore extra textual attributes and fails for extra complex
	 * attributes.<br>
	 * It is expected to extend this method to restrict and loosen the restriction.
	 *
	 * @param parser JSON parser, positioned at the expected end of the serialized structure
	 * @throws IOException if the parsing failed
	 */
	protected void handleExcessiveAttributes(final JsonParser parser) throws IOException {
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

	/**
	 * Reads a String value from the parser.
	 * @param parser JSON parser to read
	 * @return the read value
	 * @throws IOException if the parsing failed
	 */
	protected String readStringField(final JsonParser parser) throws IOException {
		final String value = parser.nextTextValue();
		if (value != null) {
			return value;
		} else {
			throw new IllegalStateException(
					"Parser is not pointing to a text value. Got " + parser.currentToken());
		}
	}

	/**
	 * Reads a long value from the parser.
	 * @param parser JSON parser to read
	 * @return the read value
	 * @throws IOException if the parsing failed
	 */
	protected long readLongField(final JsonParser parser) throws IOException {
		final long value = parser.nextLongValue(0);
		if (value >= 0) {
			return value;
		} else {
			throw new IllegalStateException(
					"Parser is not pointing to a long value. Got " + parser.currentToken());
		}
	}

	/**
	 * Reads the name of a JSON object field from the parser.
	 *
	 * <p>This performs checks to ensure that the parser is positioned on a field.
	 *
	 * @param parser JSON parser to read
	 * @return the read value
	 * @throws IOException if the parsing failed
	 */
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

	/**
	 * Consumes the parser to read a field of a given name.
	 * @param parser JSON parser to use
	 * @param fieldName expected name of the field
	 * @throws IOException if the parsing failed
	 */
	protected void readAndCheckFieldName(final JsonParser parser, final String fieldName)
			throws IOException {
		final String readName = readFieldName(parser);
		checkFieldName(fieldName, readName);
	}

	/**
	 * Checks a field name against an expected value.
	 * @param fieldName expected field name
	 * @param readName read value
	 */
	protected void checkFieldName(final String fieldName, final String readName) {
		if (!Objects.equals(fieldName, readName)) {
			throw new IllegalStateException(
					"The parser is not pointing at the correct attribute."
							+ " Expecting " + fieldName + " but got " + readName);
		}
	}


}
