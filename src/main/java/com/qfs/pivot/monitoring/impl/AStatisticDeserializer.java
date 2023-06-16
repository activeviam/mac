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
import com.qfs.jackson.impl.JacksonSerializer;
import com.qfs.monitoring.statistic.IMonitoringStatistic;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil.DeserializerTask;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Abstract base for {@link JsonDeserializer} for Memory statistics.
 *
 * @param <T> Type of {@link IMonitoringStatistic} to deserialize
 * @author ActiveViam
 */
public abstract class AStatisticDeserializer<T extends IMonitoringStatistic>
    extends JsonDeserializer<T> {

  /**
   * {@link ObjectMapper} of this deserializer for the field "attributes". It can be use to
   * change/customize the deserialization process.
   */
  protected final ObjectMapper attributesObjectMapper;

  /** Full constructor. */
  protected AStatisticDeserializer() {
    this.attributesObjectMapper = JacksonSerializer.getObjectMapper().copy();
    final SimpleModule deserializeModule = new SimpleModule();
    deserializeModule.addDeserializer(
        Map.class, new MonitoringStatisticMapAttributesDeserializer(attributesObjectMapper));
    attributesObjectMapper.registerModule(deserializeModule);
  }

  /**
   * Parses the attributes of a statistic serialized object.
   *
   * @param parser JSON parser moved at the start of the attribute list
   * @param ctx parsing context
   * @return deserialized attributes
   * @throws IOException if the parsing fails
   */
  protected Map<String, IStatisticAttribute> parseAttributes(
      final JsonParser parser, final DeserializationContext ctx) throws IOException {
    final Map<String, IStatisticAttribute> attributes;
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
    return attributes;
  }

  /**
   * Parses the children of a statistic serialized object.
   *
   * @param parser JSON parser moved at the start of the child list
   * @param ctx parsing context
   * @param klass type of the children statistic
   * @return deserialized children
   * @throws IOException if the parsing fails
   */
  protected List<T> parseChildren(
      final JsonParser parser, final DeserializationContext ctx, final Class<T> klass)
      throws IOException {
    if (!Objects.equals(parser.currentToken(), JsonToken.START_ARRAY)) {
      throw new IllegalStateException(
          "Parser not pointing at the start of the children list. Got " + parser.currentToken());
    }

    parser.nextToken(); // Consume the array start
    final List<T> children;

    final List<DeserializerTask<T>> computedTasks =
        (List<DeserializerTask<T>>) ctx.getAttribute(DeserializerTask.COMPUTED_SUBTASKS_ATTRIBUTE);
    final long parentPos =
        ctx.getAttribute(DeserializerTask.CURRENT_TASK_POSITION_ATTRIBUTE) == null
            ? Long.MAX_VALUE
            : (long) ctx.getAttribute(DeserializerTask.CURRENT_TASK_POSITION_ATTRIBUTE);

    if (computedTasks != null) {
      children = new ArrayList<>();
      while (parser.currentToken() != JsonToken.END_ARRAY) {
        final T child;
        Optional<DeserializerTask<T>> potentialComputedChildren =
            computedTasks.stream()
                .filter(
                    task -> task.start - parentPos + 1 == parser.currentLocation().getCharOffset())
                .findAny();
        if (potentialComputedChildren.isPresent()) {
          child = potentialComputedChildren.get().getRawResult();
          // no need to parse this child, just advance the parser
          do {
            parser.nextToken();
          } while (!(parser.currentToken() == JsonToken.START_OBJECT
              && parser.currentLocation().getCharOffset()
                  > potentialComputedChildren.get().end - parentPos + 1));
        } else {
          child = parser.readValueAs(klass);
          parser.nextToken();
        }
        children.add(child);
      }
    } else {
      if (parser.currentToken() != JsonToken.END_ARRAY) {
        final Iterator<T> it = parser.readValuesAs(klass);
        children = new ArrayList<>();
        it.forEachRemaining(children::add);
        assert Objects.equals(parser.currentToken(), JsonToken.END_ARRAY);
      } else {
        children = Collections.emptyList();
      }
    }
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
   *
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
   *
   * @param parser JSON parser to read
   * @return the read value
   * @throws IOException if the parsing failed
   */
  protected long readLongField(final JsonParser parser) throws IOException {
    final JsonToken valueToken = parser.nextToken();
    if (valueToken == JsonToken.VALUE_NUMBER_INT) {
      return parser.getLongValue();
    } else {
      throw new IllegalStateException("Parser is not pointing to a long value. Got " + valueToken);
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
   *
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
   *
   * @param fieldName expected field name
   * @param readName read value
   */
  protected void checkFieldName(final String fieldName, final String readName) {
    if (!Objects.equals(fieldName, readName)) {
      throw new IllegalStateException(
          "The parser is not pointing at the correct attribute."
              + " Expecting "
              + fieldName
              + " but got "
              + readName);
    }
  }
}
