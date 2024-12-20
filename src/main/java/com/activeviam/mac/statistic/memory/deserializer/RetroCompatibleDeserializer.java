/*
 * (C) ActiveViam 2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.deserializer;

import static com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryStatisticSerializerUtil.COMPRESSED_FILE_EXTENSION;

import com.activeviam.activepivot.server.impl.private_.observability.memory.AStatisticDeserializer;
import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryStatisticAdapter;
import com.activeviam.tech.core.api.exceptions.ActiveViamRuntimeException;
import com.activeviam.tech.core.api.exceptions.service.InternalServiceException;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
import com.activeviam.tech.observability.api.memory.IStatisticAttribute;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xerial.snappy.SnappyFramedInputStream;

/**
 * {@link JsonDeserializer} for {@link IMemoryStatistic}.
 *
 * @author ActiveViam
 */
public class RetroCompatibleDeserializer extends AStatisticDeserializer<AMemoryStatistic> {

  /** Mapping from 6.1 to 6.0 classes */
  private static final Map<String, String> CLASS_MAPPING_6_0_to_6_1 =
      Map.of(
          "com.qfs.monitoring.statistic.memory.impl.ChunkStatistic",
              "com.activeviam.tech.observability.internal.memory.ChunkStatistic",
          "com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic",
              "com.activeviam.tech.observability.internal.memory.ChunkSetStatistic",
          "com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic",
              "com.activeviam.tech.observability.internal.memory.DefaultMemoryStatistic",
          "com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic",
              "com.activeviam.tech.observability.internal.memory.DictionaryStatistic",
          "com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic",
              "com.activeviam.tech.observability.internal.memory.ReferenceStatistic",
          "com.qfs.monitoring.statistic.memory.impl.IndexStatistic",
              "com.activeviam.tech.observability.internal.memory.IndexStatistic");

  private static final Set<String> AVAILABLE_CLASSES_6_1 =
      Set.of(
          "com.activeviam.tech.observability.internal.memory.ChunkStatistic",
          "com.activeviam.tech.observability.internal.memory.ChunkSetStatistic",
          "com.activeviam.tech.observability.internal.memory.DefaultMemoryStatistic",
          "com.activeviam.tech.observability.internal.memory.DictionaryStatistic",
          "com.activeviam.tech.observability.internal.memory.ReferenceStatistic",
          "com.activeviam.tech.observability.internal.memory.IndexStatistic");

  private static final Logger LOGGER = Logger.getLogger("mac.statistic.memory.deserializer");
  private static ObjectMapper serializer;

  static {
    serializer =
        new ObjectMapper()
            .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, false)
            .configure(JsonReadFeature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER.mappedFeature(), true)
            .configure(JsonReadFeature.ALLOW_UNQUOTED_FIELD_NAMES.mappedFeature(), true)
            .configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
            .configure(JsonReadFeature.ALLOW_NON_NUMERIC_NUMBERS.mappedFeature(), true);
    serializer.setSerializationInclusion(JsonInclude.Include.NON_NULL);

    final SimpleModule deserializeModule = new SimpleModule();
    deserializeModule.addDeserializer(AMemoryStatistic.class, new RetroCompatibleDeserializer());
    serializer.registerModule(deserializeModule);
  }

  @Override
  public AMemoryStatistic deserialize(final JsonParser parser, final DeserializationContext ctx)
      throws IOException {
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
      attributes = parseAttributes(parser, ctx);
    } else {
      attributes = Collections.emptyMap();
    }

    parser.nextToken();
    final List<AMemoryStatistic> children;
    if (Objects.equals(readFieldName(parser), MemoryStatisticAdapter.CHILDREN_ATTR)) {
      parser.nextToken();
      children = parseChildren(parser, ctx, AMemoryStatistic.class);
    } else {
      children = Collections.emptyList();
    }

    handleExcessiveAttributes(parser);

    if (!Objects.equals(parser.currentToken(), JsonToken.END_OBJECT)) {
      throw new IllegalStateException(
          "Unexpected additional tokens. First is " + parser.currentToken());
    }

    return createDeserialized(klassName, name, onHeap, offHeap, attributes, children);
  }

  /**
   * Creates the actual statistic using the parsed attributed.
   *
   * @param klassName name of the statistic class
   * @param name name of the statistic
   * @param onHeap value read for the consumed on-heap memory - negative if unset
   * @param offHeap value read for the consume off-heap memory - negative if unset
   * @param attributes statistic attributes
   * @param children child statistics
   * @return the created deserialized statistic
   */
  protected AMemoryStatistic createDeserialized(
      final String klassName,
      final String name,
      final long onHeap,
      final long offHeap,
      final Map<String, IStatisticAttribute> attributes,
      final List<AMemoryStatistic> children) {
    Class<?> klass;
    if (AVAILABLE_CLASSES_6_1.contains(klassName)) {
      klass = getClass(klassName);
    } else if (classNeedsTranslation(klassName)) {
      klass = getClass(getClassTranslation(klassName));
    } else {
      throw new InternalServiceException("Cannot find statistic class " + klassName);
    }

    final AMemoryStatistic objDeserialized;
    try {
      objDeserialized = (AMemoryStatistic) klass.getDeclaredConstructor().newInstance();
    } catch (final InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new InternalServiceException("Cannot create instance of " + klassName, e);
    }

    try {
      klass.getMethod("setAttributes", Map.class).invoke(objDeserialized, attributes);
    } catch (final IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new InternalServiceException("Cannot set attributes for class " + klassName, e);
    }
    try {
      klass.getMethod("setChildren", Collection.class).invoke(objDeserialized, children);
    } catch (final IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new InternalServiceException("Cannot set children for class " + klassName, e);
    }

    if (name != null) {
      objDeserialized.setName(name);
    }
    if (onHeap >= 0) {
      objDeserialized.setShallowOnHeap(onHeap);
    }
    if (offHeap >= 0) {
      objDeserialized.setShallowOffHeap(offHeap);
    }

    return objDeserialized;
  }

  public static AMemoryStatistic readStatisticFile(final Path file) {
    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine("Reading statistics from " + file.toAbsolutePath());
    }
    final AMemoryStatistic read = readStatistic(file.toFile());
    if (LOGGER.isLoggable(Level.FINE)) {
      LOGGER.fine("Statistics read from " + file.toAbsolutePath());
    }
    return read;
  }

  public static AMemoryStatistic readStatistic(final File file) {

    try {
      try (final InputStream inputStream = inputStream(file);
          final InputStreamReader statistic =
              new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
        final ObjectReader reader = serializer.readerFor(AMemoryStatistic.class);
        return readStatisticObject(reader, statistic);
      }
    } catch (IOException e) {
      throw new ActiveViamRuntimeException(e);
    }
  }

  private static AMemoryStatistic readStatisticObject(
      ObjectReader reader, InputStreamReader statistic) {
    try {
      return reader.readValue(new BufferedReader(statistic));
    } catch (final IOException e) {
      throw new ActiveViamRuntimeException(e);
    }
  }

  private static InputStream inputStream(final File file) throws IOException {
    final boolean isCompressedFile = file.getName().endsWith("." + COMPRESSED_FILE_EXTENSION);

    InputStream inputStream =
        new FileInputStream(file); // NOSONAR: used in try-with-resources above
    if (isCompressedFile) {
      inputStream = new SnappyFramedInputStream(inputStream);
    }
    return inputStream;
  }

  private static String getClassTranslation(final String klassName) {
    return CLASS_MAPPING_6_0_to_6_1.get(klassName);
  }

  private static boolean classNeedsTranslation(final String klassName) {
    return CLASS_MAPPING_6_0_to_6_1.containsKey(klassName);
  }

  private static Class<?> getClass(final String klassName) {
    Class<?> klass;
    try {
      klass = Class.forName(klassName);
    } catch (final ClassNotFoundException notFound) {
      throw new InternalServiceException("Cannot get class " + klassName, notFound);
    }
    return klass;
  }
}
