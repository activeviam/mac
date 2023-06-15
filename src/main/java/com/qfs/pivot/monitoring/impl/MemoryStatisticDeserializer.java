/*
 * (C) ActiveViam 2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.qfs.pivot.monitoring.impl;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.qfs.fwk.services.InternalServiceException;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * {@link JsonDeserializer} for {@link IMemoryStatistic}.
 *
 * @author ActiveViam
 */
public class MemoryStatisticDeserializer extends AStatisticDeserializer<IMemoryStatistic> {

	@Override
	public IMemoryStatistic deserialize(final JsonParser parser, final DeserializationContext ctx)
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
		final List<IMemoryStatistic> children;
		if (Objects.equals(readFieldName(parser), MemoryStatisticAdapter.CHILDREN_ATTR)) {
			parser.nextToken();
			children = parseChildren(parser, ctx, IMemoryStatistic.class);
		} else {
			children = Collections.emptyList();
		}

		handleExcessiveAttributes(parser);

		if (!Objects.equals(parser.currentToken(), JsonToken.END_OBJECT)) {
			throw new IllegalStateException("Unexpected additional tokens. First is " + parser.currentToken());
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
	protected IMemoryStatistic createDeserialized(
			final String klassName,
			final String name,
			final long onHeap,
			final long offHeap,
			final Map<String, IStatisticAttribute> attributes,
			final List<IMemoryStatistic> children) {
		final Class<?> klass;
		try {
			klass = Class.forName(klassName);
		} catch (ClassNotFoundException e) {
			throw new InternalServiceException("Cannot find statistic class " + klassName, e);
		}

		final IMemoryStatistic objDeserialized;
		try {
			objDeserialized = (IMemoryStatistic) klass.getDeclaredConstructor().newInstance();
		} catch (InstantiationException
				 | IllegalAccessException
				 | NoSuchMethodException
				 | InvocationTargetException e) {
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

}
