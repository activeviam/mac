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
import com.fasterxml.jackson.databind.DeserializationContext;
import com.qfs.fwk.services.InternalServiceException;
import com.qfs.monitoring.statistic.IMonitoringStatistic;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Deserializer for {@link IMonitoringStatistic}.
 *
 * @author ActiveViam
 */
@Workaround(jira = "PIVOT-4093", solution = "Waiting for the next version with the fix")
public class MonitoringStatisticDeserializer extends AStatisticDeserializer<IMonitoringStatistic> {

	@Override
	public IMonitoringStatistic deserialize(
			final JsonParser parser,
			final DeserializationContext ctx) throws IOException {
		if (!JsonToken.START_OBJECT.equals(parser.currentToken())) {
			throw new IllegalArgumentException("Should be called at the start of an object");
		}
		parser.nextToken();

		readAndCheckFieldName(parser, MemoryStatisticAdapter.STATISTIC_CLASS_ATTR);
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
		final List<IMonitoringStatistic> children;
		if (Objects.equals(readFieldName(parser), MemoryStatisticAdapter.CHILDREN_ATTR)) {
			parser.nextToken();
			children = parseChildren(parser, ctx, IMonitoringStatistic.class);
		} else {
			children = Collections.emptyList();
		}

		handleExcessiveAttributes(parser);

		if (!Objects.equals(parser.currentToken(), JsonToken.END_OBJECT)) {
			throw new IllegalStateException(
					"Unexpected additional tokens. First is " + parser.currentToken());
		}

		return createDeserialized(
				klassName,
				attributes,
				children);
	}

	/**
	 * Creates the actual statistic using the parsed attributed.
	 * @param klassName name of the statistic class
	 * @param attributes statistic attributes
	 * @param children child statistics
	 * @return the created deserialized statistic
	 */
	protected IMonitoringStatistic createDeserialized(
			final String klassName,
			final Map<String, IStatisticAttribute> attributes,
			final List<IMonitoringStatistic> children) {
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

		return objDeserialized;
	}

}
