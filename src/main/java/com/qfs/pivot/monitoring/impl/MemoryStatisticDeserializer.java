/*
 * (C) ActiveViam 2015
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.qfs.pivot.monitoring.impl;

import com.fasterxml.jackson.databind.JsonDeserializer;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import java.util.List;
import java.util.Map;

/**
 * {@link JsonDeserializer} for {@link IMemoryStatistic}.
 *
 * @author ActiveViam
 */
public class MemoryStatisticDeserializer extends MonitoringStatisticDeserializer<IMemoryStatistic> {

	/**
	 * Default constructor.
	 */
	public MemoryStatisticDeserializer() {
		super(IMemoryStatistic.class);
	}

	@Override
	protected IMemoryStatistic createDeserialized(
			final String klassName,
			final String name,
			final long onHeap,
			final long offHeap,
			final Map<String, IStatisticAttribute> attributes,
			final List<IMemoryStatistic> children) {
		final IMemoryStatistic deserialized = super.createDeserialized(
				klassName,
				name,
				onHeap,
				offHeap,
				attributes,
				children);
		if (name != null) {
			deserialized.setName(name);
		}
		if (onHeap >= 0) {
			deserialized.setShallowOnHeap(onHeap);
		}
		if (offHeap >= 0) {
			deserialized.setShallowOffHeap(offHeap);
		}

		return deserialized;
	}

}
