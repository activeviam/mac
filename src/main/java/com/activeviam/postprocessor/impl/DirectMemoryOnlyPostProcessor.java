/*
 * (C) Quartet FS 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.postprocessor.impl;

import java.util.Arrays;
import java.util.Properties;

import com.quartetfs.biz.pivot.ILocation;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IPostProcessorCreationContext;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessor;
import com.quartetfs.biz.pivot.postprocessing.impl.ADynamicAggregationPostProcessor;
import com.quartetfs.fwk.QuartetException;
import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.QuartetRuntimeException;

/**
 * @author Quartet FS
 */
@QuartetExtendedPluginValue(intf = IPostProcessor.class, key = DirectMemoryOnlyPostProcessor.PLUGIN_KEY)
public class DirectMemoryOnlyPostProcessor extends ADynamicAggregationPostProcessor<Long, Long> {

	/** post processor plugin key */
	public final static String PLUGIN_KEY = "DIRECT_MEMORY_ONLY";

	protected final static String MEASURE_KEY = "evaluatedMeasure";

	protected int index;

	public DirectMemoryOnlyPostProcessor(String name, IPostProcessorCreationContext creationContext) {
		super(name, creationContext);
	}

	@Override
	public void init(Properties properties) throws QuartetException {
		super.init(properties);

		String underlyingMeasureName = properties.getProperty(MEASURE_KEY);
		if (underlyingMeasureName == null) {
			throw new QuartetRuntimeException("Please set a value for the propery key '" + MEASURE_KEY + "'");
		}

		if (underlyingMeasureName.equals(underlyingMeasures[0])) {
			index = 0;
			return; // same measure
		}

		this.prefetchers.add(new DynamicAggregationPostProcessorPrefetcher(
				getActivePivot(),
				this,
				getMeasuresProvider(),
				new String[] { underlyingMeasureName }));

		this.underlyingMeasures = Arrays.copyOf(underlyingMeasures, underlyingMeasures.length + 1);
		this.index = underlyingMeasures.length - 1;
		this.underlyingMeasures[index] = underlyingMeasureName;
	}

	@Override
	public String getType() {
		return PLUGIN_KEY;
	}

	@Override
	protected Long evaluateLeaf(ILocation leafLocation, Object[] underlyingMeasures) {
		long offheap = (long) underlyingMeasures[index];
		if (offheap == 0)
			return null;

		return (Long) underlyingMeasures[0];
	}

}
