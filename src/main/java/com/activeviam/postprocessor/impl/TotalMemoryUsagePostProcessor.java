/*
 * (C) Quartet FS 2007-2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of Quartet Financial Systems Limited. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.postprocessor.impl;

import java.util.Properties;

import com.quartetfs.biz.pivot.ILocation;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IPostProcessorCreationContext;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessor;
import com.quartetfs.biz.pivot.postprocessing.impl.ABasicPostProcessor;
import com.quartetfs.fwk.QuartetException;
import com.quartetfs.fwk.QuartetExtendedPluginValue;

/**
 * @author ActiveViam
 *
 */
@QuartetExtendedPluginValue(intf = IPostProcessor.class, key = TotalMemoryUsagePostProcessor.PLUGIN_KEY)
public class TotalMemoryUsagePostProcessor extends ABasicPostProcessor<Long> {

	private static final long serialVersionUID = 1L;

	/** post processor plugin key */
	public final static String PLUGIN_KEY = "TOTAL_MEMORY_USAGE";

	/**
	 * Constructor of {@link TotalMemoryUsagePostProcessor}.
	 *
	 * @param name
	 * @param creationContext
	 */
	public TotalMemoryUsagePostProcessor(String name, IPostProcessorCreationContext creationContext) {
		super(name, creationContext);
	}

	@Override
	public void init(Properties properties) throws QuartetException {
		properties.put(UNDERLYING_MEASURES, "DirectMemoryChunksUsage.SUM,HeapMemoryChunksUsage.SUM");
		super.init(properties);
	}

	@Override
	public String getType() {
		return PLUGIN_KEY;
	}

	@Override
	public Long evaluate(ILocation location, Object[] underlyingMeasures) {
		final Long v1 = (Long) underlyingMeasures[0];
		final Long v2 = (Long) underlyingMeasures[1];
		return (v1 == null ? 0 : v1) + (v2 == null ? 0 : v2);
	}

}
