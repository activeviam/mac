/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.postprocessor.impl;

import com.quartetfs.biz.pivot.ILocation;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IPostProcessorCreationContext;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessor;
import com.quartetfs.biz.pivot.postprocessing.impl.ADynamicAggregationPostProcessor;
import com.quartetfs.fwk.QuartetExtendedPluginValue;

/**
 * @author Quartet FS
 */
@QuartetExtendedPluginValue(intf = IPostProcessor.class, key = ChunkSetFreeRowsRatePostProcessor.PLUGIN_KEY)
public class ChunkSetFreeRowsRatePostProcessor extends ADynamicAggregationPostProcessor<Double, Double> {

	/** post processor plugin key */
	public final static String PLUGIN_KEY = "FREE_ROWS_RATE";

	public ChunkSetFreeRowsRatePostProcessor(String name, IPostProcessorCreationContext creationContext) {
		super(name, creationContext);
	}

	@Override
	public String getType() {
		return PLUGIN_KEY;
	}

	@Override
	protected Double evaluateLeaf(ILocation leafLocation, Object[] underlyingMeasures) {
		if ( /* Unlikely */underlyingMeasures[1] == null) {
			return null;
		}

		int physicalSize = (int) underlyingMeasures[1];
		if ( /* Unlikely */physicalSize == 0) {
			return null;
		}

		return ((double) (int) underlyingMeasures[0] / physicalSize);
	}

}
