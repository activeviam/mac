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
import com.quartetfs.fwk.QuartetException;
import com.quartetfs.fwk.QuartetExtendedPluginValue;
import com.quartetfs.fwk.QuartetRuntimeException;
import java.util.Arrays;
import java.util.Properties;

/** @author Quartet FS */
@QuartetExtendedPluginValue(
    intf = IPostProcessor.class,
    key = DirectMemoryOnlyPostProcessor.PLUGIN_KEY)
public class DirectMemoryOnlyPostProcessor extends ADynamicAggregationPostProcessor<Long, Long> {

  /** Serialization ID */
  private static final long serialVersionUID = 5_8_2L;

  /** post processor plugin key */
  public static final String PLUGIN_KEY = "DIRECT_MEMORY_ONLY";

  /** Key of the measure */
  public static final String MEASURE_KEY = "evaluatedMeasure";

  private int index;

  /**
   * Constuctor of the {@link DirectMemoryOnlyPostProcessor}
   *
   * @param name name of the postprocesssor
   * @param creationContext Container for additional parameters
   */
  public DirectMemoryOnlyPostProcessor(String name, IPostProcessorCreationContext creationContext) {
    super(name, creationContext);
  }

  @Override
  public void init(Properties properties) throws QuartetException {
    super.init(properties);

    String underlyingMeasureName = properties.getProperty(MEASURE_KEY);
    if (underlyingMeasureName == null) {
      throw new QuartetRuntimeException(
          "Please set a value for the propery key '" + MEASURE_KEY + "'");
    }

    if (underlyingMeasureName.equals(underlyingMeasures[0])) {
      index = 0;
      return; // same measure
    }

    this.underlyingMeasures =
        Arrays.copyOf(this.underlyingMeasures, this.underlyingMeasures.length + 1);
    this.index = this.underlyingMeasures.length - 1;
    this.underlyingMeasures[index] = underlyingMeasureName;

    this.prefetchers.add(
        new DynamicAggregationPostProcessorPrefetcher(
            getActivePivot(), this, getMeasuresProvider(), this.underlyingMeasures));
  }

  @Override
  public String getType() {
    return PLUGIN_KEY;
  }

  @Override
  protected Long evaluateLeaf(ILocation leafLocation, Object[] underlyingMeasures) {
    long offheap = (long) underlyingMeasures[index];
    if (offheap == 0) return null;

    return (Long) underlyingMeasures[0];
  }
}
