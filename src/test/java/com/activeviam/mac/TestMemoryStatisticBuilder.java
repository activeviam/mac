/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac;

import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.AMemoryStatisticBuilder;
import com.activeviam.tech.observability.internal.memory.ChunkSetStatistic;
import com.activeviam.tech.observability.internal.memory.ChunkStatistic;
import com.activeviam.tech.observability.internal.memory.DefaultMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.DictionaryStatistic;
import com.activeviam.tech.observability.internal.memory.IndexStatistic;
import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;
import com.activeviam.tech.observability.internal.memory.ReferenceStatistic;

/**
 * @author ActiveViam
 */
public class TestMemoryStatisticBuilder extends AMemoryStatisticBuilder<AMemoryStatistic> {

  /** Name of the statistic */
  protected String name;

  /** Offheap mem. footprint, in bytes */
  protected long offHeap;

  /** Onheap mem. footprint, in bytes */
  protected long onHeap;

  /** Default constructor. */
  public TestMemoryStatisticBuilder(final String name) {
    this.name = name;
    this.offHeap = 0;
    this.onHeap = 0;
  }

  @Override
  protected AMemoryStatistic createStatistic() {
    AMemoryStatistic statistic;
    switch (this.name) {
      case MemoryStatisticConstants.STAT_NAME_CHUNK:
        statistic = new ChunkStatistic();
        break;
      case MemoryStatisticConstants.STAT_NAME_CHUNKSET:
        statistic = new ChunkSetStatistic();
        break;
      case MemoryStatisticConstants.STAT_NAME_DICTIONARY:
        statistic = new DictionaryStatistic();
        break;
      case MemoryStatisticConstants.STAT_NAME_INDEX:
        statistic = new IndexStatistic();
        break;
      case MemoryStatisticConstants.STAT_NAME_REFERENCE:
        statistic = new ReferenceStatistic();
        break;
      default:
        statistic = new DefaultMemoryStatistic();
    }
    statistic.setParent(null);
    return statistic;
  }
}
