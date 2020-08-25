/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory;

import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;

public class StatisticCollector implements IMemoryStatisticVisitor<Object> {

  @Override
  public Object visit(DefaultMemoryStatistic defaultMemoryStatistic) {

    return null;
  }

  @Override
  public Object visit(ChunkSetStatistic chunkSetStatistic) {
    return null;
  }

  @Override
  public Object visit(ChunkStatistic chunkStatistic) {
    return null;
  }

  @Override
  public Object visit(ReferenceStatistic referenceStatistic) {
    return null;
  }

  @Override
  public Object visit(IndexStatistic indexStatistic) {
    return null;
  }

  @Override
  public Object visit(DictionaryStatistic dictionaryStatistic) {
    return null;
  }
}
