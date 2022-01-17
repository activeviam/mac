/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.impl.IntegerStatisticAttribute;
import com.qfs.monitoring.statistic.impl.LongStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Debug implementation of {@link IMemoryStatisticVisitor}.
 *
 * @author ActiveViam
 */
public class DebugVisitor implements IMemoryStatisticVisitor<Void> {

  /** Boolean stating if the debug mode is activated or not. */
  public static final Boolean DEBUG = false;
  /** key string for the debug-id attribute. */
  protected static final String ID_KEY = "debug-id";

  private static final String DEPTH_KEY = "debug-depth";
  /** Debug Id of the current {@link IMemoryStatistic}. */
  protected final AtomicLong id = new AtomicLong(0);
  /** Depth of the current memory statistic in the tree. */
  protected int depth = 0;

  /**
   * Created a {@link StatisticTreePrinter printer} of the tree created from the input {@link
   * IMemoryStatistic}.
   *
   * @param root Memory statistic being the root of the printed tree
   * @return the generated TreePrinter
   */
  public static StatisticTreePrinter createDebugPrinter(IMemoryStatistic root) {
    root.accept(new DebugVisitor());
    return new StatisticTreePrinter();
  }

  @Override
  public Void visit(final IMemoryStatistic memoryStatistic) {
    enrichStatisticWithDebugAttributes(memoryStatistic);
    return null;
  }

  /**
   * Enriches a statistic and its children with debug attributes.
   *
   * @param parent root parent of the {@link IMemoryStatistic} to be enriched
   */
  protected void enrichStatisticWithDebugAttributes(IMemoryStatistic parent) {
    addDebugAttributes(parent);
    if (parent.getChildren() == null) {
      return;
    }

    this.depth++;
    for (IMemoryStatistic child : parent.getChildren()) {
      addDebugAttributes(child);
      child.accept(this);
    }
    this.depth--;
  }

  /**
   * Adds debug attributes to a {@link IMemoryStatistic}.
   *
   * @param statistic to be enriched
   */
  protected void addDebugAttributes(IMemoryStatistic statistic) {
    IntegerStatisticAttribute depthValue = new IntegerStatisticAttribute(this.depth);
    IStatisticAttribute old = statistic.getAttributes().put(DEPTH_KEY, depthValue);
    if (old != null && !old.equals(depthValue)) {
      throw new RuntimeException(
          DEPTH_KEY
              + " an depthValue already exists and are not equal: old="
              + old
              + "; new="
              + depthValue);
    }
    statistic
        .getAttributes()
        .computeIfAbsent(ID_KEY, k -> new LongStatisticAttribute(this.id.getAndIncrement()));
  }

  @Override
  public Void visit(DefaultMemoryStatistic memoryStatistic) {
    enrichStatisticWithDebugAttributes(memoryStatistic);
    return null;
  }

  @Override
  public Void visit(ChunkSetStatistic chunkSetStatistic) {
    enrichStatisticWithDebugAttributes(chunkSetStatistic);
    return null;
  }

  @Override
  public Void visit(ChunkStatistic chunkStatistic) {
    enrichStatisticWithDebugAttributes(chunkStatistic);
    return null;
  }

  @Override
  public Void visit(ReferenceStatistic referenceStatistic) {
    enrichStatisticWithDebugAttributes(referenceStatistic);
    return null;
  }

  @Override
  public Void visit(IndexStatistic indexStatistic) {
    enrichStatisticWithDebugAttributes(indexStatistic);
    return null;
  }

  @Override
  public Void visit(DictionaryStatistic dictionaryStatistic) {
    enrichStatisticWithDebugAttributes(dictionaryStatistic);
    return null;
  }
}
