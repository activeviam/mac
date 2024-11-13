/*
 * (C) ActiveViam 2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac;

import com.activeviam.tech.core.api.exceptions.ActiveViamRuntimeException;
import com.activeviam.tech.observability.api.memory.IMemoryStatistic;
import com.activeviam.tech.observability.api.memory.IStatisticAttribute;
import com.activeviam.tech.observability.internal.memory.AMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.AMemoryStatisticBuilder;
import com.activeviam.tech.observability.internal.memory.ChunkSetStatistic;
import com.activeviam.tech.observability.internal.memory.ChunkStatistic;
import com.activeviam.tech.observability.internal.memory.DefaultMemoryStatistic;
import com.activeviam.tech.observability.internal.memory.DictionaryStatistic;
import com.activeviam.tech.observability.internal.memory.IMemoryStatisticBuilder;
import com.activeviam.tech.observability.internal.memory.IndexStatistic;
import com.activeviam.tech.observability.internal.memory.MemoryStatisticConstants;
import com.activeviam.tech.observability.internal.memory.ReferenceStatistic;
import java.util.Arrays;
import java.util.stream.Collectors;

/** @author ActiveViam */
public class TestMemoryStatisticBuilder extends AMemoryStatisticBuilder<AMemoryStatistic> {

  /** Name of the statistic */
  protected String name;

  /** Offheap mem. footprint, in bytes */
  protected long offHeap;

  /** Onheap mem. footprint, in bytes */
  protected long onHeap;

  /** Default constructor. */
  public TestMemoryStatisticBuilder() {
    this.offHeap = 0;
    this.onHeap = 0;
  }

  @Override
  public IMemoryStatistic build() {
    IStatisticAttribute parentClass =
        this.attributes.get(MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS);
    if (parentClass == null) {
      throw new ActiveViamRuntimeException(
          "It is mandatory to add a parent class to statistic with name '" + this.name + "'");
    }

    if (this.name == null) {
      // Default name
      this.name = parentClass.asText();
    }

    IMemoryStatistic statistic;
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
    statistic.setShallowOffHeap(this.offHeap);
    statistic.setShallowOnHeap(this.onHeap);
    statistic.setName(this.name);
    statistic.setAttributes(this.attributes);
    IMemoryStatistic delegateParent = null;
    if (this.children != null && !this.children.isEmpty()) {
      // The children should have a distinct name, see setChildren method.
      statistic.setChildren(this.children);
      for (final IMemoryStatistic child : this.children) {
        if (child.getParent() != null && child.getParent() != statistic) {
          if (delegateParent == null) {
            delegateParent = child.getParent();
          } else {
            if (delegateParent != child.getParent()) {
              throw new IllegalStateException("Inconsistency in statistics parenthood...Bad.");
            }
          }

        } else {
          child.setParent(statistic);
        }
      }
    }
    if (delegateParent == null) return statistic;
    else return delegateParent;
  }

  @Override
  protected IMemoryStatistic createStatistic() {
    return null;
  }

  @Override
  public IMemoryStatisticBuilder withName(String name) {
    this.name = name;
    return this;
  }

  @Override
  public IMemoryStatisticBuilder withCreatorClasses(Class<?>... parentClass) {
    withAttribute(
        MemoryStatisticConstants.ATTR_NAME_CREATOR_CLASS,
        Arrays.stream(parentClass).map(Class::getName).collect(Collectors.joining(",")));
    return this;
  }

  @Override
  public IMemoryStatisticBuilder withMemoryFootPrint(long offHeap, long onHeap) {
    this.onHeap = onHeap;
    this.offHeap = offHeap;
    return this;
  }
}
