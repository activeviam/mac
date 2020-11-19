/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.statistic.memory.visitor.impl;

import com.activeviam.mac.entities.ChunkOwner;
import com.activeviam.mac.entities.CubeOwner;
import com.activeviam.mac.entities.NoOwner;
import com.activeviam.mac.entities.StoreOwner;
import com.qfs.distribution.IMultiVersionDistributedActivePivot;
import com.qfs.monitoring.statistic.IStatisticAttribute;
import com.qfs.monitoring.statistic.memory.IMemoryStatistic;
import com.qfs.monitoring.statistic.memory.MemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.PivotMemoryStatisticConstants;
import com.qfs.monitoring.statistic.memory.impl.ChunkSetStatistic;
import com.qfs.monitoring.statistic.memory.impl.ChunkStatistic;
import com.qfs.monitoring.statistic.memory.impl.DefaultMemoryStatistic;
import com.qfs.monitoring.statistic.memory.impl.DictionaryStatistic;
import com.qfs.monitoring.statistic.memory.impl.IndexStatistic;
import com.qfs.monitoring.statistic.memory.impl.ReferenceStatistic;
import com.qfs.monitoring.statistic.memory.visitor.IMemoryStatisticVisitor;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class EpochVisitor implements IMemoryStatisticVisitor<Void> {

  protected TLongSet datastoreEpochs;

  protected Map<ChunkOwner, SortedSet<Long>> regularEpochsPerOwner;
  protected Map<ChunkOwner, Set<Long>> distributedEpochsPerOwner;
  protected Set<ChunkOwner> distributedCubes;

  protected ChunkOwner currentOwner;
  protected OptionalLong currentlyVisitedStatEpoch;

  public EpochVisitor() {
    this.datastoreEpochs = new TLongHashSet();
    this.regularEpochsPerOwner = new HashMap<>();
    this.distributedEpochsPerOwner = new HashMap<>();
    this.currentOwner = NoOwner.getInstance();
    this.distributedCubes = new HashSet<>();
  }

  public TLongSet getDatastoreEpochs() {
    return datastoreEpochs;
  }

  public Map<ChunkOwner, SortedSet<Long>> getRegularEpochsPerOwner() {
    return regularEpochsPerOwner;
  }

  public Map<ChunkOwner, Set<Long>> getDistributedEpochsPerOwner() {
    return distributedEpochsPerOwner;
  }

  public Set<ChunkOwner> getDistributedCubes() {
    return distributedCubes;
  }

  @Override
  public Void visit(DefaultMemoryStatistic memoryStatistic) {

    switch (memoryStatistic.getName()) {

      case MemoryStatisticConstants.STAT_NAME_MULTIVERSION_STORE:
      case PivotMemoryStatisticConstants.STAT_NAME_MULTIVERSION_PIVOT:
        visitChildren(memoryStatistic);
        this.currentlyVisitedStatEpoch = OptionalLong.empty();
        break;

      case MemoryStatisticConstants.STAT_NAME_STORE:
        currentlyVisitedStatEpoch = readEpoch(memoryStatistic);
        currentOwner = new StoreOwner(
            memoryStatistic.getAttribute(
                MemoryStatisticConstants.ATTR_NAME_STORE_NAME)
                .asText());

        if (currentlyVisitedStatEpoch.isPresent()) {
          datastoreEpochs.add(currentlyVisitedStatEpoch.getAsLong());
        }
        break;

      case PivotMemoryStatisticConstants.STAT_NAME_PIVOT:
        currentlyVisitedStatEpoch = readEpoch(memoryStatistic);
        currentOwner = new CubeOwner(
            memoryStatistic.getAttribute(
                PivotMemoryStatisticConstants.ATTR_NAME_PIVOT_ID)
                .asText());

        visitChildren(memoryStatistic); // to check if the cube is distributed
        break;

      case PivotMemoryStatisticConstants.STAT_NAME_PROVIDER:
        if (isProviderStatFromDistributedCube(memoryStatistic)) {
          if (currentlyVisitedStatEpoch.isPresent()) {
            distributedEpochsPerOwner.computeIfAbsent(
                currentOwner,
                owner -> new HashSet<>())
                .add(currentlyVisitedStatEpoch.getAsLong());
          }
          return null;
        }
        break;

      default:
        visitChildren(memoryStatistic);
        break;
    }

		if (currentlyVisitedStatEpoch.isPresent()) {
      regularEpochsPerOwner.computeIfAbsent(
          currentOwner,
          owner -> new TreeSet<>())
          .add(currentlyVisitedStatEpoch.getAsLong());
    }

		return null;
	}

  protected OptionalLong readEpoch(final IMemoryStatistic memoryStatistic) {
    final IStatisticAttribute epochAttr =
        memoryStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_EPOCH);

    if (epochAttr != null) {
      return OptionalLong.of(epochAttr.asLong());
    }
    return OptionalLong.empty();
  }

  protected boolean isProviderStatFromDistributedCube(final IMemoryStatistic statistic) {
    return IMultiVersionDistributedActivePivot.PLUGIN_KEY.equals(statistic
        .getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_TYPE)
        .asText());
  }

  protected void visitChildren(final IMemoryStatistic statistic) {
    if (statistic.getChildren() != null) {
      for (final IMemoryStatistic child : statistic.getChildren()) {
        child.accept(this);
      }
    }
  }

  @Override
  public Void visit(ChunkSetStatistic chunkSetStatistic) {
    return null;
  }

  @Override
  public Void visit(ChunkStatistic chunkStatistic) {
    return null;
  }

  @Override
  public Void visit(ReferenceStatistic referenceStatistic) {
    return null;
  }

  @Override
  public Void visit(IndexStatistic indexStatistic) {
    return null;
  }

  @Override
  public Void visit(DictionaryStatistic dictionaryStatistic) {
    return null;
  }
}
