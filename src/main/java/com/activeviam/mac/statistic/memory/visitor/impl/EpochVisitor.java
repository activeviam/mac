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
import com.activeviam.mac.memory.AnalysisDatastoreFeeder;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A memory statistic visitor that collects present epochs.
 *
 * @see AnalysisDatastoreFeeder
 */
public class EpochVisitor implements IMemoryStatisticVisitor<Void> {

  /** The epochs collected from store statistics. */
  protected Set<Long> datastoreEpochs;

  /** The epochs collected from statistics that do not come from distributed cubes. */
  protected Map<ChunkOwner, SortedSet<Long>> regularEpochsPerOwner;

  /** The epochs collected from statistics that come from distributed cubes. */
  protected Map<ChunkOwner, Set<Long>> distributedEpochsPerOwner;

  /** The owner of the currently visited statistic. */
  protected ChunkOwner currentOwner;

  /** The epoch of the currently visited statistic. */
  protected OptionalLong currentlyVisitedStatEpoch;

  /** Constructor. */
  public EpochVisitor() {
    this.datastoreEpochs = new HashSet<>();
    this.regularEpochsPerOwner = new HashMap<>();
    this.distributedEpochsPerOwner = new HashMap<>();
    this.currentOwner = NoOwner.getInstance();
  }

  /**
   * Gets the collected datastore epochs.
   *
   * <p>Should only be called after the visit is done.
   *
   * @return the datastore epochs
   */
  public Set<Long> getDatastoreEpochs() {
    return datastoreEpochs;
  }

  /**
   * Gets the collected epochs collected from statistics that do not come from distributed cubes.
   *
   * <p>Should only be called after the visit is done.
   *
   * @return the regular epochs
   */
  public Map<ChunkOwner, SortedSet<Long>> getRegularEpochsPerOwner() {
    return regularEpochsPerOwner;
  }

  /**
   * Gets the collected epochs collected from statistics that come from distributed cubes.
   *
   * <p>Should only be called after the visit is done.
   *
   * @return the distributed epochs
   */
  public Map<ChunkOwner, Set<Long>> getDistributedEpochsPerOwner() {
    return distributedEpochsPerOwner;
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
        currentOwner =
            new StoreOwner(
                memoryStatistic
                    .getAttribute(MemoryStatisticConstants.ATTR_NAME_STORE_NAME)
                    .asText());

        if (currentlyVisitedStatEpoch.isPresent()) {
          datastoreEpochs.add(currentlyVisitedStatEpoch.getAsLong());
          mapRegularEpoch(currentOwner, currentlyVisitedStatEpoch.getAsLong());
        }
        break;

      case PivotMemoryStatisticConstants.STAT_NAME_PIVOT:
        currentlyVisitedStatEpoch = readEpoch(memoryStatistic);
        currentOwner =
            new CubeOwner(
                memoryStatistic
                    .getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PIVOT_ID)
                    .asText());

        visitChildren(memoryStatistic); // to check if the cube is distributed
        break;

      case PivotMemoryStatisticConstants.STAT_NAME_PROVIDER:
        if (currentlyVisitedStatEpoch.isPresent()) {
          if (isProviderStatFromDistributedCube(memoryStatistic)) {
            mapDistributedEpoch(currentOwner, currentlyVisitedStatEpoch.getAsLong());
          } else {
            mapRegularEpoch(currentOwner, currentlyVisitedStatEpoch.getAsLong());
          }
        }
        return null;

      default:
        visitChildren(memoryStatistic);
        break;
    }

    return null;
  }

  /**
   * Attempts to read an epoch id from the attributes of the given statistic.
   *
   * <p>Child statistics are not considered and should be visited if needed.
   *
   * @param memoryStatistic the memory statistic to read the epoch from
   * @return optionally, the epoch of the statistic
   */
  protected OptionalLong readEpoch(final IMemoryStatistic memoryStatistic) {
    final IStatisticAttribute epochAttr =
        memoryStatistic.getAttribute(MemoryStatisticConstants.ATTR_NAME_EPOCH);

    if (epochAttr != null) {
      return OptionalLong.of(epochAttr.asLong());
    }
    return OptionalLong.empty();
  }

  /**
   * Returns whether or not the given aggregate provider statistic indicates that the cube it is
   * tied to is distributed.
   *
   * @param statistic the aggregate provider statistic to consider
   * @return true if it come from a distributed cube, false otherwise
   */
  protected boolean isProviderStatFromDistributedCube(final IMemoryStatistic statistic) {
    return IMultiVersionDistributedActivePivot.PLUGIN_KEY.equals(
        statistic.getAttribute(PivotMemoryStatisticConstants.ATTR_NAME_PROVIDER_TYPE).asText());
  }

  /**
   * Maps an epoch to the given owner, as a non-distributed epoch.
   *
   * @param owner the owner for the epoch
   * @param epoch the epoch to map
   */
  protected void mapRegularEpoch(final ChunkOwner owner, final long epoch) {
    regularEpochsPerOwner.computeIfAbsent(owner, key -> new TreeSet<>()).add(epoch);
  }

  /**
   * Maps an epoch to the given owner, as a distributed epoch.
   *
   * @param owner the owner for the epoch
   * @param epoch the epoch to map
   */
  protected void mapDistributedEpoch(final ChunkOwner owner, final long epoch) {
    distributedEpochsPerOwner.computeIfAbsent(owner, key -> new HashSet<>()).add(epoch);
  }

  /**
   * Visits the children of the given statistic.
   *
   * <p>This method is not recursive and only goes down one level.
   *
   * @param statistic the statistic whose children need to be visited
   */
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
