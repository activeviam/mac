/*
 * (C) ActiveViam 2013-2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.copper.foundry.casting.solid.agg;

import com.activeviam.copper.LevelCoordinate;
import com.activeviam.mac.Workaround;
import com.qfs.condition.ICondition;
import com.qfs.condition.IConstantCondition;
import com.qfs.condition.impl.BaseConditions;
import com.qfs.pivot.cube.provider.jit.impl.ToDatastoreConditionFilterCompiler;
import com.qfs.store.query.plan.condition.impl.ConditionParser.AndConditionMerger;
import com.quartetfs.biz.pivot.IActivePivot;
import com.quartetfs.biz.pivot.IActivePivotVersion;
import com.quartetfs.biz.pivot.ILocation;
import com.quartetfs.biz.pivot.IPointLocationBuilder;
import com.quartetfs.biz.pivot.IPointLocationReader;
import com.quartetfs.biz.pivot.cellset.IAggregatesLocationResult;
import com.quartetfs.biz.pivot.context.subcube.ICubeFilter;
import com.quartetfs.biz.pivot.context.subcube.impl.CubeFilter;
import com.quartetfs.biz.pivot.context.subcube.visitor.impl.FilterVisitHelper;
import com.quartetfs.biz.pivot.cube.hierarchy.IHierarchy;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevel;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevelInfo.ClassificationType;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.DatastorePrefetchRequest;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.IAggregationHierarchyCreationContext;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.IAnalysisAggregationProcedure;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.IJoinMeasure;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.ILocationBuilder;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.impl.AAnalysisAggregationProcedure;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.impl.AnalysigAggregationProcedureFactory;
import com.quartetfs.biz.pivot.cube.hierarchy.axis.impl.JoinContext;
import com.quartetfs.biz.pivot.cube.hierarchy.impl.HierarchiesUtil;
import com.quartetfs.biz.pivot.cube.hierarchy.measures.IMeasureMember;
import com.quartetfs.biz.pivot.cube.provider.IHierarchicalMapping;
import com.quartetfs.biz.pivot.definitions.IAnalysisAggregationProcedureDescription;
import com.quartetfs.biz.pivot.impl.LocationUtil;
import com.quartetfs.biz.pivot.impl.LocationUtil.HasAnalysisLevelCoordinatesResult;
import com.quartetfs.biz.pivot.query.aggregates.IDatastoreRetrievalResult;
import com.quartetfs.biz.pivot.query.aggregates.IScopeLocation;
import com.quartetfs.fwk.QuartetPluginValue;
import com.quartetfs.fwk.QuartetRuntimeException;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.map.TIntObjectMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The procedure for joining an isolated store to the cube's selection. The join may create analysis
 * hierarchies. They will then be handled by this {@link IAnalysisAggregationProcedure procedure}.
 *
 * @author ActiveViam
 */
@Workaround(solution = "Fix the creation of the field mapping, as done in CoPPer II")
public class JoinAnalysisAggregationProcedure extends AAnalysisAggregationProcedure<JoinContext> {

  /** Plugin type */
  public static final String PLUGIN_KEY = "Copper.JoinProcedure";

  /**
   * The name of the property that contains the mapping between the cube's selection and the
   * isolated store
   */
  public static final String MAPPING = "mapping";

  /** The name of the property that contains the name of isolated store to query */
  public static final String STORE = "store";

  /** The name of the isolated store to query */
  protected final String store;

  /**
   * The mapping between the cube's selection via the cube levels (keys) and the isolated store via
   * store field names (values).
   */
  protected final Map<LevelCoordinate, String> mapping;

  /** Hierarchy and level ordinals (0 based) indexed by the {@link LevelCoordinate} */
  protected final Map<LevelCoordinate, int[]> mappingIndices;

  /**
   * Function that takes a cube filter as argument and that creates a {@link ICondition} from it.
   * The condition will be used to create the appropriate {@link DatastorePrefetchRequest}.
   */
  protected final Function<ICubeFilter, ICondition> filterCompiler;

  /** The name of the fields on which the join can be made. See {@link JoinContext} */
  protected final String[] fields;

  /**
   * Mapping from fields to corresponding level index in the hierarchical mapping. This is aligned
   * with the the argument `fields`. See {@link JoinContext}.
   */
  protected final int[] fieldMappingIdxs;

  /**
   * Full constructor.
   *
   * @param description description of this procedure
   * @param hierarchies hierarchies handled by this procedure
   * @param context creation context of this procedure
   */
  @SuppressWarnings("unchecked")
  public JoinAnalysisAggregationProcedure(
      final IAnalysisAggregationProcedureDescription description,
      final Collection<? extends IHierarchy> hierarchies,
      final IAggregationHierarchyCreationContext context) {
    super(description, hierarchies, context);
    final IActivePivot activePivot = context.getActivePivot();

    this.mapping = (Map<LevelCoordinate, String>) description.getProperties().get(MAPPING);
    this.store = description.getProperties().getProperty(STORE);

    this.mappingIndices = new HashMap<>();
    for (LevelCoordinate coord : this.mapping.keySet()) {
      final ILevel l =
          HierarchiesUtil.getLevel(
              context.getActivePivot(),
              coord.parent.dimension,
              coord.parent.hierarchy,
              coord.level);
      this.mappingIndices.put(
          coord, new int[] {l.getHierarchyInfo().getOrdinal() - 1, l.getOrdinal()});
    }

    IHierarchicalMapping hierarchicalMapping =
        activePivot.getAggregateProvider().getHierarchicalMapping();
    if (!this.hierarchies.isEmpty()) {
      final Map<String, Map<String, Integer>> restrictedLevels = new HashMap<>();
      final List<String> fieldExpressionPerCoordinateList = new ArrayList<>();
      this.hierarchies.forEach(
          h -> {
            String dimName = h.getHierarchyInfo().getDimensionInfo().getName();
            restrictedLevels
                .computeIfAbsent(dimName, __ -> new HashMap<>())
                .put(h.getHierarchyInfo().getName(), null);
          });
      final IHierarchicalMapping restrictedMapping =
          hierarchicalMapping.restrictMapping(restrictedLevels);
      // Create the fieldExpressionPerCoordinateList from the restricted mapping to ensure
      // consistency of order for level coordinates
      for (int i = 0; i < restrictedMapping.getCoordinateCount(); i++) {
        final ILevelInfo lInfo = restrictedMapping.getLevelInfo(i);
        if (lInfo.getClassificationType() == ClassificationType.ALL) {
          continue;
        }
        String field;
        if (lInfo.getProperties() != null) {
          field =
              lInfo.getProperties().getProperty("fieldExpression") != null
                  ? lInfo.getProperties().getProperty("fieldExpression")
                  : lInfo.getName();
        } else {
          field = lInfo.getName();
        }
        fieldExpressionPerCoordinateList.add(field);
      }

      final List<IHierarchy> hierarchiesToVisit = new ArrayList<>();
      hierarchiesToVisit.add(activePivot.getHierarchies().get(0));
      hierarchiesToVisit.addAll(this.hierarchies);

      final ToDatastoreConditionFilterCompiler visitor =
          new ToDatastoreConditionFilterCompiler(
              fieldExpressionPerCoordinateList.toArray(new String[0]), false);
      this.filterCompiler =
          filter -> FilterVisitHelper.visit(filter, hierarchiesToVisit, restrictedMapping, visitor);
    } else {
      this.filterCompiler = __ -> BaseConditions.TRUE;
    }

    final List<String> fieldsList = new ArrayList<>();
    final TIntList fieldMappingIdxsList = new TIntArrayList();
    final BiConsumer<LevelCoordinate, String> levelAndFieldConsumer =
        (level, field) -> {
          fieldsList.add(field);
          fieldMappingIdxsList.add(
              hierarchicalMapping.getCoordinate(
                  this.mappingIndices.get(level)[0] + 1, this.mappingIndices.get(level)[1]));
        };
    this.mapping.forEach(levelAndFieldConsumer);

    this.fields = fieldsList.toArray(new String[0]);
    this.fieldMappingIdxs = fieldMappingIdxsList.toArray();
  }

  @Override
  public JoinContext createContext(
      IActivePivotVersion pivot,
      IScopeLocation primitiveRetrievalScope,
      IScopeLocation targetScope,
      IDatastoreRetrievalResult externalResult) {
    return new JoinContext(
        targetScope, primitiveRetrievalScope, externalResult, this.fields, this.fieldMappingIdxs);
  }

  /**
   * Iterates over each level declared in {@link #mappingIndices} and applies the given consumer on
   * each of them.
   *
   * @param pivot the pivot
   * @param consumer the consumer to apply
   */
  protected void forEachLevelInMapping(IActivePivot pivot, Consumer<ILevelInfo> consumer) {
    for (int[] hierAndLvlOrdinals : this.mappingIndices.values()) {
      final IHierarchy h = pivot.getHierarchies().get(hierAndLvlOrdinals[0] + 1);
      consumer.accept(h.getLevels().get(hierAndLvlOrdinals[1]).getLevelInfo());
    }
  }

  @Override
  public boolean buildRequestScope(
      ILocationBuilder location,
      ICubeFilter securityAndFilters,
      Collection<? extends IJoinMeasure<?, ?>> joinMeasures,
      IActivePivotVersion pivot,
      Collection<IMeasureMember> underlyingMeasures) {
    if (!handledHierarchiesAreExpressedOrRestricted(pivot, location, securityAndFilters)) {
      return false;
    }

    if (joinMeasures.isEmpty()) {
      // A primitive measure is requested. Make sure the requested location is at a "correct" depth
      // i.e every
      // level of handled hierarchies is expressed.
      forEachLevelInMapping(pivot, l -> location.expandToLevel(l));
      return true;
    } else if (!Collections.disjoint(this.supportedJoinMeasures, joinMeasures)) {
      // A requested join measure is provided by this procedure. Since in CoPPer, the creation of a
      // join measure
      // is done with the SingleValueFunction, we need to make sure the requested location is a the
      // correct depth
      // i.e every level of handled hierarchies is expressed
      forEachLevelInMapping(
          pivot,
          l -> {
            if (!LocationUtil.isAtOrBelowLevel(location, l)) {
              throw new QuartetRuntimeException(
                  "One of the join measure "
                      + joinMeasures
                      + " was not request "
                      + "at the correct depth for the hierarchy "
                      + l.getHierarchyInfo().getName()
                      + ". Requested location: "
                      + location);
            }
          });
      return true;
    } else {
      // A requested join measure is not handled by this procedure
      return false;
    }
  }

  /**
   * Checks that at least one hierarchy that should be handled by this procedure is either expressed
   * in the request location or a restriction exists on one of them.
   *
   * @param pivot the pivot
   * @param location the requested location
   * @param securityAndFilters cube filter grouping both security and filters restrictions
   * @return true if at least one hierarchy that should be handled by this procedure is either
   *     expressed in the request location or a restriction exists on one of them, false otherwise.
   */
  protected boolean handledHierarchiesAreExpressedOrRestricted(
      IActivePivotVersion pivot, ILocationBuilder location, ICubeFilter securityAndFilters) {
    if (getHandledHierarchies().isEmpty()) {
      // If no hierarchy, always true!
      return true;
    }

    final TIntObjectMap<HasAnalysisLevelCoordinatesResult> expressedHierarchies =
        LocationUtil.collectExpressedAnalysisHierarchies(
            location,
            CubeFilter.getUnderlyingSubCube(securityAndFilters),
            pivot.getAggregateProvider().getHierarchicalMapping());

    // If at least one hierarchy is expressed, the procedure should handled it.
    for (IHierarchy h : getHandledHierarchies()) {
      if (expressedHierarchies.containsKey(h.getOrdinal())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void mapToFullLocation(
      JoinContext context,
      IPointLocationReader primitiveRetrievalLocation,
      IAggregatesLocationResult aggregates,
      IPointLocationBuilder targetLocationBuilder,
      ResultConsumer consumer) {
    context.forEach(primitiveRetrievalLocation, targetLocationBuilder, consumer);
  }

  @Override
  public DatastorePrefetchRequest getDatastorePrefetch(
      ILocation prefetchLocation, ICubeFilter filter) {
    List<ICondition> conditions = new ArrayList<>();

    // Conditions from the location
    for (Entry<LevelCoordinate, int[]> entry : this.mappingIndices.entrySet()) {
      String field = this.mapping.get(entry.getKey());
      Object coord = prefetchLocation.getCoordinate(entry.getValue()[0], entry.getValue()[1]);
      IConstantCondition condition;
      if (coord == null) {
        condition = null;
      } else if (coord instanceof Collection) {
        condition = BaseConditions.In(field, (Collection<?>) coord);
      } else {
        condition = BaseConditions.Equal(field, coord);
      }

      if (condition != null) {
        conditions.add(condition);
      }
    }

    // Finally, the condition coming from the cube filter
    conditions.add(this.filterCompiler.apply(filter));

    return new DatastorePrefetchRequest(
        this.store,
        new HashSet<>(this.mapping.values()),
        AndConditionMerger.INSTANCE.mergeConditions(conditions));
  }

  /** Plugin factory. */
  @QuartetPluginValue(intf = IAnalysisAggregationProcedure.IFactory.class)
  public static class Factory extends AnalysigAggregationProcedureFactory<JoinContext> {

    private static final long serialVersionUID = 5_7L;

    /** Empty constructor. */
    public Factory() {
      super(PLUGIN_KEY, JoinAnalysisAggregationProcedure::new);
    }
  }
}
