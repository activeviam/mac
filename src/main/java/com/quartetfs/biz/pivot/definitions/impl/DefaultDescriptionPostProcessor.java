/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.quartetfs.biz.pivot.definitions.impl;

import com.activeviam.copper.HierarchyIdentifier;
import com.activeviam.mac.Workaround;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.pivot.util.impl.MdxNamingUtil;
import com.quartetfs.biz.pivot.context.IContextValue;
import com.quartetfs.biz.pivot.context.IMdxContext;
import com.quartetfs.biz.pivot.context.impl.ContextUtils;
import com.quartetfs.biz.pivot.context.impl.MdxContext;
import com.quartetfs.biz.pivot.cube.dimension.IDimension;
import com.quartetfs.biz.pivot.cube.hierarchy.IHierarchy;
import com.quartetfs.biz.pivot.cube.hierarchy.ILevel;
import com.quartetfs.biz.pivot.definitions.IActivePivotDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotSchemaDescription;
import com.quartetfs.biz.pivot.definitions.IAnalysisAggregationProcedureDescription;
import com.quartetfs.biz.pivot.definitions.IAxisDimensionDescription;
import com.quartetfs.biz.pivot.definitions.IAxisDimensionsDescription;
import com.quartetfs.biz.pivot.definitions.IAxisHierarchyDescription;
import com.quartetfs.biz.pivot.definitions.IContextValuesDescription;
import com.quartetfs.biz.pivot.definitions.IDistributedActivePivotDescription;
import com.quartetfs.biz.pivot.definitions.IEpochDimensionDescription;
import com.quartetfs.biz.pivot.definitions.IMeasuresDescription;
import com.quartetfs.biz.pivot.definitions.INativeMeasureDescription;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/*
 * HACK In order to consider the aggregation porcedures' measures when
 *  we update the MDX context
 */

/**
 * Post process an {@link IActivePivotDescription} to update it's {@link MdxContext}, putting the
 * formatters and default members defined outside the {@link MdxContext} to the {@link MdxContext}.
 */
@Workaround(jira = "APS-12725", solution = "Correctly exposes the formatter for join measures")
public class DefaultDescriptionPostProcessor
    implements IActivePivotDescription.IDescriptionPostProcessor {

  @Override
  public IActivePivotDescription postProcessActivePivotDescription(
      IDatastoreSchemaDescription datastoreDescription,
      IActivePivotSchemaDescription schemaDescription,
      String pivotId,
      IActivePivotDescription pivotDescription) {
    return postProcess(pivotDescription);
  }

  /**
   * Post-processes the description of an ActivePivot.
   *
   * @param pivotDescription initial description
   * @return processed description
   */
  public static IActivePivotDescription postProcess(IActivePivotDescription pivotDescription) {
    mergeSharedContexts(pivotDescription);
    updateMdxContext(pivotDescription);
    collectAggregationProceduresFromHierarchies(pivotDescription);
    return pivotDescription;
  }

  /**
   * Merge the identical context values together, with the last context value taking precedence.
   *
   * @param apDescription The {@link IActivePivotDescription description} to merge the values for.
   */
  public static void mergeSharedContexts(final IActivePivotDescription apDescription) {
    final IContextValuesDescription sharedContexts = apDescription.getSharedContexts();
    final Map<Class<? extends IContextValue>, IContextValue> map = new HashMap<>();

    for (IContextValue contextValue : sharedContexts.getValues()) {
      final Class<? extends IContextValue> intf = contextValue.getContextInterface();
      ContextUtils.addToContext(intf, contextValue, map, map);
    }

    apDescription.setSharedContexts(new ContextValuesDescription(new ArrayList<>(map.values())));
  }

  /**
   * Update the {@link MdxContext}, by adding the default formatters and default members.
   *
   * @param apDescription The {@link IActivePivotDescription description} to update the context for.
   */
  public static void updateMdxContext(final IActivePivotDescription apDescription) {
    final List<IContextValue> sharedContexts =
        new LinkedList<>(apDescription.getSharedContexts().getValues());

    // MDX context as defined in the xml
    final IMdxContext oldMdxContext = DescriptionUtil.getMdxContext(sharedContexts);
    final IMdxContext updatedContext = updateMdxContext(oldMdxContext, apDescription, false);
    if (updatedContext != null) {
      sharedContexts.add(updatedContext);
    }
    apDescription.setSharedContexts(new ContextValuesDescription(sharedContexts));
  }

  /**
   * Update the given {@link IMdxContext} by adding the formatters/default members/measures alias
   * from the given {@code apDescription}.
   *
   * <p>If {@code mdxContextToUpdate} is null, create an instance of {@link IMdxContext} to keep the
   * formatters/default members/measures alias. Otherwise, they are added into the given instance.
   *
   * <p>If the context value and the description both describe default members/measures alias for
   * the same measure or hierarchy, those defined by the context value are kept.
   *
   * <p>If the context value and the description both define formatters for the same measures and if
   * {@code overrideFormatters} is {@code true}, then keep those defined by the description.
   * Otherwise, keep the ones defined by the context value.
   *
   * @param mdxContextToUpdate The mdx context to update, can be null. If the value is non-null, and
   *     there are formatters/default members/alias in the pivot description to add into mdx
   *     context, they are added into this parameter.
   * @param apDescription The {@link IActivePivotDescription description} to update the context for.
   * @param overrideFormatters true if the formatters in the description will override those in the
   *     mdx context.
   * @return an instance of {@link IMdxContext} if the given mdx context is null, and there are
   *     formatters/default members/alias in the pivot description to add into mdx context
   *     Otherwise, returns null.
   */
  public static IMdxContext updateMdxContext(
      final IMdxContext mdxContextToUpdate,
      final IActivePivotDescription apDescription,
      final boolean overrideFormatters) {
    // Hacked part
    final IMeasuresDescription measuresDescription = apDescription.getMeasuresDescription();
    final IEpochDimensionDescription epochDimensionDescription =
        apDescription.getEpochDimensionDescription();
    final IAxisDimensionsDescription axisDimensionsDescription = apDescription.getAxisDimensions();
    final Collection<IAnalysisAggregationProcedureDescription> aggregationProceduresDescriptions =
        apDescription.getAggregationProcedures();

    final Map<String, String> formatters =
        mergeFormatters(
            mdxContextToUpdate,
            measuresDescription,
            epochDimensionDescription,
            axisDimensionsDescription,
            aggregationProceduresDescriptions,
            overrideFormatters);
    // End of hack
    final Map<String, List<String>> defaultMembers =
        mergeDefaultMembers(mdxContextToUpdate, measuresDescription, axisDimensionsDescription);
    final Map<String, String> alias = mergeMeasureAlias(mdxContextToUpdate, measuresDescription);

    final MdxContext ctx =
        (MdxContext) (mdxContextToUpdate != null ? mdxContextToUpdate : new MdxContext());
    ctx.setFormatters(formatters);
    ctx.setDefaultMembers(defaultMembers);
    ctx.setMeasureAliases(alias);

    if (mdxContextToUpdate == null
        && !(formatters.isEmpty() && defaultMembers.isEmpty() && alias.isEmpty())) {
      return ctx;
    }

    return null;
  }

  /**
   * Collects all the aggregation procedures defined directly in an analysis hierarchy description.
   *
   * <p>This explores hierarchy descriptions. Only the factory plugin key and the underlying levels
   * are retrieved from the hierarchy definitions.
   *
   * @param description pivot description to explore
   */
  public static void collectAggregationProceduresFromHierarchies(
      final IActivePivotDescription description) {
    if (description instanceof IDistributedActivePivotDescription) {
      return; // Nothing to do for distributed pivots
    }

    final Collection<IAnalysisAggregationProcedureDescription> newProcs = new ArrayList<>();
    for (final IAxisDimensionDescription dimension : description.getAxisDimensions().getValues()) {
      for (final IAxisHierarchyDescription hierarchy : dimension.getHierarchies()) {
        final Optional<String> aggregationPlugin =
            Optional.ofNullable(hierarchy.getProperties())
                .map(
                    props ->
                        props.getProperty(
                            IAnalysisAggregationProcedureDescription
                                .AGGREGATION_PROCEDURE_PROPERTY));
        if (aggregationPlugin.isPresent()) {
          final String levelProp =
              hierarchy
                  .getProperties()
                  .getProperty(IAnalysisAggregationProcedureDescription.UNDERLYING_LEVELS_PROPERTY);
          final Collection<String> underlyingLevels;
          if (levelProp != null) {
            underlyingLevels = Arrays.asList(levelProp.split(","));
          } else {
            underlyingLevels = Collections.emptyList();
          }

          final AnalysisAggregationProcedureDescription procedureDesc =
              new AnalysisAggregationProcedureDescription(
                  aggregationPlugin.get(),
                  Collections.singleton(
                      HierarchyIdentifier.toDescription(dimension.getName(), hierarchy.getName())),
                  underlyingLevels,
                  Collections.emptyList(),
                  new Properties());
          newProcs.add(procedureDesc);
        }
      }
    }

    if (!newProcs.isEmpty()) {
      if (description.getAggregationProcedures() != null) {
        newProcs.addAll(description.getAggregationProcedures());
      }
      description.setAggregationProcedures(newProcs);
    }
  }

  /**
   * Extract the measures and hierarchies' formatters in the given descriptions and add to the given
   * {@code mdxContext}. The formatters in {@code mdxContext} won't be overridden by those defined
   * in the descriptions.
   *
   * @param mdxContext The mdx context to merge the formatters, could be null.
   * @param measuresDescription The measures' description.
   * @param epochDimensionDescription The description of epoch dimension.
   * @param axisDimensionsDescription The axis' description.
   * @param aggregationProceduresDescriptions The aggregation procedures descriptions.
   * @param overrideFormattersInMdxContext {@code true} if the formatters in the given measure
   *     descriptions will override those in the {@code mdxContext}. {@code false} otherwise.
   * @return the formatters from {@code mdxContext} and the descriptions.
   */
  // Hacked method to consider the aggregation procedures
  public static Map<String, String> mergeFormatters(
      final IMdxContext mdxContext,
      final IMeasuresDescription measuresDescription,
      final IEpochDimensionDescription epochDimensionDescription,
      final IAxisDimensionsDescription axisDimensionsDescription,
      final Collection<IAnalysisAggregationProcedureDescription> aggregationProceduresDescriptions,
      final boolean overrideFormattersInMdxContext) {
    final Map<String, String> formatters = new HashMap<>();

    if (!overrideFormattersInMdxContext) {
      // Formatters for measures
      DescriptionUtil.addFormattersFromMeasures(
          formatters, measuresDescription.getAggregatedMeasuresDescription());
      DescriptionUtil.addFormattersFromMeasures(
          formatters, measuresDescription.getPostProcessorsDescription());
      DescriptionUtil.addFormattersFromMeasures(
          formatters, measuresDescription.getNativeMeasures());
      if (aggregationProceduresDescriptions != null) {
        DescriptionUtil.addFormattersFromMeasures(
            formatters,
            aggregationProceduresDescriptions.stream()
                .flatMap(aggProcDesc -> aggProcDesc.getJoinMeasures().stream())
                .collect(Collectors.toList()));
      }
    }

    // Formatters for axis dimensions
    if (axisDimensionsDescription != null) { // null if distributed description
      DescriptionUtil.addFormattersFromDimensions(
          formatters, axisDimensionsDescription.getValues());
    }

    // Formatters for the Epoch dimension
    if (epochDimensionDescription.isEnabled()) {
      if (epochDimensionDescription.getBranchLevelFormatter() != null) {
        String levelUniqueName =
            MdxNamingUtil.quote(
                IDimension.EPOCH_DIMENSION_NAME,
                IHierarchy.EPOCH_HIERARCHY_NAME,
                ILevel.BRANCH_LEVEL_NAME);
        formatters.put(levelUniqueName, epochDimensionDescription.getBranchLevelFormatter());
      }
      if (epochDimensionDescription.getEpochLevelFormatter() != null) {
        String levelUniqueName =
            MdxNamingUtil.quote(
                IDimension.EPOCH_DIMENSION_NAME,
                IHierarchy.EPOCH_HIERARCHY_NAME,
                ILevel.EPOCH_LEVEL_NAME);
        formatters.put(levelUniqueName, epochDimensionDescription.getEpochLevelFormatter());
      }
    }

    if (mdxContext != null) {
      // The formatters in the MDX context are privileged
      formatters.putAll(mdxContext.getFormatters());
    }

    if (overrideFormattersInMdxContext) {
      // Formatters for measures
      DescriptionUtil.addFormattersFromMeasures(
          formatters, measuresDescription.getAggregatedMeasuresDescription());
      DescriptionUtil.addFormattersFromMeasures(
          formatters, measuresDescription.getPostProcessorsDescription());
      DescriptionUtil.addFormattersFromMeasures(
          formatters, measuresDescription.getNativeMeasures());
    }

    return formatters;
  }

  /**
   * Extract the measures' and hierarchies' default members in the given descriptions and add to the
   * given {@code mdxContext}. The default members in {@code mdxContext} won't be overridden by
   * those defined in the descriptions.
   *
   * @param mdxContext The mdx context to merge the default members, could be null.
   * @param measuresDescription The measures' description.
   * @param axisDimensionsDescription The axis' description.
   * @return the default members from {@code mdxContext} and the descriptions.
   */
  public static Map<String, List<String>> mergeDefaultMembers(
      final IMdxContext mdxContext,
      final IMeasuresDescription measuresDescription,
      final IAxisDimensionsDescription axisDimensionsDescription) {
    final Map<String, List<String>> defaultMembers =
        axisDimensionsDescription != null
            ? DescriptionUtil.getDefaultMembers(axisDimensionsDescription.getValues())
            : new LinkedHashMap<>();
    if (measuresDescription.getDefaultMeasures() != null
        && measuresDescription.getDefaultMeasures().length > 0) {
      final List<String> uniqueNames =
          new ArrayList<>(measuresDescription.getDefaultMeasures().length);
      for (final String measureName : measuresDescription.getDefaultMeasures()) {
        uniqueNames.add(MdxNamingUtil.quote(measureName));
      }
      defaultMembers.put(MdxNamingUtil.quote(IDimension.MEASURES), uniqueNames);
    }

    if (mdxContext != null) {
      // The defautMembers defined in the MDX context are privileged
      defaultMembers.putAll(mdxContext.getDefaultMembers());
    }
    return defaultMembers;
  }

  /**
   * Extract the measures' alias in the given {@code measuresDescription} and add to the given
   * {@code mdxContext}. The default members in {@code mdxContext} won't be overridden by those
   * defined in the descriptions.
   *
   * @param mdxContext The mdx context to merge the default members, could be null.
   * @param measuresDescription The measures' description.
   * @return the measures' alias from {@code mdxContext} and the descriptions.
   */
  public static Map<String, String> mergeMeasureAlias(
      final IMdxContext mdxContext, final IMeasuresDescription measuresDescription) {
    final Map<String, String> alias = new HashMap<>();
    for (final INativeMeasureDescription desc : measuresDescription.getNativeMeasures()) {
      if (desc.getAlias() != null) {
        alias.put(desc.getName(), desc.getAlias());
      }
    }

    if (mdxContext != null) {
      // The alias defined in the MDX context are privileged
      alias.putAll(mdxContext.getMeasureAliases());
    }
    return alias;
  }
}
