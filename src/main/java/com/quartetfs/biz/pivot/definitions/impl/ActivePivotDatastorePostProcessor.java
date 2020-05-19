/*
 * (C) ActiveViam 2013-2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.quartetfs.biz.pivot.definitions.impl;

import static com.qfs.pivot.cube.provider.multi.IMultipleAggregateProvider.UNDERLYINGPROVIDER_PLUGINKEY;
import static com.quartetfs.biz.pivot.cube.hierarchy.axis.impl.MultiVersionAxisHierarchy.FIELD_NAME_PROPERTY;
import static com.quartetfs.biz.pivot.cube.hierarchy.axis.impl.MultiVersionAxisHierarchy.STORE_NAME_PROPERTY;

import com.activeviam.copper.IUserDatastoreSchemaDescription;
import com.activeviam.copper.LevelIdentifier;
import com.activeviam.copper.UserDatastoreSchemaDescription;
import com.activeviam.copper.agg.AJoinAnalysisAggregationProcedure;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.desc.IDatastoreSchemaDescriptionPostProcessor;
import com.qfs.desc.IFieldDescription;
import com.qfs.desc.IReferenceDescription;
import com.qfs.desc.IStoreDescription;
import com.qfs.desc.impl.DatastoreSchemaDescription;
import com.qfs.desc.impl.DatastoreSchemaDescriptionUtil;
import com.qfs.desc.impl.DictionarizeFieldsPostProcessor;
import com.qfs.desc.impl.NonNullableFieldsPostProcessor;
import com.qfs.desc.impl.ReferenceDescription;
import com.qfs.desc.impl.WeightMaximizingNumaSeletorPostProcessor;
import com.qfs.literal.ILiteralType;
import com.qfs.literal.impl.LiteralType;
import com.qfs.pivot.cube.provider.bitmap.impl.BitmapAggregateProviderBase;
import com.qfs.pivot.cube.provider.multi.impl.GlobalMultipleAggregateProviderBase;
import com.qfs.store.ConfigurationException;
import com.qfs.store.IDatastoreSchema;
import com.qfs.store.part.INumaNodeSelector;
import com.qfs.store.selection.ISelectionField;
import com.quartetfs.biz.pivot.definitions.IActivePivotDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotSchemaDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotSchemaInstanceDescription;
import com.quartetfs.biz.pivot.definitions.IAggregateProviderDefinition;
import com.quartetfs.biz.pivot.definitions.IAnalysisAggregationProcedureDescription;
import com.quartetfs.biz.pivot.definitions.IAxisDimensionDescription;
import com.quartetfs.biz.pivot.definitions.IAxisDimensionsDescription;
import com.quartetfs.biz.pivot.definitions.IAxisHierarchyDescription;
import com.quartetfs.biz.pivot.definitions.IAxisLevelDescription;
import com.quartetfs.biz.pivot.definitions.IPartialProviderDefinition;
import com.quartetfs.biz.pivot.definitions.ISelectionDescription;
import com.quartetfs.fwk.IPair;
import com.quartetfs.fwk.impl.Pair;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

/**
 * A {@link IDatastoreSchemaDescriptionPostProcessor} that propagates ActivePivot constraints to the
 * underlying datastore configuration.
 *
 * <ul>
 *   <li>All the fields used as levels in ActivePivot cubes must have a dictionary
 *   <li>None of the fields used as levels in ActivePivot cubes can be nullable
 * </ul>
 *
 * @author ActiveViam
 */
public class ActivePivotDatastorePostProcessor implements IDatastoreSchemaDescriptionPostProcessor {

  /** Field path separator */
  protected static final String SEP = IDatastoreSchema.PATH_SEPARATOR;

  /** Post processors */
  IDatastoreSchemaDescriptionPostProcessor[] postProcessors;
  /** The descriptions of the schemas that will impact the datastore description. */
  protected final Collection<IActivePivotSchemaDescription> schemasDesc;

  /**
   * Full constructor.
   *
   * @param schemasDesc The descriptions of the schemas that will impact the datastore description.
   * @param postProcessors list of datastore post-processor to execute
   */
  public ActivePivotDatastorePostProcessor(
      Collection<IActivePivotSchemaDescription> schemasDesc,
      IDatastoreSchemaDescriptionPostProcessor... postProcessors) {
    this.postProcessors = postProcessors;
    this.schemasDesc = schemasDesc;
  }

  /**
   * Finds the list of schema fields that are used as levels in the {@link #schemasDesc}.
   *
   * @return The list of schema field paths starting from (and including) the selection base store.
   */
  protected Collection<String> getUsedFieldPaths() {
    Set<String> fields = new HashSet<>();
    for (IActivePivotSchemaDescription schemaDesc : this.schemasDesc) {
      if (schemaDesc.getDatastoreSelection() != null) { // handle schema with only QUERY cubes
        addFieldsFromCubes(fields, schemaDesc);
      }
    }
    return fields;
  }

  @Override
  public IDatastoreSchemaDescription process(final IDatastoreSchemaDescription original) {
    IDatastoreSchemaDescription desc = original;

    if (postProcessors != null) {
      for (IDatastoreSchemaDescriptionPostProcessor pp : postProcessors) {
        desc = pp.process(desc);
      }
    }

    Collection<String> fields = getUsedFieldPaths();

    /*
     * Customize the behavior of this PP to throw a beautiful message
     * in case of misconfiguration. In this case, it is done here and
     * not directly in the basic implementation because the basic
     * implementation does not know anything about the roles of the
     * input fields. Related ticket PIVOT-2979
     */
    // Verifies the absence of vector fields in levels
    for (final IStoreDescription originalStore : original.getStoreDescriptions()) {
      final String storeName = originalStore.getName();
      for (final IFieldDescription field : originalStore.getFields()) {
        if (fields.contains(storeName + SEP + field.getName())) {
          final ILiteralType type = LiteralType.getType(field.getDataType());
          if (type == null) {
            throw new ConfigurationException(
                "The field named "
                    + field.getName()
                    + " in store "
                    + originalStore.getName()
                    + " is of Unknown type.",
                "Please make sure that the type defined in the field description is a valid one.");
          }
          if (type.isArray()) {
            throw new ConfigurationException(
                "The field named "
                    + field.getName()
                    + " in store "
                    + originalStore.getName()
                    + " is of type Vector and "
                    + "intended to be used as level.",
                "Vector fields are not allowed. Please use List as a substitute.");
          }
        }
      }
    }
    IDatastoreSchemaDescriptionPostProcessor nonNullable =
        new NonNullableFieldsPostProcessor(fields) {

          private static final String template =
              "Field %s of store %s is intended to be used as a level but is nullable or has a null default value or this default value is an empty string.";

          @Override
          protected void nonNullableFieldWithNullDefaultValueHandler(
              IStoreDescription storeDescription, IFieldDescription fieldDescription) {
            throw new ConfigurationException(
                String.format(template, fieldDescription.getName(), storeDescription.getName()),
                "Make it not nullable or choose a non null non-empty default value for this field.");
          }
        };

    IDatastoreSchemaDescriptionPostProcessor dictionarize =
        new DictionarizeFieldsPostProcessor(fields);
    desc = nonNullable.process(desc);
    desc = dictionarize.process(desc);

    for (IActivePivotSchemaDescription schemaDescription : schemasDesc) {
      desc = new JoinProcedurePostProcessor(schemaDescription).process(desc);
    }

    return desc;
  }

  /**
   * Create the list of fields to dictionarize in the Datastore based on a selection and the list of
   * the fields names of this selection that need to be dictionarized on ActivePivot side.
   *
   * @param selection the Datastore field selection.
   * @param fieldsToDictionarize the list of field names in this selection that need to be
   *     dictionarized.
   * @return the list of fields to dictionarize.
   */
  protected static Collection<String> extractFields(
      ISelectionDescription selection, Collection<String> fieldsToDictionarize) {
    Collection<String> fields = new ArrayList<>(selection.getFields().size());
    String baseStore = selection.getBaseStore();
    for (ISelectionField field : selection.getFields()) {
      if (fieldsToDictionarize.contains(field.getName())) {
        fields.add(baseStore + SEP + field.getExpression());
      }
    }
    return fields;
  }

  /**
   * Collects all store fields referenced in an {@link IActivePivotSchemaDescription}.
   *
   * @param target target collection to fill with discovered fields
   * @param schemaDesc schema description
   */
  public static void addFieldsFromCubes(
      Collection<String> target, IActivePivotSchemaDescription schemaDesc) {
    for (IActivePivotInstanceDescription apid : schemaDesc.getActivePivotInstanceDescriptions()) {
      for (IAxisDimensionDescription add :
          apid.getActivePivotDescription().getAxisDimensions().getValues()) {
        addFieldsFromDimension(target, schemaDesc.getDatastoreSelection(), add);
      }
    }
  }

  /**
   * Adds in a collection all the paths of the datastore fields used as levels of a cube.
   *
   * @param target The collection to which the fields are added.
   * @param selectionDescription The description of the selection feeding the cube.
   * @param dimensionDesc The dimensions of the cube containing the aforementioned levels.
   */
  public static void addFieldsFromDimension(
      Collection<String> target,
      ISelectionDescription selectionDescription,
      IAxisDimensionDescription dimensionDesc) {
    Collection<String> schemaFieldsUsedAsLevels = new HashSet<>();
    for (IAxisHierarchyDescription hier : dimensionDesc.getHierarchies()) {
      final String storeName = getProperty(hier.getProperties(), STORE_NAME_PROPERTY);
      if (storeName == null) {
        // Hierarchy linked to base store
        for (IAxisLevelDescription ald : hier.getLevels()) {
          schemaFieldsUsedAsLevels.add(ald.getPropertyName());
        }
      } else {
        // Fact-less hierarchy based on fields not linked to the base store
        for (IAxisLevelDescription ald : hier.getLevels()) {
          final String fieldName = getProperty(ald.getProperties(), FIELD_NAME_PROPERTY);
          target.add(storeName + SEP + fieldName);
        }
      }
    }
    target.addAll(extractFields(selectionDescription, schemaFieldsUsedAsLevels));
  }

  /**
   * Gets a given property if a {@link Properties} is defined.
   *
   * @param props properties holder
   * @param key property key
   * @return property value or null if no properties are provided
   */
  protected static String getProperty(Properties props, String key) {
    return null == props ? null : props.getProperty(key);
  }

  /**
   * Creates a schema post processor that applies ActivePivot requirements to an existing datastore
   * description.
   *
   * @param desc schema description
   * @return ActivePivot Datastore post processor
   */
  public static ActivePivotDatastorePostProcessor createFrom(IActivePivotSchemaDescription desc) {
    final Collection<IActivePivotSchemaDescription> schemas = Collections.singleton(desc);
    return new ActivePivotDatastorePostProcessor(schemas, numaSelectorPostProcessor(schemas));
  }

  /**
   * Creates a schema post processor that applies ActivePivot requirements to an existing datastore
   * description.
   *
   * @param desc manager description
   * @return ActivePivot Datastore post processor
   */
  public static ActivePivotDatastorePostProcessor createFrom(IActivePivotManagerDescription desc) {
    List<IActivePivotSchemaDescription> schemas =
        desc.getSchemas().stream()
            .map(IActivePivotSchemaInstanceDescription::getActivePivotSchemaDescription)
            .collect(Collectors.toList());
    return new ActivePivotDatastorePostProcessor(schemas, numaSelectorPostProcessor(schemas));
  }

  /**
   * Increments the counter of the expressions with the field present in a given level.
   *
   * @param fieldToExpression for each schema field, the expression in the base store
   * @param expressionCounter The field counters
   * @param incrementedExpressions collection to complete with field expresssions incremented by
   *     this call
   * @param levelDescription level description
   */
  protected static void increment(
      Map<String, String> fieldToExpression,
      TObjectIntMap<String> expressionCounter,
      Set<String> incrementedExpressions,
      IAxisLevelDescription levelDescription) {
    final String schemaField = levelDescription.getPropertyName();
    final String baseStoreField = fieldToExpression.get(schemaField);
    if (baseStoreField != null && !incrementedExpressions.contains(baseStoreField)) {
      // A same expression may appear multiple times in the dimensions
      // Only the first one should be take in to account
      expressionCounter.increment(baseStoreField);
      incrementedExpressions.add(baseStoreField);
    }
  }

  /**
   * Increments the counter of the expressions in the selection of the bitmap provider
   *
   * @param fieldToExpression for each schema field, the expression in the base store
   * @param expressionCounter The field counters
   * @param provider The provider
   * @param dims The dimensions
   */
  protected static void incrementExpressionCounter(
      Map<String, String> fieldToExpression,
      TObjectIntMap<String> expressionCounter,
      IPartialProviderDefinition provider,
      IAxisDimensionsDescription dims) {
    assert provider.getPluginKey().equals(BitmapAggregateProviderBase.PLUGIN_TYPE);

    Set<String> incrementedExpressions = new HashSet<>();
    final boolean include = provider.getIncludeHierarchies();
    final Map<String, Map<String, String>> scope = provider.getScope();
    for (IAxisDimensionDescription di : dims.getValues()) {
      for (IAxisHierarchyDescription hier : di.getHierarchies()) {
        final List<IAxisLevelDescription> levels = hier.getLevels();
        final String hierarchy = hier.getName();
        if (scope == null) {
          // If the scope is null, then increment for all the levels
          for (IAxisLevelDescription ald : levels) {
            increment(fieldToExpression, expressionCounter, incrementedExpressions, ald);
          }
        } else {
          // Get the scope for the dimension of this hierarchy
          final Map<String, String> dimScope = scope.get(di.getName());
          String level;
          if (dimScope == null) {
            // Maybe this hierarchy was declared without dimension
            // Get the scope for the dimension NO_DIMENSION_DEFINED and check
            final Map<String, String> noDimDefScope =
                scope.get(PartialProviderDefinition.NO_DIMENSION_DEFINED);
            if (noDimDefScope != null && noDimDefScope.containsKey(hierarchy)) {
              // This hierarchy was declared without dimension
              level = noDimDefScope.get(hierarchy);
            } else {
              // This hierarchy was not declared
              if (include) {
                // This hierarchy was not included, then pass to the next hierarchy
                continue;
              } else {
                // This hierarchy was included, then get the last level
                final int size = levels.size();
                // Check the level size
                if (size == 0) {
                  continue;
                } else {
                  // Get the last level
                  level = levels.get(size - 1).getLevelName();
                }
              }
            }
          } else {
            // Check if this hierarchy was declared for this dimension
            if (dimScope.containsKey(hierarchy)) {
              // This hierarchy was declared for this dimension
              level = dimScope.get(hierarchy);
            } else {
              // This hierarchy was not declared for this dimension
              if (include) {
                // This hierarchy was not included, then pass to the next hierarchy
                continue;
              } else {
                // This hierarchy was included, then get the last level
                final int size = levels.size();
                // Check the level size
                if (size == 0) {
                  continue;
                } else {
                  // Get the last level
                  level = levels.get(size - 1).getLevelName();
                }
              }
            }
          }

          // Increments the expression counter
          if (include) {
            // Check the level after incrementing
            // Increment till arrive to the level declared
            // If level is null then increment for all the levels
            for (IAxisLevelDescription ald : levels) {
              increment(fieldToExpression, expressionCounter, incrementedExpressions, ald);
              if (ald.getLevelName().equals(level)) {
                break;
              }
            }
          } else {
            // Check the level before incrementing
            // Increment till arrive to the parent of the level declared
            // If level is null then pass to the the hierarchy
            for (IAxisLevelDescription ald : levels) {
              if (level == null || ald.getLevelName().equals(level)) {
                break;
              }
              increment(fieldToExpression, expressionCounter, incrementedExpressions, ald);
            }
          }
        }
      }
    }

    assert provider.getPartialProviders() == null || provider.getPartialProviders().isEmpty();
  }

  /**
   * Counts for each base store field, the number of bitmap providers that contain it.
   *
   * @param schema The pivot schema
   * @param numBitmapByDatastoreField for each base store's field, the number of bitmap providers
   *     that contain it
   */
  public static void countBitmap(
      IActivePivotSchemaDescription schema, TObjectIntMap<String> numBitmapByDatastoreField) {
    Map<String, String> fieldToExpression = new HashMap<>();
    for (ISelectionField field : schema.getDatastoreSelection().getFields()) {
      String expr = field.getExpression();
      if (isOfBaseStore(expr)) { // Base store field
        fieldToExpression.put(field.getName(), expr);
        numBitmapByDatastoreField.putIfAbsent(expr, 0);
      }
    }

    // Number of bitmap that contains on the field of the selection
    int globalBitmapCount = 0;
    for (IActivePivotInstanceDescription instance : schema.getActivePivotInstanceDescriptions()) {
      IActivePivotDescription pivot = instance.getActivePivotDescription();
      IAggregateProviderDefinition globalProvider = pivot.getAggregateProvider();

      final boolean isBitmap =
          globalProvider.getPluginKey().equals(BitmapAggregateProviderBase.PLUGIN_TYPE);
      final boolean isMultiple =
          globalProvider.getPluginKey().equals(GlobalMultipleAggregateProviderBase.PLUGIN_TYPE);
      final boolean withBitmapUnderlying =
          (globalProvider.getProperties() == null)
              ? true
              : globalProvider
                  .getProperties()
                  .getProperty(
                      UNDERLYINGPROVIDER_PLUGINKEY, BitmapAggregateProviderBase.PLUGIN_TYPE)
                  .equals(BitmapAggregateProviderBase.PLUGIN_TYPE);
      if (isBitmap || (isMultiple && withBitmapUnderlying)) {
        globalBitmapCount++;
      }

      if (globalProvider.getPartialProviders() != null) {
        for (IPartialProviderDefinition partial : globalProvider.getPartialProviders()) {
          if (partial.getPluginKey().equals(BitmapAggregateProviderBase.PLUGIN_TYPE)) {
            incrementExpressionCounter(
                fieldToExpression, numBitmapByDatastoreField, partial, pivot.getAxisDimensions());
          }
        }
      }
    }

    if (globalBitmapCount > 0) {
      for (String expr : fieldToExpression.values()) {
        numBitmapByDatastoreField.adjustValue(expr, globalBitmapCount);
      }
    }
  }

  /**
   * Tests whether an expression is of the base store.
   *
   * @param expression The expression
   * @return true if the expression is a field of the base store.
   */
  protected static boolean isOfBaseStore(String expression) {
    return expression.indexOf(SEP) == -1;
  }

  /**
   * Creates a {@link WeightMaximizingNumaSeletorPostProcessor} that uses "the number of bitmap
   * providers that contain each datastore field" as criteria of choosing {@link INumaNodeSelector
   * numa selector} field for each store.
   *
   * @param schemas list of schemas to consider
   * @return the numa post processor
   */
  public static WeightMaximizingNumaSeletorPostProcessor numaSelectorPostProcessor(
      final Collection<? extends IActivePivotSchemaDescription> schemas) {

    // Triple map: base store - store field name - number of bitmap provider that contain this field
    Map<String, TObjectIntMap<String>> storeFieldCount = new HashMap<>();
    for (IActivePivotSchemaDescription schema : schemas) {
      ISelectionDescription datastoreSelection = schema.getDatastoreSelection();
      if (datastoreSelection != null) { // Schema with only QUERY cubes
        String baseStore = datastoreSelection.getBaseStore();
        TObjectIntMap<String> fieldCount;
        if (storeFieldCount.containsKey(baseStore)) {
          fieldCount = storeFieldCount.get(baseStore);
        } else {
          fieldCount = new TObjectIntHashMap<>();
          storeFieldCount.put(baseStore, fieldCount);
        }

        // increment fieldCount by the number of bitmaps that contains each field
        countBitmap(schema, fieldCount);
      }
    }
    return new WeightMaximizingNumaSeletorPostProcessor(storeFieldCount);
  }

  /**
   * Adds {@link IReferenceDescription} to the schema description to process to share dictionaries
   * for fields used in the mapping of joins are made in Copper. See {@link
   * IDatastoreSchemaDescription#getSameDictionaryDescriptions()}.
   */
  protected static class JoinProcedurePostProcessor
      implements IDatastoreSchemaDescriptionPostProcessor {

    /** The description of the schema that will impact the datastore description. */
    protected final IActivePivotSchemaDescription schemaDescription;

    /** The generator for unique IDs for references created by Copper. */
    protected static final IntSupplier ID_GENERATOR = new AtomicInteger()::getAndIncrement;

    /**
     * Constructor.
     *
     * @param schemaDescription The descriptions of the schema that will impact the datastore
     *     description.
     */
    public JoinProcedurePostProcessor(IActivePivotSchemaDescription schemaDescription) {
      this.schemaDescription = schemaDescription;
    }

    @Override
    public IDatastoreSchemaDescription process(
        IDatastoreSchemaDescription datastoreSchemaDescription) {
      List<IReferenceDescription> refs =
          new ArrayList<>(datastoreSchemaDescription.getSameDictionaryDescriptions());
      for (IActivePivotInstanceDescription cubeDescription :
          this.schemaDescription.getActivePivotInstanceDescriptions()) {
        refs.addAll(
            extractJoinAggregationProcedureReferences(
                datastoreSchemaDescription,
                this.schemaDescription,
                cubeDescription.getActivePivotDescription()));
      }
      return refs.isEmpty()
          ? datastoreSchemaDescription
          : new DatastoreSchemaDescription(
              datastoreSchemaDescription.getStoreDescriptions(),
              datastoreSchemaDescription.getReferenceDescriptions(),
              refs);
    }

    /**
     * Builds the references coming from join made in Copper. These references are only used for
     * sharing dictionaries when possible. See {@link
     * IDatastoreSchemaDescription#getSameDictionaryDescriptions()}.
     *
     * @param datastoreSchemaDescription the {@link IDatastoreSchemaDescription}
     * @param schemaDescription the IActivePivotSchemaDescription
     * @param description the cube description that contains the {@link
     *     IAnalysisAggregationProcedureDescription} that describe the joins between the cube and
     *     other (isolated) stores)
     * @return the references
     */
    protected static Collection<IReferenceDescription> extractJoinAggregationProcedureReferences(
        IDatastoreSchemaDescription datastoreSchemaDescription,
        IActivePivotSchemaDescription schemaDescription,
        IActivePivotDescription description) {
      Collection<IAnalysisAggregationProcedureDescription> procedures =
          description.getAggregationProcedures();
      Collection<IReferenceDescription> result = new ArrayList<>();
      IUserDatastoreSchemaDescription userDatastoreSchemaDescription =
          UserDatastoreSchemaDescription.createFromSchemaDescription(datastoreSchemaDescription);

      for (IAnalysisAggregationProcedureDescription procedure : procedures) {
        if (AJoinAnalysisAggregationProcedure.JOIN_KEYS.contains(procedure.getPluginKey())) {
          @SuppressWarnings("unchecked")
          Map<LevelIdentifier, String> mapping =
              (Map<LevelIdentifier, String>)
                  procedure.getProperties().get(AJoinAnalysisAggregationProcedure.MAPPING);
          String store =
              (String) procedure.getProperties().get(AJoinAnalysisAggregationProcedure.STORE);

          Map<LevelIdentifier, IPair<String, String>> info =
              getStoreAndFieldByLevel(
                  mapping.keySet(),
                  userDatastoreSchemaDescription,
                  schemaDescription.getDatastoreSelection(),
                  description);
          for (Map.Entry<LevelIdentifier, IPair<String, String>> entry : info.entrySet()) {
            // Modified part :
            // We need to extract potential references in the field path and
            // change the target and owner stores accordingly if necessary.
            // If not done properly, dictionaries will not be merged on the datastore creation
            String fromField = entry.getValue().getRight();
            String fromStore = entry.getValue().getLeft();

            final Pair<String, String> processedFromPath =
                processPath(fromField, fromStore, datastoreSchemaDescription);
            fromField = processedFromPath.getLeft();
            fromStore = processedFromPath.getRight();

            // Doing the same thing for the target field/stores :
            String toField = mapping.get(entry.getKey());
            String toStore = store;

            final Pair<String, String> processedToPath =
                processPath(toField, toStore, datastoreSchemaDescription);
            toField = processedToPath.getLeft();
            toStore = processedToPath.getRight();
            // End of modified part
            // The name of the reference does not matter here.
            String referenceName = "copper.ref." + ID_GENERATOR.getAsInt();
            // The two fields can share the same dictionary
            result.add(
                new ReferenceDescription(
                    fromStore,
                    toStore,
                    referenceName,
                    Collections.singletonList(new Pair<>(fromField, toField))));
          }
        }
      }
      return result;
    }

    /**
     * Returns a field,store pair with the field string cleaned p from references and the store
     * corresponding to the store obtained by following references, from the input store through the
     * references obtained from the field path.
     *
     * @param field input field path
     * @param store input store
     * @param datastoreSchemaDescription current datastore description
     * @return the processed field and stores
     */
    protected static Pair<String, String> processPath(
        final String field,
        final String store,
        IDatastoreSchemaDescription datastoreSchemaDescription) {
      String[] splitFromField = DatastoreSchemaDescriptionUtil.splitPath(field);
      if (splitFromField.length > 1) {
        final String resField = splitFromField[splitFromField.length - 1];
        String resStore;
        String finalFromStore = store;
        List<IStoreDescription> startStores =
            datastoreSchemaDescription.getStoreDescriptions().stream()
                .filter(p -> p.getName().equals(finalFromStore))
                .collect(Collectors.toList());
        assert startStores.size() == 1
            : "There should be exactly one store with the name"
                + finalFromStore
                + " in the datastore";
        IStoreDescription currentStore = startStores.get(0);
        // Follow the references on the split path
        for (int i = 0; i < splitFromField.length - 1; i++) {
          IStoreDescription finalCurrentStore = currentStore;
          List<? extends IReferenceDescription> validRefsFromCurrent =
              datastoreSchemaDescription.getReferenceDescriptions().stream()
                  .filter(
                      r ->
                          r.getOwnerStore().equals(finalCurrentStore.getName())
                              && r.getName().equals(splitFromField[0]))
                  .collect(Collectors.toList());
          assert validRefsFromCurrent.size() == 1
              : "There should be exactly one valid reference on the path";
          currentStore =
              datastoreSchemaDescription.getStoreDescriptions().stream()
                  .filter(s -> s.getName().equals(validRefsFromCurrent.get(0).getTargetStore()))
                  .collect(Collectors.toList())
                  .get(0);
        }
        resStore = currentStore.getName();
        return new Pair<>(resField, resStore);
      } else {
        return new Pair<>(field, store);
      }
    }

    /**
     * For the given levels, retrieves for each one of them identified by their {@link
     * LevelIdentifier} the store and the field it refers to.
     *
     * @param levels the levels
     * @param userDatastoreSchemaDescription the datastore schema description
     * @param datastoreSelection the datastore selection
     * @param description the cube description
     * @return pairs of store and field indexed by level
     */
    protected static Map<LevelIdentifier, IPair<String, String>> getStoreAndFieldByLevel(
        Set<LevelIdentifier> levels,
        IUserDatastoreSchemaDescription userDatastoreSchemaDescription,
        ISelectionDescription datastoreSelection,
        IActivePivotDescription description) {
      Map<LevelIdentifier, IPair<String, String>> fieldInfoByLevelCoord = new HashMap<>();
      levelLoop:
      for (LevelIdentifier coordinate : levels) {
        for (IAxisDimensionDescription dim : description.getAxisDimensions().getValues()) {
          if (dim.getName().equals(coordinate.parent.dimension)) {
            for (IAxisHierarchyDescription hier : dim.getHierarchies()) {
              if (hier.getName().equals(coordinate.parent.hierarchy)) {
                for (IAxisLevelDescription lvl : hier.getLevels()) {
                  if (lvl.getLevelName().equals(coordinate.level)) {
                    // The mapping may contain a field used in an analysis hierarchy.
                    // But this field is not part of the selection
                    // so the result map won't contain any information about it.
                    for (ISelectionField field :
                        datastoreSelection.getSelectionFields().getValues()) {
                      if (field.getName().equals(lvl.getPropertyName())) {
                        String[] path =
                            DatastoreSchemaDescriptionUtil.splitPath(field.getExpression());
                        IUserDatastoreSchemaDescription.IUserStoreDescription storeDescription =
                            userDatastoreSchemaDescription.getStoreDescriptionAlongPath(
                                datastoreSelection.getBaseStore(), path);
                        fieldInfoByLevelCoord.put(
                            coordinate,
                            new Pair<>(storeDescription.getStoreName(), path[path.length - 1]));
                        continue levelLoop;
                      }
                    }
                    continue levelLoop;
                  }
                }
              }
            }
          }
        }
      }
      return fieldInfoByLevelCoord;
    }
  }
}
