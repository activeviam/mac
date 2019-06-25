/*
 * (C) ActiveViam 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.copper.foundry.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import com.activeviam.builders.manager.SimpleManagerDescriptionBuilder;
import com.activeviam.collections.ImmutableList;
import com.activeviam.collections.ImmutableMap;
import com.activeviam.collections.ImmutableSet;
import com.activeviam.collections.impl.Empty;
import com.activeviam.collections.impl.Immutable;
import com.activeviam.collections.impl.ImmutableArrayList;
import com.activeviam.collections.impl.ImmutableHashMap;
import com.activeviam.copper.LevelCoordinate;
import com.activeviam.copper.builders.dataset.CoreDataset;
import com.activeviam.copper.builders.dataset.Datasets.Dataset;
import com.activeviam.copper.builders.impl.BuildOptions;
import com.activeviam.copper.builders.impl.PublishOptions;
import com.activeviam.copper.columns.CoreColumn;
import com.activeviam.copper.columns.PublicationMetadata;
import com.activeviam.copper.exceptions.InvalidCalculationException;
import com.activeviam.copper.exceptions.UserErrorException;
import com.activeviam.copper.exceptions.codeinfo.CreatingSourceCodeInfo;
import com.activeviam.copper.foundry.CubeBuildFoundry;
import com.activeviam.copper.foundry.CubeDescriptionWithoutCopperMeasures;
import com.activeviam.copper.foundry.IUserDatastoreSchemaDescription;
import com.activeviam.copper.foundry.casting.AlloyGraph;
import com.activeviam.copper.foundry.casting.AnalysisAggregationProcedureAlloy;
import com.activeviam.copper.foundry.casting.HierarchyAlloy;
import com.activeviam.copper.foundry.casting.LiquidAlloy;
import com.activeviam.copper.foundry.casting.LiquidAlloyVisitor;
import com.activeviam.copper.foundry.casting.MeasureAlloy;
import com.activeviam.copper.foundry.casting.SolidAlloy;
import com.activeviam.copper.foundry.casting.liquid.impl.OperatorAlloy;
import com.activeviam.copper.foundry.casting.liquid.impl.StoreFieldAlloy;
import com.activeviam.copper.foundry.casting.mold.MeasureNameProvider;
import com.activeviam.copper.foundry.casting.mold.storequery.TrivialAnalysisHierarchyDescription;
import com.activeviam.copper.foundry.casting.solid.agg.JoinAnalysisAggregationProcedure;
import com.activeviam.copper.foundry.casting.solid.impl.HiddenMeasureAlloy;
import com.activeviam.copper.operator.column.values.HierarchyCustomizerOperator;
import com.activeviam.copper.operator.visitor.impl.HierarchyCustomizerOperatorRetriever;
import com.activeviam.copper.testing.printer.JungDatasetPrintOptions;
import com.activeviam.copper.testing.printer.JungDatasetPrinter;
import com.qfs.desc.IDatastoreSchemaDescription;
import com.qfs.pivot.cube.provider.jit.impl.JustInTimeAggregateProviderBase;
import com.quartetfs.biz.pivot.IActivePivot;
import com.quartetfs.biz.pivot.context.impl.QueriesTimeLimit;
import com.quartetfs.biz.pivot.definitions.IActivePivotDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotManagerDescription;
import com.quartetfs.biz.pivot.definitions.IActivePivotSchemaDescription;
import com.quartetfs.biz.pivot.definitions.IAggregatedMeasureDescription;
import com.quartetfs.biz.pivot.definitions.IAnalysisAggregationProcedureDescription;
import com.quartetfs.biz.pivot.definitions.IAxisDimensionDescription;
import com.quartetfs.biz.pivot.definitions.IAxisHierarchyDescription;
import com.quartetfs.biz.pivot.definitions.IJoinMeasureDescription;
import com.quartetfs.biz.pivot.definitions.IMeasureMemberDescription;
import com.quartetfs.biz.pivot.definitions.INativeMeasureDescription;
import com.quartetfs.biz.pivot.definitions.IPostProcessorDescription;
import com.quartetfs.biz.pivot.definitions.ISelectionDescription;
import com.quartetfs.biz.pivot.definitions.impl.ActivePivotInstanceDescription;
import com.quartetfs.biz.pivot.definitions.impl.AggregateProviderDefinition;
import com.quartetfs.biz.pivot.definitions.impl.AggregatesCacheDescription;
import com.quartetfs.biz.pivot.definitions.impl.AnalysisAggregationProcedureDescription;
import com.quartetfs.biz.pivot.definitions.impl.AxisDimensionDescription;
import com.quartetfs.biz.pivot.definitions.impl.AxisDimensionsDescription;
import com.quartetfs.biz.pivot.definitions.impl.ContextValuesDescription;
import com.quartetfs.biz.pivot.definitions.impl.EpochDimensionDescription;
import com.quartetfs.biz.pivot.definitions.impl.MeasuresDescription;
import com.quartetfs.biz.pivot.postprocessing.IPostProcessor;
import com.quartetfs.fwk.QuartetRuntimeException;


import static com.activeviam.copper.foundry.casting.mold.impl.CommonErrorMessages.UNFINISHED_CALCULATION_MESSAGE_SEVERAL_LEVELS;
import static com.activeviam.copper.foundry.casting.mold.impl.CommonErrorMessages.UNFINISHED_CALCULATION_MESSAGE_SINGLE_LEVEL;

/**
 * Implementation using a {@link Caster} to converts the {@link Dataset dataset} into
 * {@link IMeasureMemberDescription measures}.
 */
public class CastingCubeBuildFoundry implements CubeBuildFoundry {

	/** Class logger */
	private static final Logger LOGGER = Logger.getLogger(CastingCubeBuildFoundry.class.getName());

	/** The datasets contained in this foundry. */
	protected final ImmutableMap<String, CoreDataset> datasets;
	/** the {@link PublishOptions} of the {@link #datasets} */
	protected final ImmutableMap<String, PublishOptions> publishingOptions;
	/** The description of datastore schema. */
	protected final IDatastoreSchemaDescription datastoreDescription;
	/** The target schema selection. */
	protected final ISelectionDescription selection;
	/** The target cube description. */
	protected final CubeDescriptionWithoutCopperMeasures cubeDescription;
	/** The caster. */
	protected final Caster caster;
	/** The result of the dataset casts. Lazily created. */
	protected ImmutableMap<CoreColumn, SolidAlloy> castResult;
	/** The list of added measures. Lazily created. */
	protected ImmutableList<IMeasureMemberDescription> addedMeasures;
	/**
	 * The list of added joined measures. Lazily created. They are treated separately from {@link #addedMeasures} because
	 * they will be added to the cube via the {@link #procedures} they belong to.
	 */
	protected ImmutableList<IJoinMeasureDescription> addedJoinMeasures;
	/** The list of added hierarchies within their dimensions. Lazily created. */
	protected ImmutableList<IAxisDimensionDescription> hierarchiesWithinDimensions;
	/** The list of procedures that works with {@link #hierarchiesWithinDimensions}. Lazily created. */
	protected ImmutableList<IAnalysisAggregationProcedureDescription> procedures;
	/**
	 * Constructor.
	 *
	 * @param datasets The contained datasets.
	 * @param publishingOptions the {@link PublishOptions} of the {@code datasets}
	 * @param datastoreDescription The description of datastore schema.
	 * @param selection The base store selection.
	 * @param cubeDescription The cube description from user.
	 */
	public CastingCubeBuildFoundry(
			ImmutableMap<String, CoreDataset> datasets,
			ImmutableMap<String, PublishOptions> publishingOptions,
			IDatastoreSchemaDescription datastoreDescription,
			ISelectionDescription selection,
			CubeDescriptionWithoutCopperMeasures cubeDescription) {
		this.datasets = datasets;
		this.publishingOptions = publishingOptions;
		this.datastoreDescription = datastoreDescription;
		this.selection = selection;
		this.cubeDescription = cubeDescription;
		IUserDatastoreSchemaDescription schemaDescription = UserDatastoreSchemaDescription.createFromSchemaDescription(datastoreDescription);

		Map<CoreDataset, PublishOptions> optionsPerDataset = new HashMap<>();
		for (Entry<String, CoreDataset> d : datasets.entrySet()) {
			if(publishingOptions.get(d.getKey()) == null) {
				throw new QuartetRuntimeException("No publishing option is defined for the dataset: " + d.getKey());
			}
			optionsPerDataset.put(d.getValue(), publishingOptions.get(d.getKey()));
		}
		this.caster = createCaster(ImmutableHashMap.of(optionsPerDataset), schemaDescription, selection, cubeDescription);
	}

	/**
	 * Creates the caster for multiple datasets.
	 *
	 * @param datasets The datasets that will be cast.
	 * @param datastoreSchema The description of datastore schema.
	 * @param selection The base store selection.
	 * @param cubeDescription The cube description from user.
	 *
	 * @return The caster.
	 */
	public static Caster createCaster(
			ImmutableMap<CoreDataset, PublishOptions> datasets,
			IUserDatastoreSchemaDescription datastoreSchema,
			ISelectionDescription selection,
			CubeDescriptionWithoutCopperMeasures cubeDescription) {
		return new Caster(
				extractColumnToExport(datasets),
				datastoreSchema,
				selection,
				cubeDescription);
	}

	/**
	 * Extracts the {@link CoreColumn columns} to export from the given datasets. These columns are meant to be cast.
	 *
	 * @param datasets The datasets that will be cast to their {@link PublishOptions}
	 * @return The columns that are meant to be cast to their {@link PublishOptions}
	 */
	protected static ImmutableMap<CoreColumn, PublishOptions> extractColumnToExport(ImmutableMap<CoreDataset, PublishOptions> datasets){
		Map<CoreColumn, PublishOptions> optionsPerColumn = new HashMap<>();
		Set<CoreColumn> toExportAsHierarchies = new HashSet<>();
		for (CoreDataset d : datasets.keys()) {
			for (CoreColumn c : d.getColumns()) {
				optionsPerColumn.put(c, datasets.get(d));
				if (c.getPublicationMetadata().isHierarchy()) {
					toExportAsHierarchies.add(c);
				}
			}
		}

		// Iterate over the columns to export to identify the ones we don't need to export.
		Set<CoreColumn> unncessaryColumnsToExport = new HashSet<>();
		for (CoreColumn toExportAsHierarchy : toExportAsHierarchies) {
			HierarchyCustomizerOperator op = new HierarchyCustomizerOperatorRetriever().visit(toExportAsHierarchy);
			if (op != null) {
				CoreColumn operand = op.getOperand();
				for (CoreColumn columnToExport : toExportAsHierarchies) {
					/*
					 * Find in the column to export a column that comes from the same dataset and that has the
					 * same name than the operand of the operator. If one is found, it means the column 'c'
					 * is used to customize the hierarchy represented by 'columnToExport'. In that case, we
					 * can remove 'columnToExport' from set of columns to export because having 'c' is enough.
					 */
					if (columnToExport.getDataset() == operand.getDataset() && operand.name().equals(columnToExport.name())) {
						unncessaryColumnsToExport.add(columnToExport);
						break;
					}
				}
			}
		}
		toExportAsHierarchies.removeAll(unncessaryColumnsToExport);

		Map<CoreColumn, PublishOptions> optionsPerColumnToExport = new HashMap<>();
		for (Entry<CoreDataset, PublishOptions> e : datasets.entrySet()) {
			CoreDataset d = e.getKey();
			PublishOptions options = e.getValue();
			for (CoreColumn coreColumn : d.getColumnsExportableAsMeasures()) {
				optionsPerColumnToExport.put(coreColumn, options);
			}
		}

		for (CoreColumn toExportAsHierarchy : toExportAsHierarchies) {
			optionsPerColumnToExport.put(toExportAsHierarchy, optionsPerColumn.get(toExportAsHierarchy));
		}

		return ImmutableHashMap.<CoreColumn, PublishOptions>of(optionsPerColumnToExport);
	}

	@Override
	public int size() {
		return datasets.size();
	}

	@Override
	public CubeBuildFoundry addDataset(CoreDataset dataset, String name, PublishOptions options) {
		return new CastingCubeBuildFoundry(
			this.datasets.put(name, dataset),
			this.publishingOptions.put(name, options),
			this.datastoreDescription,
			this.selection,
			this.cubeDescription);
	}

	@Override
	public CubeBuildFoundry removeDataset(String name) {
		return new CastingCubeBuildFoundry(datasets.remove(name), publishingOptions.remove(name), datastoreDescription, selection, cubeDescription);
	}

	@Override
	public CubeBuildFoundry showStructure(String name, boolean blocking, JungDatasetPrintOptions options) {
		new JungDatasetPrinter(
				blocking,
				caster.datastoreSchema,
				caster.inputSelection,
				caster.inputCubeDescription)
			.print(name, this.datasets.values(), options);
		return this;
	}

	@Override
	public AlloyGraph graph() {
		getCastResult(); // call it to initialize the cast result
		return caster.graph;
	}

	@Override
	public ImmutableMap<CoreColumn, SolidAlloy> getCastResult() {
		if (this.castResult == null) {
			this.castResult = caster.cast();
		}
		return this.castResult;
	}

	/**
	 * Returns the {@link MeasureNameProvider} of the {@link Caster}
	 *
	 * @return the {@link MeasureNameProvider} of the {@link Caster}
	 */
	public MeasureNameProvider getMeasureNameProvider() {
		return this.caster.measureNameProvider;
	}

	/**
	 * Throw exception indicating that there are columns defining two different measures with the
	 * same name.
	 *
	 * @param name The name of the measure.
	 * @param first The first column.
	 * @param second The second column.
	 * @param firstDescription The measure description in the first column.
	 * @param secondDescription The measure description in the second column.
	 */
	protected void onColumnConflict(
			String name,
			CoreColumn first,
			CoreColumn second,
			IMeasureMemberDescription firstDescription,
			IMeasureMemberDescription secondDescription) {
		String explanation = "They produce respectively: "
			+ System.lineSeparator() + "- Description " + firstDescription
			+ System.lineSeparator() + "- Description " + secondDescription;
		if (firstDescription.getClass() != secondDescription.getClass()) {
			explanation = "The first ones produces a " + firstDescription.getClass().getName() + " whereas the second one produces a " + secondDescription.getClass().getName() + ".";
		}
		throw new UserErrorException(
			"Two columns try to create two different measures with the same name: " + name + ": "
					+ System.lineSeparator() + "- Column " + first
					+ System.lineSeparator() + "- Column " + second + "."
					+ System.lineSeparator() + explanation);
	}

	/**
	 * Tests that two measures are equal if we ignore their parameters that have no effect when they are hidden.
	 *
	 * @param first The first comparison operand. Should describe a hidden measure.
	 * @param second The second comparison operand. Should describe a hidden measure.
	 *
	 * @return If there are no differences between the two measures if we ignore their parameters that have no effect
	 * when they are hidden.
	 */
	protected boolean areEqualWhenIgnoringUselessParametersOnHiddenMeasure(
			IMeasureMemberDescription first,
			IMeasureMemberDescription second) {
		assert !first.isVisible();
		assert !second.isVisible();
		// Remember the ignored fields before switching them
		String previousFormatter = first.getFormatter();
		String previousFolder = first.getFolder();
		String previousGroup = first.getGroup();
		try {
			// Temporarily change the ignored fields
			first.setFormatter(second.getFormatter());
			first.setFolder(second.getFolder());
			first.setGroup(second.getGroup());
			return first.equals(second);
		} finally {
			first.setFolder(previousFolder);
			first.setFormatter(previousFormatter);
			first.setGroup(previousGroup);
		}
	}

	/**
	 * Tests that two measure descriptions are equal, ignoring the {@link IMeasureMemberDescription#isVisible()} flag
	 * and all other options that are not read when a measure is not visible (formatter, folder and measure group).
	 *
	 * @param first The first comparison operand.
	 * @param second The second comparison operand.
	 *
	 * @return True if the descriptions are equal for each attribute except their visibility.
	 */
	protected boolean areEqualIgnoringVisibility(IMeasureMemberDescription first, IMeasureMemberDescription second) {
		// If the two descriptions are completely equal this is simple.
		if (first.equals(second)) {
			return true;
		}
		// If the two descriptions are different and are both visible the difference lies elsewhere so
		// they are not equal.
		if (first.isVisible() && second.isVisible()) {
			return false;
		}
		if (!first.isVisible() && !second.isVisible()) {
			return areEqualWhenIgnoringUselessParametersOnHiddenMeasure(first, second);
		}
		// At this point one and only one of the descriptions is hidden, retrieve it.
		IMeasureMemberDescription hidden = first.isVisible() ? second : first;
		IMeasureMemberDescription visible = first.isVisible() ? first : second;
		// Remember the values before switching them
		String previousFormatter = hidden.getFormatter();
		String previousFolder = hidden.getFolder();
		String previousGroup = hidden.getGroup();
		// Temporarily switch its visibility and optional attributes to be the same than the other description.
		hidden.setVisible(true);
		hidden.setFormatter(visible.getFormatter());
		hidden.setFolder(visible.getFolder());
		hidden.setGroup(visible.getGroup());
		try {
			return first.equals(second);
		} finally {
			// Rollback the changes.
			hidden.setVisible(false);
			hidden.setFormatter(previousFormatter);
			hidden.setFolder(previousFolder);
			hidden.setGroup(previousGroup);
		}
	}

	/**
	 * Produces the {@link IMeasureMemberDescription description} for the measures defined by our datasets
	 * and checks for naming conflicts.
	 * @return the list of {@link IMeasureMemberDescription descriptions} for the added measures.
	 */
	protected ImmutableList<IMeasureMemberDescription> createAddedMeasuresAndCheckConflicts() {
		Map<String, IMeasureMemberDescription> createdMeasureByName = new HashMap<>();
		Map<String, CoreColumn> creatingColumnByMeasureName = new HashMap<>();
		MeasureNameProvider nameProvider = caster.measureNameProvider;

		getCastResult().filterByValueClass(MeasureAlloy.class).entrySet().forEach(e -> {
			MeasureAlloy ma = e.getValue();
			CoreColumn c = e.getKey();

			// Ignore the alloys that should not actually create measures
			if (!ma.shouldCreateMeasure()) {
				return;
			}

			PublicationMetadata publicationMetadata = c.getPublicationMetadata();

			// List of measures created by the alloy sa. It may also include its dependencies
			List<IMeasureMemberDescription> createdMeasures = new ArrayList<>();

			IMeasureMemberDescription m = ma.toMeasureDescription(nameProvider);
			createdMeasures.add(addPublicationMetadata(m, publicationMetadata));

			handleAdditionalMeasureAlloysRecursively(ma, publicationMetadata, createdMeasures);

			// Log what is going to be published into the cube
			logPublishMeasuresToPublish(c, createdMeasures);

			createdMeasures.forEach(
				measureDesc -> {
					String name = measureDesc.getName();
					IMeasureMemberDescription previous = createdMeasureByName.get(name);
					if (previous == null) {
						createdMeasureByName.put(name, measureDesc);
						creatingColumnByMeasureName.put(name, c);
					} else if (areEqualIgnoringVisibility(measureDesc, previous)) {
						if (measureDesc.isVisible()) {
							// We expose the same measure than a previous column but as visible whereas the previously known
							// measure was hidden, update the created measures.
							createdMeasureByName.put(name, measureDesc);
							creatingColumnByMeasureName.put(name, c);
						}
						// Id measureDesc is not visible we are recreating a hidden column whereas the visible version
						// already exists. Keep the visible one.
					} else {
						onColumnConflict(name, creatingColumnByMeasureName.get(name), c, previous, measureDesc);
					}
				});
		});
		return Immutable.map(createdMeasureByName).values().toList();
	}

	/**
	 * Logs the measures to be published by the transformation of the given column into ActivePivot calculations.
	 *
	 * @param c the column whose calculations are the measures given as input
	 * @param createdMeasures the calculations associated to the given column
	 */
	protected void logPublishMeasuresToPublish(CoreColumn c, List<IMeasureMemberDescription> createdMeasures) {
		PublishOptions publishOptions = caster.toExport.get(c).publishOptions;
		if(publishOptions.log) {
			final StringBuilder sb = new StringBuilder();
			sb.append("The publication of the column '")
					.append(c.name())
					.append("' leads to the creation of the following measures:")
					.append(System.lineSeparator());

			for (int i = 0; i < createdMeasures.size(); i++) {
				// indent
				for (int j = 0; j < 2; j++) {
					sb.append(' ');
				}
				sb.append(createdMeasures.get(i));
				if(i < createdMeasures.size() - 1) {
					sb.append(System.lineSeparator());
				}
			}
			LOGGER.log(Level.INFO, sb.toString());
		}
	}

	/**
	 * Handles the additional measure alloys of {@code ma} if any by creating a dedicated measure description for each
	 * of its measure alloys. The created measures will be hidden and the given {@link PublicationMetadata} will be
	 * applied to it.
	 * <p>
	 * If one of its additional alloys also provides additional alloys they are handled the same way recursively.
	 *
	 * @param ma (IN) the measure alloy for which the additional alloys are handled
	 * @param publicationMetadata (IN) the metadata of the measure descriptions to apply
	 * @param createdMeasures (OUT) the list of created measures
	 */
	protected void handleAdditionalMeasureAlloysRecursively(
			MeasureAlloy ma,
			PublicationMetadata publicationMetadata,
			List<IMeasureMemberDescription> createdMeasures) {
		MeasureNameProvider nameProvider = caster.measureNameProvider;
		for (MeasureAlloy additional : ma.getAdditionalMeasureAlloys()) {
			// Create the measure and hide it
			IMeasureMemberDescription dependency = HiddenMeasureAlloy.create(additional, nameProvider);
			createdMeasures.add(addPublicationMetadata(dependency, publicationMetadata));
			handleAdditionalMeasureAlloysRecursively(additional, publicationMetadata, createdMeasures);
		}
	}

	/**
	 * Throws exception indicating that an added measure defines by the calculation is overriding a measure from the description
	 * of the cube.
	 * @param addedMeasure The description of the measure.
	 * @param measureFromCube The description of the postprocessor.
	 */
	protected static void onOverridingMeasureFromCubeDescription(
			IMeasureMemberDescription addedMeasure,
			IMeasureMemberDescription measureFromCube) {
		throw new UserErrorException("The calculation you describe would create a measure with the same name than "
				+ "an existing measure, choose another name for your description by using .as()."
				+ " The existing measure is " + measureFromCube + " and the measure defined by your calculation is "
				+ addedMeasure);
	}

	/**
	 * Merges the measure descriptions from the cube description and the calculation and checks for naming conflicts.
	 *
	 * @param nativeMeasures The native measures from the cube description.
	 * @param aggregatedMeasures The aggregated measures from the cube description.
	 * @param postProcessors The postprocessors from the cube description.
	 * @param addedMeasuresDescription The descriptions from the calculation.
	 * @return the descriptions of measures that will be available on the cube.
	 */
	public static ImmutableList<IMeasureMemberDescription> mergeMeasureDescriptionAndCheckConflicts(
			ImmutableList<INativeMeasureDescription> nativeMeasures,
			ImmutableList<IAggregatedMeasureDescription> aggregatedMeasures,
			ImmutableList<IPostProcessorDescription> postProcessors,
			ImmutableList<IMeasureMemberDescription> addedMeasuresDescription) {
		final Map<String, IMeasureMemberDescription> measureByName = new HashMap<>();
		postProcessors.forEach(pp -> {
			final String name = pp.getName();// the list of legacy post-processors should never have duplicates
			assert measureByName.get(name) == null : "The postprocessor " + name + " is declared twice.";
			measureByName.put(name, pp);
		});

		nativeMeasures.forEach(nm -> measureByName.put(nm.getName(), nm));
		aggregatedMeasures.forEach(am -> measureByName.put(am.getName(), am));

		addedMeasuresDescription.forEach(measureDesc -> {
			final String name = measureDesc.getName();
			final IMeasureMemberDescription previous = measureByName.get(name);
			if (previous == null || measureDesc.equals(previous)) {
				measureByName.put(name, measureDesc);
			} else {
				onOverridingMeasureFromCubeDescription(measureDesc, previous);
			}
		});

		return Immutable.map(measureByName).values().toList();
	}

	/**
	 * Merges the hierarchy descriptions from the cube description and the hierarchies brought by CoPPer.
	 *
	 * @param dimensions The dimensions from the cube description
	 * @param addedDimensions The dimensions from the calculation
	 * @return the descriptions of dimensions that will be available on the cube.
	 */
	public static List<IAxisDimensionDescription> mergeHierarchyDescriptionsAndCheckConflicts(
			ImmutableList<IAxisDimensionDescription> dimensions,
			ImmutableList<IAxisDimensionDescription> addedDimensions) {
		Map<String, List<IAxisHierarchyDescription>> hierarchiesToAdd = new HashMap<>();
		for (IAxisDimensionDescription d : addedDimensions) {
			hierarchiesToAdd.put(d.getName(), d.getHierarchies());
		}

		List<IAxisDimensionDescription> newDimensions = new ArrayList<>();
		for(IAxisDimensionDescription d : dimensions) {
			Map<String, IAxisHierarchyDescription> currentHierarchies = d.getHierarchies().stream().collect(Collectors.toMap(IAxisHierarchyDescription::getName, Function.identity()));
			List<IAxisHierarchyDescription> toAdd = hierarchiesToAdd.get(d.getName());
			if(toAdd != null) {
				for(IAxisHierarchyDescription h : toAdd) {
					//  Check that all hierarchies we want to add do not already exist.
					if(currentHierarchies.containsKey(h.getName())) {
						throw new UserErrorException("The calculation you describe would create a hierarchy with the same name than "
								+ "an existing one, choose another name for your description by using .asHierarchy(String)."
								+ " The existing hierarchy is " + currentHierarchies.get(h.getName()) + " and the hierarchy defined by your calculation is "
								+ h);
					}
				}
				newDimensions.add(new AxisDimensionDescription(d.getName(), ImmutableArrayList.of(d.getHierarchies()).addAll(toAdd).toList()));
				hierarchiesToAdd.remove(d.getName());
			} else {
				newDimensions.add(d);
			}
		}

		// Add the remaining
		for (Entry<String, List<IAxisHierarchyDescription>> toAdd : hierarchiesToAdd.entrySet()) {
			newDimensions.add(new AxisDimensionDescription(toAdd.getKey(), toAdd.getValue()));
		}

		return newDimensions;
	}

	/**
	 * Mutate a measure description to take into account {@link PublicationMetadata publication metadata}.
	 *
	 * @param measureDescriptionWithoutMetadata The measure description to update in-place.
	 * @param publicationMetadata The metadata containing multiple attributes to apply to the description.
	 *
	 * @return The mutated description.
	 */
	protected IMeasureMemberDescription addPublicationMetadata(
			IMeasureMemberDescription measureDescriptionWithoutMetadata,
			PublicationMetadata publicationMetadata) {// We mutate because we can't clone

		IMeasureMemberDescription result = measureDescriptionWithoutMetadata;
		if (publicationMetadata.getFolder() != null) {
			result.setFolder(publicationMetadata.getFolder());
		}
		ImmutableList<String> groups = publicationMetadata.getGroups();
		if (groups != null && groups.isNotEmpty()) {
			if(groups.size() > 1) {
				throw new UserErrorException("The measure " + measureDescriptionWithoutMetadata.getName()
						+ " cannot belong to several measure groups: " + groups);
			}
			result.setGroup(groups.first());
		}
		if (publicationMetadata.getFormatter() != null) {
			result.setFormatter(publicationMetadata.getFormatter());
		}
		return result;
	}

	@Override
	public ImmutableList<IMeasureMemberDescription> getAddedMeasuresDescriptions() {
		if (addedMeasures == null) {
			checkExportedMeasuresAreFullyAggregated();
			ImmutableList<IMeasureMemberDescription> all = createAddedMeasuresAndCheckConflicts();
			// Exclude the joined measures. They must be added to the cube description via the
			// IAnalysisAggregationProcedureDescription
			addedMeasures = all.filter(m -> !(m instanceof IJoinMeasureDescription));
			addedJoinMeasures = all.filterByClass(IJoinMeasureDescription.class);
		}
		return addedMeasures;
	}

	/**
	 * Checks that all {@link SolidAlloy} from the cast result {@link #getCastResult()} are fully aggregated.
	 */
	protected void checkExportedMeasuresAreFullyAggregated() {
		getCastResult().filterByValueClass(MeasureAlloy.class).entrySet().forEach(e -> {
			MeasureAlloy sa = e.getValue();
			CoreColumn c = e.getKey();
			CastOptions options = caster.getToExport().get(c).castOptions;

			if (sa.isOnlyLeafAggregated() && !options.allowLeafAggregation) {
				// The calculation makes sense only at some levels. The user has to decide what to do when these levels
				// are not expressed in a query.

				// Build error message
				LiquidAlloy liquidAlloy = caster.graph.getLiquidAlloy(c);
				CreatingSourceCodeInfo sourceCode = liquidAlloy.accept(new LiquidAlloyVisitor<CreatingSourceCodeInfo>() {
					@Override
					public CreatingSourceCodeInfo visit(OperatorAlloy alloy) {
						return alloy.operator.getCallInfo();
					}

					@Override
					public CreatingSourceCodeInfo visit(StoreFieldAlloy alloy) {
						// should never happen
						throw new IllegalStateException(c + " | " + alloy);
					}
				});

				int size = sa.requiredLevels().size();
				String message = String.format(
						size == 1 ? UNFINISHED_CALCULATION_MESSAGE_SINGLE_LEVEL : UNFINISHED_CALCULATION_MESSAGE_SEVERAL_LEVELS,
						String.valueOf(sa.requiredLevels().map(LevelCoordinate::toDescription).join(IPostProcessor.SEPARATOR)));

				throw new InvalidCalculationException(message, sourceCode);
			}
		});
	}

	/**
	 * Extract from our cast result the analysis hierarchies descriptions that can directly be used to create an
	 * {@link IActivePivot}.
	 *
	 * @return The analysis hierarchies descriptions.
	 */
	protected ImmutableList<IAxisDimensionDescription> getAddedAnalysisHierarchiesInDimension() {
		if(this.hierarchiesWithinDimensions == null) {
			Map<String, List<IAxisHierarchyDescription>> analysisHierarchiesPerDimension = this.getAddedAnalysisHierarchies();
			if (analysisHierarchiesPerDimension.isEmpty()) {
				return Empty.list();
			}

			List<IAxisDimensionDescription> dimensions = new ArrayList<>();
			for (Entry<String, List<IAxisHierarchyDescription>> e : analysisHierarchiesPerDimension.entrySet()) {
				dimensions.add(new AxisDimensionDescription(e.getKey(), e.getValue()));
			}
			this.hierarchiesWithinDimensions = Immutable.list(dimensions);
		}
		return this.hierarchiesWithinDimensions;
	}

	/**
	 * Merge two analysis hierarchy descriptions that have the same origin but one of them can be AllMemberEnabled
	 * because it was built by a later column knowing how to aggregate up to the ALL level.
	 *
	 * @param left The first description to merge.
	 * @param right The second description to merge.
	 *
	 * @return The merged description.
	 */
	protected static TrivialAnalysisHierarchyDescription merge(
			TrivialAnalysisHierarchyDescription left,
			TrivialAnalysisHierarchyDescription right) {
		if (right.isAllMemberEnabled()) {
			return right;
		}
		return left;
	}

	@Override
	public Map<String, List<IAxisHierarchyDescription>> getAddedAnalysisHierarchies() {
		// Store in a set to merge identical definitions.
		List<TrivialAnalysisHierarchyDescription> definitions = new ArrayList<>();
		getCastResult().filterByValueClass(HierarchyAlloy.class).entrySet().forEach(e -> {
			HierarchyAlloy ha = e.getValue();
			TrivialAnalysisHierarchyDescription addCandidate = ha.toHierarchyDescription();
			Optional<TrivialAnalysisHierarchyDescription> previous = Immutable.list(definitions).find(existing -> existing.getLevel().equals(addCandidate.getLevel()));
			if (previous.isPresent()) {
				definitions.remove(previous.get());
				definitions.add(merge(previous.get(), addCandidate));
			} else {
				definitions.add(addCandidate);
			}
		});

		// Index by dimension
		Map<String, List<IAxisHierarchyDescription>> m = new HashMap<>();
		for (TrivialAnalysisHierarchyDescription trivialAnalysisHierarchyDescription : definitions) {
			m.computeIfAbsent(trivialAnalysisHierarchyDescription.toCoordinate().dimension, k -> new ArrayList<>())
				.add(trivialAnalysisHierarchyDescription.toHierarchyDescription());
		}

		// Lexical order per dimension
		for (List<IAxisHierarchyDescription> h : m.values()) {
			Collections.sort(h, Comparator.comparing(IAxisHierarchyDescription::getName));
		}

		return m;
	}

	@Override
	public ImmutableList<IAnalysisAggregationProcedureDescription> getAddedAnalysisAggregationProcedureDescriptions() {
		if(procedures == null) {
			procedures = collectAndMergeAggregationProcedureDescriptions();
		}
		return procedures;
	}

	/**
	 * Collects the procedures created columns that create {@link AnalysisAggregationProcedureAlloy}. It makes sure that
	 * two procedures that are not the same do not handled the same hierarchies.
	 *
	 * @return the collected procedures
	 */
	protected ImmutableList<IAnalysisAggregationProcedureDescription> collectAndMergeAggregationProcedureDescriptions() {
		// Collect all join measures for each store
		Map<String, Set<IJoinMeasureDescription>> joinMeasuresByStore = new HashMap<>();
		for (IJoinMeasureDescription m : this.addedJoinMeasures) {
			joinMeasuresByStore.computeIfAbsent(m.getStore(), __ -> new HashSet<>()).add(m);
		}

		// Collection all procedures for each plugin key
		Map<String, Set<IAnalysisAggregationProcedureDescription>> proceduresByPluginKey = new HashMap<>();
		for(AnalysisAggregationProcedureAlloy sa : getCastResult().filterByValueClass(AnalysisAggregationProcedureAlloy.class).values()) {
			IAnalysisAggregationProcedureDescription procedure = sa.toProcedureDescription();
			proceduresByPluginKey.computeIfAbsent(procedure.getPluginKey(), __ -> new HashSet<>()).add(procedure);
		}

		// The attributes of the description of a JoinAnalysisAggregationProcedure need to be merged (hierarchies and
		// underlyingLevels) and the join measures need "injected"
		if (proceduresByPluginKey.containsKey(JoinAnalysisAggregationProcedure.PLUGIN_KEY)) {
			Map<String, Set<IAnalysisAggregationProcedureDescription>> joinProceduresByStore = new HashMap<>();
			for (IAnalysisAggregationProcedureDescription procedure : proceduresByPluginKey.get(JoinAnalysisAggregationProcedure.PLUGIN_KEY)) {
				String store = procedure.getProperties().getProperty(JoinAnalysisAggregationProcedure.STORE);
				joinProceduresByStore.computeIfAbsent(store, __ -> new HashSet<>()).add(procedure);
			}

			Map<String, IAnalysisAggregationProcedureDescription> newJoinProcedureByStore = new HashMap<>();
			for (Entry<String, Set<IAnalysisAggregationProcedureDescription>> joinProceduresByStoreEntry : joinProceduresByStore.entrySet()) {
				IAnalysisAggregationProcedureDescription mergeProcedure = null;
				for (IAnalysisAggregationProcedureDescription procedure : joinProceduresByStoreEntry.getValue()) {
					if (mergeProcedure == null) {
						mergeProcedure = procedure;
						continue;
					}
					Map<LevelCoordinate, Object> modifiedHierarchies = new ConcurrentHashMap<>();
					// Make sure the properties are the same. We don't support other cases for the time being.
					Properties properties = new Properties();
					properties.putAll(mergeProcedure.getProperties());
					procedure.getProperties().entrySet().forEach(e -> {
						Object previous = properties.put(e.getKey(), e.getValue());
						if (previous != null && !previous.equals(e.getValue())) {
							if (e.getKey() == JoinAnalysisAggregationProcedure.MAPPING && previous instanceof HashMap<?, ?>) {
								((HashMap<LevelCoordinate, Object>) e.getValue()).forEach((k,v)->{
									if (k.level != k.parent.hierarchy || k.level !=k.parent.dimension )
									{
										modifiedHierarchies.put(k,v);
									}
								});
								((HashMap<LevelCoordinate, Object>) previous).forEach((k,v)->{
									if (k.level != k.parent.hierarchy || k.level !=k.parent.dimension )
									{
										modifiedHierarchies.put(k,v);
									}
								});

							} else {
								throw new QuartetRuntimeException("Trying to replace the value '" + previous + "' by '"
										+ e.getValue() + "' for the key '" + e.getKey() + "'");
							}
						}
					});

					@SuppressWarnings("unchecked")
					HashMap<LevelCoordinate, Object> m = (HashMap<LevelCoordinate, Object>) properties
							.get(JoinAnalysisAggregationProcedure.MAPPING);
					HashMap<LevelCoordinate, Object> markedForDeletion = new HashMap<>();
					m.forEach((lvl, colname) -> {
						if (modifiedHierarchies.containsValue(colname)) {
							markedForDeletion.put(lvl, colname);
						}
					});
					markedForDeletion.forEach((k, v) -> {
						m.remove(k);
					});
					m.putAll(modifiedHierarchies);
					properties.put(JoinAnalysisAggregationProcedure.MAPPING, m);

					mergeProcedure = new AnalysisAggregationProcedureDescription(
							mergeProcedure.getPluginKey(),
							Empty.<String>set().addAll(mergeProcedure.getHandledHierarchies())
									.addAll(procedure.getHandledHierarchies()).toCollection(),
							Empty.<String>set().addAll(mergeProcedure.getUnderlyingLevels())
									.addAll(procedure.getUnderlyingLevels()).toCollection(),
							Empty.<IJoinMeasureDescription>set().addAll(mergeProcedure.getJoinMeasures())
									.addAll(procedure.getJoinMeasures()).toCollection(),
							properties) ;
				}

				String store = joinProceduresByStoreEntry.getKey();
				Set<IJoinMeasureDescription> joinMeasures = joinMeasuresByStore.get(store);
				if (joinMeasures != null && !joinMeasures.isEmpty()) {
					mergeProcedure = new AnalysisAggregationProcedureDescription(
							mergeProcedure.getPluginKey(),
							mergeProcedure.getHandledHierarchies(),
							mergeProcedure.getUnderlyingLevels(),
							Empty.<IJoinMeasureDescription>set().addAll(mergeProcedure.getJoinMeasures())
									.addAll(joinMeasures).toSet(),
							mergeProcedure.getProperties());
				}
				newJoinProcedureByStore.put(store, mergeProcedure);
			}

			proceduresByPluginKey.put(JoinAnalysisAggregationProcedure.PLUGIN_KEY, new HashSet<>(newJoinProcedureByStore.values()));
		}

		// Finally, checks a hierarchy is handled by only one procedure
		List<IAnalysisAggregationProcedureDescription> hierarchylessProcedures = new ArrayList<>();
		Map<String, IAnalysisAggregationProcedureDescription> procedureByHierarchy = new HashMap<>();

		List<IAnalysisAggregationProcedureDescription> proceduresToCheck = proceduresByPluginKey.entrySet().stream()
				.flatMap(e -> e.getValue().stream())
				.collect(Collectors.toList());

		for (IAnalysisAggregationProcedureDescription procedureToCheck : proceduresToCheck) {
			if (procedureToCheck.getHandledHierarchies().isEmpty()) {
				hierarchylessProcedures.add(procedureToCheck);
				continue;
			}

			for (final String hier : procedureToCheck.getHandledHierarchies()) {
				IAnalysisAggregationProcedureDescription procedure = procedureByHierarchy.get(hier);
				if (procedure != null) {
					throw new QuartetRuntimeException("The hierarchy '" + hier + "' is already handled by this procedure "
							+ procedure + " and cannot also be handled by " + procedureToCheck);
				}

				procedureByHierarchy.put(hier, procedureToCheck);
			}
		};

		return ImmutableArrayList.of(new HashSet<>(procedureByHierarchy.values())).addAll(hierarchylessProcedures);
	}

	/**
	 * Write into a measures description the measures created by a cast.
	 *
	 * @param castResult The result to write into the description.
	 * @param descriptionToPopulate The description to mutate.
	 */
	public static void injectCastResult(
			ImmutableList<IMeasureMemberDescription> castResult,
			MeasuresDescription descriptionToPopulate) {
		descriptionToPopulate.setNativeMeasures(castResult.filterByClass(INativeMeasureDescription.class).toList());
		descriptionToPopulate.setAggregatedMeasuresDescription(
			castResult.filterByClass(IAggregatedMeasureDescription.class).toList());
		descriptionToPopulate.setPostProcessorsDescription(
			castResult.filterByClass(IPostProcessorDescription.class).toList());
	}

	/**
	 * Converts a cast result to a measures descriptions that we can directly build an {@link IActivePivot} from.
	 *
	 * @param castResult The result to translate.
	 *
	 * @return The measures description.
	 */
	protected static MeasuresDescription toMeasuresDescription(ImmutableList<IMeasureMemberDescription> castResult) {
		MeasuresDescription description = new MeasuresDescription();
		injectCastResult(castResult, description);
		return description;
	}

	/**
	 * Finds the description of the schema containing a cube by the cube's name.
	 *
	 * @param manager The manager containing the cube to search for.
	 * @param cubeName The name of the cube to search for.
	 *
	 * @return The description of the schema containing the searched cube.
	 *
	 * @throws IllegalArgumentException If no schema contains the given cube.
	 */
	public static IActivePivotSchemaDescription findSchemaContainingCube(
			final IActivePivotManagerDescription manager,
			final String cubeName) throws IllegalArgumentException {
		return Immutable
				.list(manager.getSchemas())
				.find(instanceDesc -> SimpleManagerDescriptionBuilder.getSchemaContainedCubes(instanceDesc).contains(cubeName))
				.orElseThrow(() -> onUnknownCubeName(manager, cubeName))
				.getActivePivotSchemaDescription();
	}

	/**
	 * Creates an error message indicating that there is no cube of the given in the manager description.
	 *
	 * @param manager The manager description.
	 * @param cubeName The name of the unknown cube.
	 * @return the error message indicating that there is no cube of the given in the manager description.
	 */
	protected static IllegalArgumentException onUnknownCubeName(
			final IActivePivotManagerDescription manager,
			final String cubeName) {
		final ImmutableSet<String> allCubeNames = Immutable.list(manager.getSchemas())
				.flatMapSet(SimpleManagerDescriptionBuilder::getSchemaContainedCubes);
		return new IllegalArgumentException(
				"There is no cube called '" + cubeName + "' in the provided manager. Existing cubes are: " + allCubeNames.join(","));
	}

	/**
	 * Update a manager description to replace the description of a given cube.
	 *
	 * @param original The description to change.
	 * @param cubeName The name of the cube whose description should be replaced.
	 * @param pivotDescription The new description to use for the given cube.
	 *
	 * @throws IllegalArgumentException If no schema contains the given cube.
	 */
	protected static void replacePivotDescription(
			IActivePivotManagerDescription original,
			String cubeName,
			IActivePivotDescription pivotDescription) throws IllegalArgumentException {
		// TODO handle invalid cube name

		IActivePivotSchemaDescription schema = findSchemaContainingCube(original, cubeName);
		schema.setActivePivotInstanceDescriptions(
				Immutable
				.list(schema.getActivePivotInstanceDescriptions())
				.map(instanceDesc -> {
					if (instanceDesc.getId().equals(cubeName)) {
						return new ActivePivotInstanceDescription(cubeName, pivotDescription);
					} else {
						return instanceDesc;
					}
				})
				.toList());
	}

	/**
	 * Builds the final cube description by merging the descriptions from the calculation with those from the cube's
	 * description.
	 *
	 * @param cubeDescription The cube description from user.
	 * @param addedMeasures The measures added by the calculation.
	 * @param addedAnalysisHierarchiesInDimension The analysis hierarchies added by the calculation.
	 * @param addedAnalysisAggregationProcedureDescriptions The procedures added by the calculation.
	 *
	 * @return the {@link IActivePivotDescription pivot} description.
	 */
	public static IActivePivotDescription buildCubeDescription(
			CubeDescriptionWithoutCopperMeasures cubeDescription,
			ImmutableList<IMeasureMemberDescription> addedMeasures,
			ImmutableList<IAxisDimensionDescription> addedAnalysisHierarchiesInDimension,
			ImmutableList<IAnalysisAggregationProcedureDescription> addedAnalysisAggregationProcedureDescriptions) {
		return buildCubeDescription(cubeDescription, addedMeasures, addedAnalysisHierarchiesInDimension, addedAnalysisAggregationProcedureDescriptions, false);
	}

	/**
	 * Builds the final cube description by merging the descriptions from the calculation with those from the cube's
	 * description.
	 *
	 * @param cubeDescription The cube description from user.
	 * @param addedMeasures The measures added by the calculation.
	 * @param addedAnalysisHierarchiesInDimension The analysis hierarchies added by the calculation.
	 * @param addedAnalysisAggregationProcedureDescriptions The procedures added by the calculation.
	 * @param overrideForTests true if the input cube description will be overridden for tests, in which case,
	 *                         {@link #overrideForTests(IActivePivotDescription)} is called to override
	 *                         the result cube description with test configuration.
	 *
	 * @return the {@link IActivePivotDescription pivot} description.
	 */
	public static IActivePivotDescription buildCubeDescription(
			final CubeDescriptionWithoutCopperMeasures cubeDescription,
			final ImmutableList<IMeasureMemberDescription> addedMeasures,
			final ImmutableList<IAxisDimensionDescription> addedAnalysisHierarchiesInDimension,
			final ImmutableList<IAnalysisAggregationProcedureDescription> addedAnalysisAggregationProcedureDescriptions,
			final boolean overrideForTests) {
		final IActivePivotDescription apd = cubeDescription.toDescription().clone();

		// Enrich the description with analysis hierarchies added by Copper.
		apd.setAxisDimensions(
				new AxisDimensionsDescription(mergeHierarchyDescriptionsAndCheckConflicts(
						cubeDescription.getDimensions(),
						addedAnalysisHierarchiesInDimension)));

		// Enrich the description with measures added by Copper
		apd.setMeasuresDescription(
				toMeasuresDescription(
						mergeMeasureDescriptionAndCheckConflicts(
								cubeDescription.getNativeMeasures(),
								cubeDescription.getAggregatedMeasures(),
								cubeDescription.getPostProcessors(),
								addedMeasures)));

		ImmutableList<IAnalysisAggregationProcedureDescription> newProcedures = addedAnalysisAggregationProcedureDescriptions;
		if(apd.getAggregationProcedures() != null) {
			newProcedures = newProcedures.addAll(apd.getAggregationProcedures());
		}
		apd.setAggregationProcedures(newProcedures.toList());

		return (overrideForTests) ? overrideForTests(apd) : apd;
	}

	/**
	 * Overrides a cube description for testing purposes:
	 * <ul>
	 *     <li>Uses JIT</li>
	 *     <li>auto factless feature is enabled.</li>
	 *     <li>No aggregates cache.</li>
	 *     <li>No epoch dimension.</li>
	 *     <li>No shared context but only queries time limit set to -1.</li>
	 * </ul>
	 * @param apd The description to override.
	 * @return the overridden description.
	 */
	protected static IActivePivotDescription overrideForTests(final IActivePivotDescription apd) {
		apd.setAggregateProvider(new AggregateProviderDefinition(JustInTimeAggregateProviderBase.PLUGIN_TYPE));
		apd.setAutoFactlessHierarchies(true);
		apd.setAggregatesCacheDescription(new AggregatesCacheDescription(-1, true, new String[] {}, new String[] {}));
		apd.setEpochDimensionDescription(new EpochDimensionDescription(false, false));
		apd.setSharedContexts(new ContextValuesDescription(Collections.singletonList(QueriesTimeLimit.of(-1, TimeUnit.SECONDS))));
		return apd;
	}

	@Override
	public IActivePivotDescription buildCubeDescription(BuildOptions options) {
		if (options.printFullGraph) {
			new JungDatasetPrinter(
					false, // don't block the project start so that the user can use its cube and compare with the graph
					UserDatastoreSchemaDescription.createFromSchemaDescription(datastoreDescription),
					selection,
					cubeDescription)
			.print(
					"Measures",
					datasets.values().toList(),
					options.fullGraphPrintOptions);
		}
		return buildCubeDescription(
			cubeDescription,
			getAddedMeasuresDescriptions(),
			getAddedAnalysisHierarchiesInDimension(),
			getAddedAnalysisAggregationProcedureDescriptions());
	}

	@Override
	public Map<String, CoreColumn> getPublishedMeasureColumns() {
		final Map<String,CoreColumn> exportedColumns = new HashMap<>();
		for (final CoreDataset dataset: this.datasets.values()) {
			dataset.getColumnsExportableAsMeasures().forEach(c -> exportedColumns.put(c.name(), c));
		}
		return exportedColumns;
	}

}
