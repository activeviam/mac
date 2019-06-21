/*
 * (C) ActiveViam 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.copper.columns.impl;

import java.io.Serializable;

import com.activeviam.copper.builders.dataset.CoreCalculatedDataset;
import com.activeviam.copper.builders.dataset.CoreDataset;
import com.activeviam.copper.builders.dataset.CoreDatasetVisitor;
import com.activeviam.copper.builders.dataset.CoreStoreDataset;
import com.activeviam.copper.builders.dataset.impl.BaseCalculatedDataset;
import com.activeviam.copper.builders.dataset.impl.ConstantsDataset;
import com.activeviam.copper.builders.dataset.impl.ContextDataset;
import com.activeviam.copper.builders.dataset.impl.FactsDataset;
import com.activeviam.copper.builders.dataset.impl.LegacyPostProcessorsDataset;
import com.activeviam.copper.builders.dataset.impl.ReusePublishedMeasuresDataset;
import com.activeviam.copper.columns.Column;
import com.activeviam.copper.columns.ColumnResolver;
import com.activeviam.copper.columns.CoreColumn;
import com.activeviam.copper.columns.PublicationMetadata;
import com.activeviam.copper.columns.UnresolvedCoreColumn;
import com.activeviam.copper.exceptions.codeinfo.CreatingSourceCodeInfo;
import com.activeviam.copper.operator.column.values.HierarchyCustomizerOperator;
import com.activeviam.copper.operator.dataset.ColumnMapDatasetOperator;
import com.activeviam.copper.operator.dataset.DatasetOperator;
import com.activeviam.copper.operator.dataset.JoinDatasetOperator;
import com.activeviam.copper.operator.dataset.UnaryDatasetOperator;

/**
 *
 */
public class SimpleColumnResolver implements ColumnResolver, Serializable {

	private static final long serialVersionUID = 1L;
	/** The dataset owning the columns when resolving. */
	protected final CoreDataset dataset;
	/**
	 * Constructor.
	 *
	 * @param dataset The dataset owning the columns when resolving.
	 */
	public SimpleColumnResolver(CoreDataset dataset) {
		this.dataset = dataset;
	}

	/**
	 * Applies the default publication metadata coming from the dataset to a column of this dataset.
	 *
	 * @param userColumn The column to apply the default metadata to.
	 * @param defaults The default publication metadata coming from the dataset owning the column.
	 *
	 * @return The column having taken into account the default publication metadata.
	 */
	protected static CoreColumn applyDefaultPublicationMetadata(CoreColumn userColumn, PublicationMetadata defaults) {
		PublicationMetadata columnPublicationMetadata = userColumn.getPublicationMetadata();
		PublicationMetadata withDefaults = columnPublicationMetadata.applyDefaults(defaults);
		if (withDefaults == columnPublicationMetadata) {
			return userColumn;
		}
		return userColumn.withPublicationMetadata(withDefaults);
	}

	@Override
	public CoreColumn resolve(Column c) {
		CoreColumn resolved = ((UnresolvedCoreColumn) c).resolveDataset(dataset);
		if(resolved.isLeaf() && resolved.getPublicationMetadata().isHierarchy()) {
			// The resolved column is likely a NamedColumn and it should be used as a hierarchy.
			// The user probably wants to customize it (rename/comparator). In that case, create a dedicated calculated
			// column with the distinguishable operator.
			Boolean customizedColumnFromJoin = resolved.getDataset().accept(new CoreDatasetVisitor<Boolean>() {
				@Override
				public Boolean visit(CoreCalculatedDataset calculatedDataset) {
					// Make sure the column comes from a join and that it corresponds to a column
					// that will produced an
					// analysis hierarchy due to a missing level in the cube.
					DatasetOperator operator = calculatedDataset.getOperator();
					if (operator instanceof JoinDatasetOperator) {
						return ((JoinDatasetOperator) operator).missingColumnNamesFromRight.contains(resolved.name());
					} else {
//						if (operator instanceof ColumnMapDatasetOperator
//								&& ((BaseCalculatedDataset) (SimpleColumnResolver.java).operator instanceof JoinDatasetOperator) {
//							return ((JoinDatasetOperator) ((BaseCalculatedDataset) (((UnaryDatasetOperator) operator).operand)).operator).missingColumnNamesFromRight
//									.contains(resolved.name());
//						}
//						return false;
						return (((UnaryDatasetOperator) operator).operand).accept(this);
					}
				}

				@Override
				public Boolean visit(ContextDataset contextDataset) {
					return false;
				}

				@Override
				public Boolean visit(CoreStoreDataset coreStoreDataset) {
					return false;
				}

				@Override
				public Boolean visit(LegacyPostProcessorsDataset legacyPostProcessorDataset) {
					return false;
				}

				@Override
				public Boolean visit(ReusePublishedMeasuresDataset publishedMeasuresDataset) {
					return false;
				}

				@Override
				public Boolean visit(ConstantsDataset constantsDataset) {
					return false;
				}

				@Override
				public Boolean visit(FactsDataset factsDataset) {
					return false;
				}
			});

			if(customizedColumnFromJoin) {
				CreatingSourceCodeInfo codeInfo = CreatingSourceCodeInfo.forMethodCall(this, c);
				return new CalculatedColumn(new HierarchyCustomizerOperator(resolved, codeInfo), resolved.name());
			}
		}
		return applyDefaultPublicationMetadata(resolved, dataset.getDefaultPublicationMetadata());
	}
}
