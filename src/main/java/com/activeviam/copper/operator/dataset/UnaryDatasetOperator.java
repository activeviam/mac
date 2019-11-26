/*
 * (C) ActiveViam 2017
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.copper.operator.dataset;

import com.activeviam.collections.ImmutableCollection;
import com.activeviam.collections.ImmutableList;
import com.activeviam.copper.builders.dataset.CoreDataset;
import com.activeviam.copper.columns.CoreColumn;
import com.activeviam.copper.columns.members.DistinctValuesSupplier;
import com.activeviam.copper.exceptions.UserErrorException;
import com.activeviam.copper.exceptions.codeinfo.CreatingSourceCodeInfo;
import java.util.HashMap;
import java.util.Map;

/** An operator that produces a dataset from another. */
public abstract class UnaryDatasetOperator implements DatasetOperator {

  private static final long serialVersionUID = 1L;
  /** The operand dataset. */
  public final CoreDataset operand;
  /** The code that created this operator. */
  protected final CreatingSourceCodeInfo creatingCodeInfo;

  /**
   * Constructor.
   *
   * @param operand The operand dataset.
   * @param codeInfo The code that created this operator.
   */
  public UnaryDatasetOperator(CoreDataset operand, CreatingSourceCodeInfo codeInfo) {
    this.operand = operand;
    this.creatingCodeInfo = codeInfo;
  }

  @Override
  public CoreColumn getColumnInParentLeftUnchanged(String unchangedColumnName) {
    return this.operand.getColumn(unchangedColumnName);
  }

  /**
   * Extract the values supplier from a column contained in the dataset created by this operator.
   *
   * @param createdColumn The created column.
   * @return The values supplier.
   */
  protected DistinctValuesSupplier getMembersSupplierFor(CoreColumn createdColumn) {
    return getMembersSupplierFor(this, createdColumn);
  }

  /**
   * Gives the list of non constant {@link CoreColumn#hasConstantValues() non constant} columns from
   * our {@link #operand}.
   *
   * @return The list of non constant {@link CoreColumn#hasConstantValues() non constant} columns
   *     from our {@link #operand}.
   */
  public ImmutableList<CoreColumn> getNonConstantOperandColumns() {
    return this.operand.getColumns().filter(c -> !c.hasConstantValues());
  }

  /**
   * Extract the values supplier from a column contained in the dataset created by a {@link
   * DatasetOperator}.
   *
   * @param operator The operator that created the dataset. This operator might not be a {@link
   *     UnaryDatasetOperator} (hence this static method).
   * @param createdColumn The created column.
   * @return The values supplier.
   */
  public static DistinctValuesSupplier getMembersSupplierFor(
      DatasetOperator operator, CoreColumn createdColumn) {
    // First see if the column has a distinct member supplier
    // This could be the case if user explicitly specifies the column's members using
    // .withMemberList().
    // In which case, we return the column's supplier. Otherwise, get the output supplier from the
    // operator.
    final DistinctValuesSupplier columnSupplier = createdColumn.getMembersSupplier();
    if (columnSupplier.canSupplyValues()) {
      return columnSupplier;
    }
    return operator.toColumnOperatorProducing(createdColumn.name()).getOutputMembersSupplier();
  }

  /**
   * Tests that all the input columns have distinct names and throws otherwise.
   *
   * @param among The columns to test.
   * @throws UserErrorException If two columns have the same name.
   */
  public static void throwOnNameConflict(ImmutableCollection<CoreColumn> among)
      throws UserErrorException {
    Map<String, CoreColumn> current = new HashMap<>();
    among.forEach(
        c -> {
          String name = c.name();
          CoreColumn previous = current.put(name, c);
          if (previous != null) {
            boolean toStringAreUseless =
                String.valueOf(c).equals(name) && String.valueOf(previous).equals(name);
            String detail = toStringAreUseless ? "" : ": " + previous + " and " + c;
            throw new UserErrorException(
                "Two columns have the name '" + c.name() + "'" + detail + ".");
          }
        });
  }
}
