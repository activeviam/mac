/*
 * (C) ActiveViam 2026
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.activeviam.activepivot.core.impl.internal.utils.ApplicationInTests;
import com.activeviam.activepivot.core.intf.api.contextvalues.IContext;
import com.activeviam.activepivot.core.intf.api.contextvalues.IQueriesResultLimit;
import com.activeviam.activepivot.core.intf.api.contextvalues.IQueriesTimeLimit;
import com.activeviam.activepivot.core.intf.api.contextvalues.mdx.IMdxContext;
import com.activeviam.activepivot.core.intf.api.cube.IActivePivotVersion;
import com.activeviam.activepivot.core.intf.api.cube.hierarchy.IDimension;
import com.activeviam.activepivot.core.intf.api.cube.hierarchy.IHierarchy;
import com.activeviam.activepivot.core.intf.api.cube.hierarchy.IMeasureHierarchy;
import com.activeviam.activepivot.core.intf.api.cube.hierarchy.IMeasureMember;
import com.activeviam.activepivot.core.intf.api.cube.metadata.IHierarchyInfo;
import com.activeviam.activepivot.core.intf.api.cube.metadata.ILevelInfo;
import com.activeviam.database.datastore.internal.IInternalDatastore;
import com.activeviam.mac.statistic.memory.MacTestApplications;
import com.activeviam.tech.composer.test.internal.junit.resources.Resources;
import com.activeviam.tech.composer.test.internal.junit.resources.ResourcesExtension;
import com.activeviam.tech.composer.test.internal.junit.resources.ResourcesHolder;
import com.activeviam.tech.core.api.ordering.IComparator;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Checks that the structure of the monitoring cube (dimensions, hierarchies, levels, measures and
 * shared context values) matches the expected snapshot.
 *
 * <p>Run with {@code -Dmac.model.snapshot.update=true} to regenerate the snapshot.
 */
@ExtendWith(ResourcesExtension.class)
public class TestMonitoringModelSnapshot {

  @Resources public ResourcesHolder resources;

  private static final Path SNAPSHOT =
      Path.of("src", "test", "resources", "model", "monitoring_model_snapshot.txt");

  @BeforeAll
  public static void setupRegistry() {
    RegistryInitializationConfig.setupRegistry();
  }

  @Test
  void testModelMatchesSnapshot() throws IOException {
    final ApplicationInTests<IInternalDatastore> application =
        MacTestApplications.createMonitoringApplication();
    this.resources.register(application).start();

    final String model =
        describe(
            application
                .getManager()
                .getActivePivots()
                .get(ManagerDescriptionConfig.MONITORING_CUBE)
                .getHead());

    if (Boolean.getBoolean("mac.model.snapshot.update")) {
      Files.createDirectories(SNAPSHOT.getParent());
      Files.writeString(SNAPSHOT, model, StandardCharsets.UTF_8);
    }
    assertThat(model).isEqualTo(Files.readString(SNAPSHOT, StandardCharsets.UTF_8));
  }

  private static String describe(final IActivePivotVersion cube) {
    final StringBuilder builder = new StringBuilder();
    builder.append("Cube ").append(cube.getId()).append('\n');
    for (final IDimension<? extends IHierarchy> dimension :
        cube.getDimensions().stream().sorted(Comparator.comparing(IDimension::getName)).toList()) {
      builder
          .append("Dimension ")
          .append(dimension.getName())
          .append(" type=")
          .append(dimension.getDimensionInfo().getDimensionType())
          .append(properties(dimension.getProperties()))
          .append('\n');
      for (final IHierarchy hierarchy :
          dimension.getHierarchies().stream()
              .sorted(Comparator.comparing(IHierarchy::getName))
              .toList()) {
        if (hierarchy instanceof IMeasureHierarchy measureHierarchy) {
          describeMeasures(builder, measureHierarchy);
        } else {
          describeHierarchy(builder, hierarchy);
        }
      }
    }
    describeContext(builder, cube.getContext());
    return builder.toString();
  }

  private static void describeHierarchy(final StringBuilder builder, final IHierarchy hierarchy) {
    final IHierarchyInfo info = hierarchy.getHierarchyInfo();
    builder
        .append("  Hierarchy ")
        .append(hierarchy.getName())
        .append(" slicing=")
        .append(info.isSlicing())
        .append(" visible=")
        .append(info.isVisible())
        .append(" virtual=")
        .append(info.isVirtual())
        .append(" folder=")
        .append(info.getFolder())
        .append(properties(hierarchy.getProperties()))
        .append('\n');
    for (final ILevelInfo level : hierarchy.getLevels()) {
      final IComparator<Object> comparator = level.getComparator();
      builder
          .append("    Level ")
          .append(level.getName())
          .append(" type=")
          .append(level.getLevelType())
          .append(" dataType=")
          .append(level.getDataType())
          .append(" formatter=")
          .append(level.getDefaultFormatter())
          .append(" comparator=")
          .append(comparator == null ? null : comparator.getType())
          .append(properties(level.getProperties()))
          .append('\n');
    }
  }

  private static void describeMeasures(
      final StringBuilder builder, final IMeasureHierarchy hierarchy) {
    // Hidden measures are the intermediate measures created by Copper: their names are generated
    builder.append("  Measures (default=").append(hierarchy.getDefaultMember()).append(")\n");
    hierarchy.retrieveMembers().stream()
        .filter(IMeasureMember::isVisible)
        .sorted(Comparator.comparing(IMeasureMember::getName))
        .forEach(
            measure ->
                builder
                    .append("    Measure ")
                    .append(measure.getName())
                    .append(" folder=")
                    .append(measure.getFolder())
                    .append(" formatter=")
                    .append(measure.getDefaultFormatter())
                    .append(" dataType=")
                    .append(measure.getDataType())
                    .append(properties(measure.getProperties()))
                    .append('\n'));
  }

  private static void describeContext(final StringBuilder builder, final IContext context) {
    builder
        .append("Shared ")
        .append(context.getShared(IQueriesTimeLimit.class))
        .append('\n')
        .append("Shared ")
        .append(context.getShared(IQueriesResultLimit.class))
        .append('\n');
    final IMdxContext mdxContext = context.getShared(IMdxContext.class);
    builder
        .append("Shared MdxContext defaultMembers=")
        .append(mdxContext == null ? null : mdxContext.getDefaultMembers())
        .append('\n');
  }

  private static String properties(final Properties properties) {
    if (properties == null || properties.isEmpty()) {
      return "";
    }
    return properties.stringPropertyNames().stream()
        // Contains the generated names of the intermediate Copper measures
        .filter(key -> !key.equals("underlyingMeasures"))
        .collect(
            Collectors.toMap(
                key -> key, properties::getProperty, (a, b) -> a, () -> new TreeMap<>()))
        .toString()
        .transform(sorted -> " properties=" + sorted);
  }
}
