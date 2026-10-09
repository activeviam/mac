/*
 * (C) ActiveViam 2026
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.tools.bookmark.impl;

import static org.assertj.core.api.Assertions.assertThat;

import com.activeviam.tech.contentserver.storage.api.IContentTree;
import com.activeviam.tech.contentserver.storage.api.SnapshotContentTree;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants.Paths;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/** Tests the loading of the bookmarks packaged with the application. */
public class TestJsonUiToContentServer {

  private static final Map<String, String> FILES =
      Map.of(
          Paths.DASHBOARDS + "/content/overview.json", "{\"name\":\"overview\"}",
          Paths.DASHBOARDS + "/structure/home/home_metadata.json", "{\"name\":\"home\"}");

  @Test
  public void testLoadDirectoryFromJar(@TempDir final Path tempDir) throws IOException {
    final Path jar = tempDir.resolve("bookmarks.jar");
    try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
      for (final String directory :
          List.of(
              "ui/",
              "ui/dashboards/",
              "ui/dashboards/content/",
              "ui/dashboards/structure/",
              "ui/dashboards/structure/home/")) {
        output.putNextEntry(new JarEntry(directory));
        output.closeEntry();
      }
      for (final Map.Entry<String, String> file : FILES.entrySet()) {
        output.putNextEntry(new JarEntry(file.getKey()));
        output.write(file.getValue().getBytes(StandardCharsets.UTF_8));
        output.closeEntry();
      }
    }

    assertLoadedDashboards(jar.toUri().toURL());
  }

  @Test
  public void testLoadDirectoryFromFolder(@TempDir final Path tempDir) throws IOException {
    for (final Map.Entry<String, String> file : FILES.entrySet()) {
      final Path path = tempDir.resolve(file.getKey());
      Files.createDirectories(path.getParent());
      try (OutputStream output = Files.newOutputStream(path)) {
        output.write(file.getValue().getBytes(StandardCharsets.UTF_8));
      }
    }

    assertLoadedDashboards(tempDir.toUri().toURL());
  }

  private static void assertLoadedDashboards(final URL classpathRoot) throws IOException {
    try (URLClassLoader classLoader = new URLClassLoader(new URL[] {classpathRoot}, null)) {
      JsonUiToContentServer.setResourcePatternResolver(
          new PathMatchingResourcePatternResolver(classLoader));

      final SnapshotContentTree dashboards = JsonUiToContentServer.loadDirectory(Paths.DASHBOARDS);

      assertThat(dashboards.getChildren()).containsOnlyKeys("content", "structure");
      final IContentTree<?> content = dashboards.getChildren().get("content");
      assertThat(content.getChildren()).containsOnlyKeys("overview");
      assertThat(content.getChildren().get("overview").getEntry().getContent())
          .isEqualTo("{\"name\":\"overview\"}");
      final IContentTree<?> structure = dashboards.getChildren().get("structure");
      assertThat(structure.getChildren()).containsOnlyKeys("home");
      assertThat(structure.getChildren().get("home").getChildren())
          .containsOnlyKeys("home_metadata");
    }
  }
}
