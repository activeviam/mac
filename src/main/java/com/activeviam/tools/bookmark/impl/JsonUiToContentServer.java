/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.tools.bookmark.impl;

import com.activeviam.tech.contentserver.storage.api.ContentServiceSnapshotter;
import com.activeviam.tech.contentserver.storage.api.SnapshotContentTree;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants.Paths;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

/**
 * Reads a Directory hierarchy representing the contents and structure part of the bookmarks and
 * returns a SnapshotContentTree representing this subtree where the root is "/ui".
 */
public class JsonUiToContentServer {

  static final Map<String, List<String>> DEFAULT_PERMISSIONS = new HashMap<>();
  static final Map<String, List<String>> PERMISSIONS = new HashMap<>();
  private static final Logger LOGGER = LoggerFactory.getLogger(JsonUiToContentServer.class);
  private static PathMatchingResourcePatternResolver dashboardTreeResolver;

  static {
    DEFAULT_PERMISSIONS.put(
        ContentServerConstants.Role.OWNERS,
        Collections.singletonList(ContentServerConstants.Role.ROLE_CS_ROOT));
    DEFAULT_PERMISSIONS.put(
        ContentServerConstants.Role.READERS,
        Collections.singletonList(ContentServerConstants.Role.ROLE_USER));
  }

  /**
   * Sets the bookmark tree resource resolver to use for the lifetime of the application.
   *
   * @param toSet The resource resolver to set.
   */
  static void setResourcePatternResolver(PathMatchingResourcePatternResolver toSet) {
    dashboardTreeResolver = toSet;
  }

  /**
   * Generates and loads a tree into a given ContentServiceSnapshotter, from an ui folder.
   *
   * @param snapshotter The ContentServiceSnapshotter.
   * @param defaultPermissions The default permissions to use when no parent permissions are found.
   */
  static void importIntoContentServer(
      ContentServiceSnapshotter snapshotter, Map<String, List<String>> defaultPermissions) {
    PERMISSIONS.putAll(defaultPermissions != null ? defaultPermissions : DEFAULT_PERMISSIONS);
    try {
      InputStream res =
          dashboardTreeResolver.getClassLoader().getResourceAsStream(Paths.INITIAL_CONTENT);
      snapshotter.eraseAndImport(ContentServerConstants.Paths.UI, res);
    } catch (Exception e) {
      LOGGER.error("Cannot load the initial content file");
    }
    snapshotter.eraseAndImport(Paths.DASHBOARDS, loadDirectory(Paths.DASHBOARDS));
    snapshotter.eraseAndImport(Paths.WIDGETS, loadDirectory(Paths.WIDGETS));
  }

  /**
   * Generates a SnapshotContentTree from a directory of the classpath.
   *
   * <p>Each json file of the directory, or of one of its subdirectories, becomes a leaf of the
   * tree, named after the file without its extension. Each subdirectory becomes a directory node.
   *
   * @param path the path of the directory, relative to the root of the classpath
   * @return the SnapshotContentTree.
   */
  static SnapshotContentTree loadDirectory(String path) {
    final SnapshotContentTree tree = createEmptyDirectoryNode();
    try {
      // The files are listed from the roots of the directory, as Spring cannot find resources
      // packaged in a jar with a pattern starting with a wildcard (e.g. "classpath*:/**/ui/*"),
      // nor list the subdirectories of a directory packaged in a jar.
      for (final Resource rootDirectory :
          dashboardTreeResolver.getResources("classpath*:" + path + Paths.SEPARATOR)) {
        final String rootUrl = rootDirectory.getURL().toString();
        for (final Resource file :
            dashboardTreeResolver.getResources(rootUrl + "**/*" + Paths.JSON)) {
          final String relativePath = file.getURL().toString().substring(rootUrl.length());
          addFile(tree, relativePath.split(Paths.SEPARATOR), file);
        }
      }
      return tree;
    } catch (IOException ioe) {
      LOGGER.error("Unable to retrieve directory {} from resources. The import will fail.", path);
      return null;
    }
  }

  /**
   * Adds the content of a json file to a tree, creating the missing intermediate directories.
   *
   * @param tree the tree to add the file to
   * @param relativePath the path of the file in the tree, split into its parts
   * @param file the json file
   */
  private static void addFile(SnapshotContentTree tree, String[] relativePath, Resource file)
      throws IOException {
    SnapshotContentTree parent = tree;
    for (int i = 0; i < relativePath.length - 1; i++) {
      final String directoryName = relativePath[i];
      SnapshotContentTree directory = (SnapshotContentTree) parent.getChildren().get(directoryName);
      if (directory == null) {
        directory = createEmptyDirectoryNode();
        parent.putChild(directoryName, directory, true);
      }
      parent = directory;
    }

    final String fileName = relativePath[relativePath.length - 1];
    final JsonNode jsonNodeContent = loadFileIntoNode(file.getInputStream());
    final SnapshotContentTree node =
        new SnapshotContentTree(
            jsonNodeContent.toString(),
            false,
            PERMISSIONS.get(ContentServerConstants.Role.OWNERS),
            PERMISSIONS.get(ContentServerConstants.Role.READERS),
            new HashMap<>());
    parent.putChild(fileName.replace(Paths.JSON, ""), node, true);
  }

  /**
   * Loads the contents of an inputStream into a JsonNode.
   *
   * @param inputStream The inputStream to load.
   * @return The contents of the inputStream, as a JsonNode.
   */
  private static JsonNode loadFileIntoNode(InputStream inputStream) throws IOException {
    final ObjectMapper mapper = new JsonMapper();
    return mapper.readTree(inputStream);
  }

  private static SnapshotContentTree createEmptyDirectoryNode() {
    return new SnapshotContentTree(
        null,
        true,
        PERMISSIONS.get(ContentServerConstants.Role.OWNERS),
        PERMISSIONS.get(ContentServerConstants.Role.READERS),
        new HashMap<>());
  }
}
