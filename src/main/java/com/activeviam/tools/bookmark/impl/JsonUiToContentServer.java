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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
   * Generates a SnapshotContentTree from a directory.
   *
   * @return the SnapshotContentTree.
   */
  static SnapshotContentTree loadDirectory(String path) {
    return createDirectoryTree(path, createEmptyDirectoryNode());
  }

  /**
   * Creates a {@link SnapshotContentTree} from the content of a directory assuming that the
   * directory contains only subdirectories with one json file each. For each subdirectory, we add a
   * leaf to the root {@link SnapshotContentTree}. The key of the leaf is the name of the
   * subdirectory. The content of the leaf is a {@link SnapshotContentTree} with one leaf. The key
   * of this grandchild leaf is the name of the subdirectory + "_metadata". Its content is the
   * content of the json file inside the subdirectory.
   *
   * @param root the current directory to add to the structureTree.
   */
  private static SnapshotContentTree createDirectoryTree(String root, SnapshotContentTree parent) {
    try {
      Resource[] rootFiles = dashboardTreeResolver.getResources("classpath*:/**/" + root + "/*");
      for (Resource child : rootFiles) {
        String[] path = child.getURL().toString().split(Paths.SEPARATOR);
        String childName = path[path.length - 1];
        if (childName.endsWith(Paths.JSON)) {
          final JsonNode jsonNodeContent = loadFileIntoNode(child.getInputStream());
          final SnapshotContentTree node =
              new SnapshotContentTree(
                  jsonNodeContent.toString(),
                  false,
                  PERMISSIONS.get(ContentServerConstants.Role.OWNERS),
                  PERMISSIONS.get(ContentServerConstants.Role.READERS),
                  new HashMap<>());
          parent.putChild(childName.replace(Paths.JSON, ""), node, true);
        } else {
          final SnapshotContentTree childNode = createEmptyDirectoryNode();
          SnapshotContentTree childTree =
              createDirectoryTree(root + Paths.SEPARATOR + childName, childNode);
          parent.putChild(childName, childTree, true);
        }
      }
      return parent;
    } catch (IOException ioe) {
      LOGGER.error("Unable to retrieve directory {} from resources. The import will fail.", root);
      return null;
    }
  }

  /**
   * Loads the contents of an inputStream into a JsonNode.
   *
   * @param inputStream The inputStream to load.
   * @return The contents of the inputStream, as a JsonNode.
   */
  private static JsonNode loadFileIntoNode(InputStream inputStream) throws IOException {
    final ObjectMapper mapper = new ObjectMapper();
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
