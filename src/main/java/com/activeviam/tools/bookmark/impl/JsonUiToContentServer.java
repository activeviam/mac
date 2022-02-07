/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.tools.bookmark.impl;

import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants.Paths;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants.Tree;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.qfs.content.snapshot.impl.ContentServiceSnapshotter;
import com.qfs.content.snapshot.impl.SnapshotContentTree;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  static void setDashboardTreeResolver(PathMatchingResourcePatternResolver toSet) {
    dashboardTreeResolver = toSet;
  }

  /**
   * Generates and loads a tree into a given ContentServiceSnapshotter, from an ui folder.
   *
   * @param snapshotter The ContentServiceSnapshotter.
   * @param folderName The ui folder name.
   * @param defaultPermissions The default permissions to use when no parent permissions are found.
   */
  static void importIntoContentServer(
      ContentServiceSnapshotter snapshotter,
      String folderName,
      Map<String, List<String>> defaultPermissions) {
    PERMISSIONS.putAll(defaultPermissions != null ? defaultPermissions : DEFAULT_PERMISSIONS);
    snapshotter.eraseAndImport(ContentServerConstants.Paths.UI, Paths.INITIAL_CONTENT);
    snapshotter.eraseAndImport(
        folderName + Paths.SEPARATOR + Tree.DASHBOARDS, loadDirectory(Paths.DASHBOARDS));
    snapshotter.eraseAndImport(
        folderName + Paths.SEPARATOR + Tree.WIDGETS, loadDirectory(Paths.WIDGETS));
  }

  /**
   * Generates a SnapshotContentTree from a directory.
   *
   * @return the SnapshotContentTree.
   */
  static SnapshotContentTree loadDirectory(String path) {
    File directory = getSubDirectory(path);
    if (directory == null) {
      LOGGER.info("Nothing to load");
      return null;
    }

    return createDirectoryTree(directory, createEmptyDirectoryNode());
  }

  private static File getSubDirectory(final String folderName) {
    try {
      return dashboardTreeResolver.getResource(folderName).getFile();
    } catch (IOException ioe) {
      LOGGER.error(
          "Unable to retrieve directory {} from resources. The import will fail.", folderName);
      return null;
    }
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
  private static SnapshotContentTree createDirectoryTree(File root, SnapshotContentTree parent) {
    if (root == null) {
      LOGGER.info("Nothing to load");
      return null;
    }
    for (File child : root.listFiles()) {
      if (child.isFile()) {
        final JsonNode jsonNodeContent = loadFileIntoNode(child);
        final SnapshotContentTree node =
            new SnapshotContentTree(
                jsonNodeContent.toString(),
                false,
                PERMISSIONS.get(ContentServerConstants.Role.OWNERS),
                PERMISSIONS.get(ContentServerConstants.Role.READERS),
                new HashMap<>());
        parent.putChild(child.getName().replace(Paths.JSON, ""), node, true);
      } else {
        final SnapshotContentTree childNode = createEmptyDirectoryNode();
        SnapshotContentTree childTree = createDirectoryTree(child, childNode);
        parent.putChild(child.getName(), childTree, true);
      }
    }
    return parent;
  }

  /**
   * Loads the contents of a file into a JsonNode.
   *
   * @param file The file to load.
   * @return The contents of the file, as a JsonNode.
   */
  private static JsonNode loadFileIntoNode(File file) {
    JsonNode loadedFile = JsonNodeFactory.instance.objectNode();
    try {
      final ObjectMapper mapper = new ObjectMapper();
      loadedFile = mapper.readTree(new FileInputStream(file));
    } catch (Exception e) {
      LOGGER.warn("Unable to find file " + file.getName());
    }
    return loadedFile;
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
