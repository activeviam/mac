/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.bookmark;

import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.qfs.content.snapshot.impl.ContentServiceSnapshotter;
import com.quartetfs.fwk.IPair;
import com.quartetfs.fwk.impl.Pair;
import java.util.List;
import java.util.Map;

/**
 * Tool exposing the content server bookmark import and export functionalities.
 *
 * @author ActiveViam
 */
public class BookmarkTool {

  /**
   * Imports content server bookmarks from a directory structure representing the required UI
   * bookmark structure.
   *
   * @param snapshotter The content service snapshotter to use for the import.
   * @param folderName The folder from which to import the bookmarks.
   * @param defaultPermissions The permissions to use when not explicitly defining permissions for
   *     any bookmark path.
   */
  public static void importBookmarks(
      final ContentServiceSnapshotter snapshotter,
      final String folderName,
      final Map<String, List<String>> defaultPermissions) {
    ContentServerImporter.importIntoContentServer(snapshotter, folderName, defaultPermissions);
  }

  /**
   * Exports content server bookmarks into a directory structure representing the UI bookmark
   * structure.
   *
   * @param snapshotter The content service snapshotter to use for the export.
   * @param folderName The folder into which the bookmarks should be exported.
   * @param defaultPermissions The default permissions to use for the tree, except where explicitly
   *     defined otherwise.
   */
  public static void exportBookmarks(
      final ContentServiceSnapshotter snapshotter,
      final String folderName,
      final Map<String, List<String>> defaultPermissions) {
    ObjectMapper mapper = new ObjectMapper();
    ContentServerExporter.setMapper(mapper);
    ContentServerExporter.setWriter(mapper.writer(new DefaultPrettyPrinter()));
    ContentServerExporter.export(snapshotter, folderName, defaultPermissions);
  }

  /**
   * Transform a permissions map of type to role names into a pair of JsonNode objects.
   *
   * @param permissions The map of type to role name.
   * @return The pair of JsonNode objects.
   */
  public static IPair<JsonNode, JsonNode> transformPermissionsMapToPair(
      final Map<String, List<String>> permissions) {
    List<String> ownersList = permissions.get(CSConstants.Role.OWNERS);
    List<String> readersList = permissions.get(CSConstants.Role.READERS);
    return new Pair<>(jsonNodeFromStringList(ownersList), jsonNodeFromStringList(readersList));
  }

  /**
   * Create a JsonNode array from a list of users.
   *
   * @param users The list of users.
   * @return The JsonNode array.
   */
  private static JsonNode jsonNodeFromStringList(List<String> users) {
    JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
    ArrayNode node = nodeFactory.arrayNode();
    users.forEach(node::add);
    return node;
  }
}
