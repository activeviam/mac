/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.tools.bookmark.impl;

import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.qfs.content.snapshot.impl.ContentServiceSnapshotter;
import com.quartetfs.fwk.IPair;
import com.quartetfs.fwk.impl.Pair;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * Library class exposing content server bookmark import and export functionality for Accelerators.
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
      ContentServiceSnapshotter snapshotter,
      String folderName,
      Map<String, List<String>> defaultPermissions) {
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
    JsonUiToContentServer.setBookmarkTreeResolver(resolver);
    JsonUiToContentServer.setStandaloneResolver(resolver);
    JsonUiToContentServer.importIntoContentServer(snapshotter, folderName, defaultPermissions);
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
      ContentServiceSnapshotter snapshotter,
      String folderName,
      Map<String, List<String>> defaultPermissions) {
    ObjectMapper mapper = new ObjectMapper();
    ContentServerToJsonUi.setMapper(mapper);
    ContentServerToJsonUi.setWriter(mapper.writer(new DefaultPrettyPrinter()));
    ContentServerToJsonUi.export(snapshotter, folderName, defaultPermissions);
  }

  /**
   * Sets the character that should be used to replace forbidden characters in file names. Allows
   * customisation of forbidden characters.
   *
   * @param encodingChar The encoding character.
   * @param charsToEncode A custom list of characters that should be replaced with the encoding
   *     character.
   */
  public static void configureExportFileNames(String encodingChar, List<String> charsToEncode) {
    ContentServerToJsonUi.setEncodingChar(encodingChar);
    ContentServerToJsonUi.setCustomCharsToEncode(charsToEncode);
  }

  /**
   * Transform a permissions map of type to role names into a pair of JsonNode objects.
   *
   * @param permissions The map of type to role name.
   * @return The pair of JsonNode objects.
   */
  public static IPair<JsonNode, JsonNode> transformPermissionsMapToPair(
      Map<String, List<String>> permissions) {
    final List<String> ownersList = permissions.get(ContentServerConstants.Role.OWNERS);
    final List<String> readersList = permissions.get(ContentServerConstants.Role.READERS);
    return new Pair<>(jsonNodeFromStringList(ownersList), jsonNodeFromStringList(readersList));
  }

  /**
   * Transform the string representation of lists of role names into a pair of JsonNode objects.
   *
   * @param owners The string representation of a list of owners.
   * @param readers The string representation of a list of readers.
   * @return The pair of JsonNode objects.
   */
  public static IPair<JsonNode, JsonNode> transformPermissionsStringsToPair(
      String owners, String readers) {
    final List<String> ownersList = Arrays.asList(owners.split(","));
    final List<String> readersList = Arrays.asList(readers.split(","));
    return new Pair<>(jsonNodeFromStringList(ownersList), jsonNodeFromStringList(readersList));
  }

  /**
   * Create a JsonNode array from a list of users.
   *
   * @param users The list of users.
   * @return The JsonNode array.
   */
  protected static JsonNode jsonNodeFromStringList(List<String> users) {
    final JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
    final ArrayNode node = nodeFactory.arrayNode();
    users.forEach(node::add);
    return node;
  }

  /**
   * Transform the string representation of lists of role names into a map of type to role name.
   *
   * @param owners The string representation of a list of owners.
   * @param readers The string representation of a list of readers.
   * @return The permissions map.
   */
  public static Map<String, List<String>> transformPermissionsStringsToMap(
      String owners, String readers) {
    final Map<String, List<String>> permissionsMap = new HashMap<>();
    permissionsMap.put(ContentServerConstants.Role.OWNERS, Arrays.asList(owners.split(",")));
    permissionsMap.put(ContentServerConstants.Role.READERS, Arrays.asList(readers.split(",")));
    return permissionsMap;
  }

  /**
   * Transform a pair of JsonNode objects into a map of type to role name.
   *
   * @param permissions The pair of JsonNode objects.
   * @return The permissions map.
   */
  public static Map<String, List<String>> transformPermissionsPairToMap(
      IPair<JsonNode, JsonNode> permissions) {
    final Map<String, List<String>> permissionsMap = new HashMap<>();
    final List<String> owners = new ArrayList<>();
    final List<String> readers = new ArrayList<>();
    permissions.getLeft().forEach(user -> owners.add(user.asText()));
    permissions.getRight().forEach(user -> readers.add(user.asText()));
    permissionsMap.put(ContentServerConstants.Role.OWNERS, owners);
    permissionsMap.put(ContentServerConstants.Role.READERS, readers);
    return permissionsMap;
  }
}
