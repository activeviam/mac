/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.bookmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.qfs.content.snapshot.impl.BasicJsonContentEntry;
import com.qfs.content.snapshot.impl.ContentServiceSnapshotter;
import com.qfs.content.snapshot.impl.SnapshotContentTree;
import com.quartetfs.fwk.IPair;
import com.quartetfs.fwk.impl.Pair;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class containing methods used for the export of the Content Server bookmarks into a
 * directory structure.
 *
 * @author ActiveViam
 */
class ContentServerExporter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContentServerExporter.class);

  private static final String WINDOWS_OS = "Windows";
  private static final String UNIX_OS = "Unix/BSD";
  private static final String CUSTOM = "Custom";

  private static ObjectMapper mapper;
  private static ObjectWriter writer;

  private static final String DEFAULT_ENCODING_CHAR = "_";
  private static String encodingChar = DEFAULT_ENCODING_CHAR;
  private static final Map<String, List<String>> FILESYSTEM_RESTRICTED_CHARACTERS = new HashMap<>();

  static {
    FILESYSTEM_RESTRICTED_CHARACTERS.put(
        WINDOWS_OS, Arrays.asList("\\\\", "<", ">", ":", "\"", "\\|", "\\?", "\\*"));
    FILESYSTEM_RESTRICTED_CHARACTERS.put(UNIX_OS, Arrays.asList("\\\\0", "/"));
    FILESYSTEM_RESTRICTED_CHARACTERS.put(CUSTOM, new ArrayList<>());
  }

  /**
   * Generates the full bookmarks directory, including the i18n and settings files. Alongside the
   * bookmark files and folders, it generates meta files where required.
   *
   * @param snapshotter The content service snapshotter.
   * @param exportDirectory The name of the export directory.
   * @param defaultPermissions The default permissions for the content server.
   */
  static void export(
      ContentServiceSnapshotter snapshotter,
      String exportDirectory,
      Map<String, List<String>> defaultPermissions) {
    SnapshotContentTree bookmarks =
        snapshotter
            .export(
                CSConstants.Paths.SEPARATOR
                    + CSConstants.Paths.UI
                    + CSConstants.Paths.SEPARATOR
                    + CSConstants.Tree.BOOKMARKS)
            .get();
    exportToDirectory(bookmarks, exportDirectory, defaultPermissions);
    snapshotter.export(
        CSConstants.Paths.SETTINGS,
        Paths.get(
            System.getProperty("user.dir"),
            exportDirectory,
            CSConstants.Paths.SETTINGS_TOKEN + CSConstants.Paths.JSON));
    snapshotter.export(
        CSConstants.Paths.I18N,
        Paths.get(System.getProperty("user.dir"), exportDirectory, CSConstants.Paths.I18N_JSON));
  }

  /**
   * Generates the full bookmarks directory from a SnapshotContentTree object.
   *
   * @param bookmarks The SnapshotContentTree bookmarks object.
   * @param folderName The generated folder name.
   * @param defaultPermissions The configured default permissions to use for the bookmarks.
   */
  static void exportToDirectory(
      SnapshotContentTree bookmarks,
      String folderName,
      Map<String, List<String>> defaultPermissions) {
    SnapshotContentTree structure =
        (SnapshotContentTree) bookmarks.getChildren().get(CSConstants.Tree.STRUCTURE);
    SnapshotContentTree content =
        (SnapshotContentTree) bookmarks.getChildren().get(CSConstants.Tree.CONTENT);

    generateTree(folderName, structure, content, defaultPermissions);
  }

  /**
   * Generates the full /ui hierarchy as files and folders.
   *
   * @param exportDirectory The directory to which the bookmark should be exported.
   * @param structureTree The structure SnapshotContentTree.
   * @param contentTree The content SnapshotContentTree.
   * @param defaultPermissions The default permissions.
   */
  private static void generateTree(
      String exportDirectory,
      SnapshotContentTree structureTree,
      SnapshotContentTree contentTree,
      Map<String, List<String>> defaultPermissions) {
    SnapshotNode structure = new SnapshotNode(CSConstants.Paths.BOOKMARK_LISTING);

    Map<String, BasicJsonContentEntry> folderEntries = new HashMap<>();
    structure = parseTree(structureTree, structure, folderEntries);
    folderEntries.put(structure.getKey(), structure.getEntry());

    Map<String, List<String>> permissions = new HashMap<>();
    permissions.put(CSConstants.Role.OWNERS, structure.getEntry().getOwners());
    permissions.put(CSConstants.Role.READERS, structure.getEntry().getReaders());
    IPair<JsonNode, JsonNode> rootPermissions =
        BookmarkTool.transformPermissionsMapToPair(permissions);

    generateJsonFiles(
        structure,
        contentTree,
        exportDirectory,
        folderEntries,
        rootPermissions,
        defaultPermissions,
        false);
  }

  /**
   * Recurses through the SnapshotContentTree, creating a simplified internal representation which
   * is used to generate the directory hierarchy.
   *
   * @param nodeContentTree SnapshotContentTree object to parse.
   * @param nodeInternalTree Internal representation of the SnapshotContentTree object.
   * @param folderEntries Map used to detect if we have a empty folder to skip during generation.
   * @return An internal representation of the SnapshotContentTree object.
   */
  private static SnapshotNode parseTree(
      SnapshotContentTree nodeContentTree,
      SnapshotNode nodeInternalTree,
      Map<String, BasicJsonContentEntry> folderEntries) {
    nodeInternalTree.setEntry(nodeContentTree.getEntry());
    Map<String, SnapshotContentTree> children =
        (Map<String, SnapshotContentTree>) nodeContentTree.getChildren();
    for (Map.Entry<String, SnapshotContentTree> child : children.entrySet()) {
      SnapshotNode childNode = new SnapshotNode(child.getKey());
      childNode.setPath(
          nodeInternalTree.getPath() + CSConstants.Paths.SEPARATOR + nodeInternalTree.getKey());
      nodeInternalTree.addChild(childNode);
      if (!child.getValue().getChildren().isEmpty()) {
        folderEntries.put(child.getKey(), child.getValue().getEntry());
        parseTree(child.getValue(), childNode, folderEntries);
      }
    }
    return nodeInternalTree;
  }

  /**
   * Traverses the structure and content trees in order to generate the JSON files.
   *
   * @param structure The structure internal representation.
   * @param content The content SnapshotContentTree.
   * @param exportDirectory The directory to which the bookmark should be exported.
   * @param folderEntries The node entries that represent folders.
   * @param parentPermissions The permissions of the parent folder.
   * @param defaultPermissions The default permissions.
   * @param isTopLevel Whether or not the current entry is at the top level of the bookmarks
   *     hierarchy.
   */
  private static void generateJsonFiles(
      SnapshotNode structure,
      SnapshotContentTree content,
      String exportDirectory,
      Map<String, BasicJsonContentEntry> folderEntries,
      IPair<JsonNode, JsonNode> parentPermissions,
      Map<String, List<String>> defaultPermissions,
      boolean isTopLevel) {
    IPair<JsonNode, JsonNode> currentOwnerAndReader = new Pair<>();
    createJson(
        structure,
        content,
        exportDirectory,
        folderEntries,
        currentOwnerAndReader,
        parentPermissions,
        defaultPermissions,
        isTopLevel);
    for (SnapshotNode node : structure.getChildren()) {
      boolean topLevel = structure.getKey().equals(CSConstants.Paths.BOOKMARK_LISTING);
      generateJsonFiles(
          node,
          content,
          exportDirectory,
          folderEntries,
          currentOwnerAndReader,
          defaultPermissions,
          topLevel);
    }
  }

  /**
   * Generates a json file for each bookmark, alongside a meta file where required.
   *
   * @param node The internal representation of the node to create a JSON file for.
   * @param content The content SnapshotContentTree.
   * @param exportDirectory The directory to which the bookmark should be exported.
   * @param folderEntries The node entries that represent folders.
   * @param currentPermissions The permissions of the current entry.
   * @param parentPermissions The permissions of the parent folder.
   * @param defaultPermissions The default permissions.
   * @param isTopLevel Whether or not the current entry is at the top level of the bookmarks
   *     hierarchy.
   */
  private static void createJson(
      SnapshotNode node,
      SnapshotContentTree content,
      String exportDirectory,
      Map<String, BasicJsonContentEntry> folderEntries,
      IPair<JsonNode, JsonNode> currentPermissions,
      IPair<JsonNode, JsonNode> parentPermissions,
      Map<String, List<String>> defaultPermissions,
      Boolean isTopLevel) {
    String key = node.getKey();
    SnapshotContentTree folderContentTree = (SnapshotContentTree) content.getChildren().get(key);
    boolean invalidFolder = folderContentTree == null || folderContentTree.getEntry() == null;
    if (invalidFolder) {
      return;
    }

    Map<String, List<String>> permissionsMap = new HashMap<>();
    permissionsMap.put(CSConstants.Role.OWNERS, folderContentTree.getEntry().getOwners());
    permissionsMap.put(CSConstants.Role.READERS, folderContentTree.getEntry().getReaders());
    currentPermissions.setLeft(
        BookmarkTool.transformPermissionsMapToPair(permissionsMap).getLeft());
    currentPermissions.setRight(
        BookmarkTool.transformPermissionsMapToPair(permissionsMap).getRight());
    IPair<JsonNode, JsonNode> defaultPermissionsPair =
        BookmarkTool.transformPermissionsMapToPair(defaultPermissions);

    boolean omitPermissions =
        currentPermissions.equals(parentPermissions)
            || isTopLevel && currentPermissions.equals(defaultPermissionsPair);

    try {
      /*
       * Generates a json file containing the content alongside a meta file containg
       * the owner and reader values for the given node
       */
      String name =
          encodeForFilesystems(retrievePropertyFromContent(content, key, CSConstants.Tree.NAME));
      JsonNode descriptionJsonNode = null;

      StringBuilder exportFolder = new StringBuilder();
      exportFolder
          .append(System.getProperty("user.dir"))
          .append(CSConstants.Paths.FILESYSTEM_SEPARATOR)
          .append(exportDirectory);

      StringBuilder folderName = new StringBuilder();
      folderName.append(exportFolder.toString());

      for (String folder : node.getPath().split(CSConstants.Paths.SEPARATOR)) {
        String currentFolder = retrievePropertyFromContent(content, folder, CSConstants.Tree.NAME);
        folderName
            .append(currentFolder.isEmpty() ? "" : CSConstants.Paths.FILESYSTEM_SEPARATOR)
            .append(currentFolder);
      }

      File bookmarkFolder = new File(folderName.toString());
      if (!bookmarkFolder.exists()) {
        boolean createdDirectories = new File(folderName.toString()).mkdirs();
        if (!createdDirectories) {
          throw new IOException(
              "Could not create export directories. Check path: " + folderName.toString());
        }
      }

      JsonNode entryNode = mapper.readTree(content.getChildren().get(key).getEntry().getContent());
      descriptionJsonNode = entryNode.path(CSConstants.Tree.DESCRIPTION);
      if (!folderEntries.containsKey(key)
          && !entryNode
              .path(CSConstants.Tree.TYPE)
              .toString()
              .contains(CSConstants.Content.FOLDER)) {
        StringBuilder fileName = new StringBuilder();
        fileName
            .append(bookmarkFolder)
            .append(CSConstants.Paths.FILESYSTEM_SEPARATOR)
            .append(name)
            .append(CSConstants.Paths.JSON);
        writer.writeValue(new File(fileName.toString()), entryNode.path(CSConstants.Tree.VALUE));
      }

      ObjectNode meta;
      meta = mapper.createObjectNode();
      meta.put(CSConstants.Tree.KEY, key);
      if (descriptionJsonNode != null && !descriptionJsonNode.toString().isEmpty()) {
        meta.set(CSConstants.Tree.DESCRIPTION, descriptionJsonNode);
      }
      if (!omitPermissions) {
        meta.set(CSConstants.Role.OWNERS, currentPermissions.getLeft());
        meta.set(CSConstants.Role.READERS, currentPermissions.getRight());
      }
      if (meta.size() > 0) {
        StringBuilder metaFileName = new StringBuilder();
        metaFileName
            .append(bookmarkFolder)
            .append(CSConstants.Paths.FILESYSTEM_SEPARATOR)
            .append(name)
            .append(CSConstants.Paths.METADATA_FILE);
        writer.writeValue(new File(metaFileName.toString()), meta);
      }
    } catch (IOException e) {
      LOGGER.warn(e.toString());
    }
  }

  /**
   * Read the content SnapshotContentTree and retrieve a property of the bookmark content for a
   * given id.
   *
   * @param content The content SnapshotContentTree.
   * @param id The bookmark id.
   * @param property The property to be retrieved.
   * @return The text of the bookmark content property.
   */
  private static String retrievePropertyFromContent(
      SnapshotContentTree content, String id, String property) {
    SnapshotContentTree currentEntry = (SnapshotContentTree) content.getChildren().get(id);
    if (currentEntry == null) {
      return id;
    }
    String value = "";
    try {
      value = mapper.readTree(currentEntry.getEntry().getContent()).get(property).asText();
    } catch (Exception e) {
      LOGGER.warn("Could not extract property " + property + " from node " + id + ".");
    }
    return value;
  }

  /**
   * Replaces restricted filename characters with a defined character. The default is "_".
   *
   * @param fileName The name of the file that will be created.
   * @return The encoded file name.
   */
  static String encodeForFilesystems(String fileName) {
    for (List<String> restrictedCharacters : FILESYSTEM_RESTRICTED_CHARACTERS.values()) {
      for (String character : restrictedCharacters) {
        fileName = fileName.replaceAll(character, encodingChar);
      }
    }
    return fileName;
  }

  /**
   * Sets the character to be used in the {@link #encodeForFilesystems(String)} method.
   *
   * @param character The encoding character.
   */
  static void setEncodingChar(String character) {
    encodingChar = character;
  }

  /**
   * Adds the parameters to the list of characters replaced by the encoding character when creating
   * a bookmark file.
   *
   * @param characters The list of characters to be replaced.
   */
  static void setCustomCharsToEncode(List<String> characters) {
    FILESYSTEM_RESTRICTED_CHARACTERS.get(CUSTOM).addAll(characters);
  }

  /** Resets the encoding character to the default value. */
  static void resetEncodingChar() {
    encodingChar = DEFAULT_ENCODING_CHAR;
  }

  /** Resets the list of custom filename forbidden characters to empty. */
  static void resetCustomCharsToEncode() {
    FILESYSTEM_RESTRICTED_CHARACTERS.replace(CUSTOM, new ArrayList<>());
  }

  /**
   * Sets the writer to use for the lifetime of the application.
   *
   * @param toSet The writer to set.
   */
  static void setWriter(ObjectWriter toSet) {
    writer = toSet;
  }

  /**
   * Sets the mapper to use for the lifetime of the application.
   *
   * @param toSet The mapper to set.
   */
  static void setMapper(ObjectMapper toSet) {
    mapper = toSet;
  }
}
