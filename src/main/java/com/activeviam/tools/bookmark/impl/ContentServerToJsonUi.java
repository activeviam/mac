/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.tools.bookmark.impl;

import com.activeviam.tech.contentserver.storage.api.BasicJsonContentEntry;
import com.activeviam.tech.contentserver.storage.api.ContentServiceSnapshotter;
import com.activeviam.tech.contentserver.storage.api.IContentTree;
import com.activeviam.tech.contentserver.storage.api.SnapshotContentTree;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants.Paths;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class containing methods used for the export of the Content Server bookmarks into a
 * directory structure.
 */
class ContentServerToJsonUi {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContentServerToJsonUi.class);

  private static final String WINDOWS_OS = "Windows";
  private static final String UNIX_OS = "Unix/BSD";
  private static final String CUSTOM = "Custom";
  private static final String DEFAULT_ENCODING_CHAR = "_";
  private static final Map<String, List<String>> FILESYSTEM_RESTRICTED_CHARACTERS = new HashMap<>();
  private static ObjectMapper mapper;
  private static ObjectWriter writer;
  private static String encodingChar = DEFAULT_ENCODING_CHAR;

  static {
    FILESYSTEM_RESTRICTED_CHARACTERS.put(
        WINDOWS_OS, Arrays.asList("\\\\", "<", ">", ":", "\"", "\\|", "\\?", "\\*"));
    FILESYSTEM_RESTRICTED_CHARACTERS.put(UNIX_OS, Arrays.asList("\\\\0", "/"));
    FILESYSTEM_RESTRICTED_CHARACTERS.put(CUSTOM, new ArrayList<>());
  }

  /**
   * Generates the full ui directory.
   *
   * @param snapshotter The content service snapshotter.
   * @param exportDirectory The name of the export directory.
   */
  static void export(ContentServiceSnapshotter snapshotter, String exportDirectory) {
    final SnapshotContentTree ui = snapshotter.export(Paths.UI).get();
    writeSubTree(ui, Path.of(System.getProperty("user.dir"), exportDirectory));
  }

  static void writeSubTree(SnapshotContentTree node, Path currentPath) {
    for (Entry<String, ? extends IContentTree<BasicJsonContentEntry>> entry :
        node.getChildren().entrySet()) {
      String nodeName = entry.getKey();
      SnapshotContentTree childNode = (SnapshotContentTree) entry.getValue();
      if (childNode.getChildren() == null) {
        writeNode(nodeName, childNode, currentPath);
      } else {
        createDirectory(currentPath.resolve(encodeForFilesystems(nodeName)));
        writeSubTree(childNode, currentPath.resolve(nodeName));
      }
    }
  }

  static void createDirectory(Path path) {
    final File bookmarkFolder = path.toFile();
    if (!bookmarkFolder.exists()) {
      boolean createdDirectories = bookmarkFolder.mkdirs();
      if (!createdDirectories) {
        LOGGER.error("Could not create export directories. Check path: " + path.getFileName());
      }
    }
  }

  static void writeNode(String key, SnapshotContentTree node, Path folderName) {
    final JsonNode entryNode;
    try {
      entryNode = mapper.readTree(node.getEntry().getContent());
      final Path fileName =
          folderName.resolve(encodeForFilesystems(key) + ContentServerConstants.Paths.JSON);
      writer.writeValue(fileName.toFile(), entryNode);
    } catch (IOException e) {
      LOGGER.error("Could not write the node " + key + ": " + e.getMessage());
    }
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
