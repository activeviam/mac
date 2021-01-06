/*
 * (C) ActiveViam 2018
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.tools.bookmark.impl;

import com.activeviam.tools.bookmark.constant.impl.CsConstants;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.qfs.content.snapshot.impl.ContentServiceSnapshotter;
import com.qfs.content.snapshot.impl.SnapshotContentTree;
import com.quartetfs.fwk.IPair;
import com.quartetfs.fwk.impl.Pair;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

/**
 * Reads a Directory hierarchy representing the contents and structure part of the bookmarks and
 * returns a SnapshotContentTree representing this subtree where the root is "/ui".
 */
class JsonUiToContentServer {

  static final Map<String, List<String>> ROOT_ONLY_PERMISSIONS = new HashMap<>();
  static final Map<String, List<String>> ROOT_OWNER_PERMISSIONS = new HashMap<>();
  static final Map<String, List<String>> DEFAULT_PERMISSIONS = new HashMap<>();

  static {
    ROOT_ONLY_PERMISSIONS.put(
        CsConstants.Role.OWNERS, Collections.singletonList(CsConstants.Role.ROLE_CS_ROOT));
    ROOT_ONLY_PERMISSIONS.put(
        CsConstants.Role.READERS, Collections.singletonList(CsConstants.Role.ROLE_CS_ROOT));
    ROOT_OWNER_PERMISSIONS.put(
        CsConstants.Role.OWNERS, Collections.singletonList(CsConstants.Role.ROLE_CS_ROOT));
    ROOT_OWNER_PERMISSIONS.put(
        CsConstants.Role.READERS, Collections.singletonList(CsConstants.Role.ROLE_USER));
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonUiToContentServer.class);

  private static PathMatchingResourcePatternResolver bookmarkTreeResolver;
  private static PathMatchingResourcePatternResolver standaloneResolver;

  /**
   * Generates and loads a tree into a given ContentServiceSnapshotter, from a bookmarks folder.
   *
   * @param snapshotter The ContentServiceSnapshotter.
   * @param folderName The bookmarks folder name.
   * @param defaultPermissions The default permissions to use when no parent permissions are found.
   */
  static void importIntoContentServer(
      ContentServiceSnapshotter snapshotter,
      String folderName,
      Map<String, List<String>> defaultPermissions) {
    snapshotter.eraseAndImport(CsConstants.Paths.UI, loadBookmarks(folderName, defaultPermissions));

    try {
      snapshotter.eraseAndImport(
          CsConstants.Paths.I18N,
          standaloneResolver
              .getResource(folderName + CsConstants.Paths.I18N_JSON)
              .getInputStream());
    } catch (Exception e) {
      LOGGER.error("Could not import internationalisation file. Please check file exists.");
    }
    try {
      snapshotter.eraseAndImport(
          CsConstants.Paths.SETTINGS,
          standaloneResolver
              .getResource(folderName + CsConstants.Paths.SETTINGS_JSON)
              .getInputStream());
    } catch (Exception e) {
      LOGGER.error("Could not import settings file. Please check file exists.");
    }
  }

  /**
   * Generates a SnapshotContentTree representing the subtree containing the structure and content
   * portion of bookmarks, from a given folder.
   *
   * @param bookmarkFolder The bookmark folder.
   * @param defaultPermissions The default permissions to be used when no child or parent
   * permissions are defined.
   * @return The SnapshotContentTree.
   */
  static SnapshotContentTree loadBookmarks(
      final String bookmarkFolder, final Map<String, List<String>> defaultPermissions) {
    final File importDirectory;
    try {
      /* get URL of the directory to import */
      importDirectory = bookmarkTreeResolver.getResource(bookmarkFolder).getFile();
    } catch (IOException ioe) {
      LOGGER.error(
          "Unable to retrieve directory {} from resources. The import will fail.", bookmarkFolder);
      return null;
    }

    DEFAULT_PERMISSIONS.putAll(
        defaultPermissions != null ? defaultPermissions : ROOT_OWNER_PERMISSIONS);

    /* get filepath for root meta file */
    /* Generate the Structure and Content Trees */
    /* File path to the bookmarks */
    final File bookDir;
    try {
      /* get URL of the directory to import */
      bookDir =
          bookmarkTreeResolver
              .getResource(
                  bookmarkFolder + CsConstants.Paths.SEPARATOR + CsConstants.Paths.BOOKMARK_LISTING)
              .getFile();
    } catch (IOException e) {
      LOGGER.error(
          "Unable to load directory {} from resources. The import will fail.",
          bookmarkFolder + CsConstants.Paths.SEPARATOR + CsConstants.Paths.BOOKMARK_LISTING);
      return null;
    }

    final var contentChildren = new HashMap<String, SnapshotContentTree>();
    final var structureChildren = new HashMap<String, SnapshotContentTree>();
    createStructure(bookDir, contentChildren, structureChildren, DEFAULT_PERMISSIONS);

    final SnapshotContentTree contentTree =
        createDirectoryNode(importDirectory.listFiles()[0], true, false, ROOT_OWNER_PERMISSIONS)
            .getLeft();
    addChildrenToNode(contentTree, contentChildren);

    final SnapshotContentTree structureTree =
        createDirectoryNode(importDirectory.listFiles()[0], true, false, ROOT_OWNER_PERMISSIONS)
            .getLeft();
    addChildrenToNode(structureTree, structureChildren);

    /*
     * add Bookmark header and connect the contentTre and the structureTree as
     * children
     */
    SnapshotContentTree bookmarkContentRoot =
        new SnapshotContentTree(
            null,
            true,
            ROOT_OWNER_PERMISSIONS.get(CsConstants.Role.OWNERS),
            ROOT_OWNER_PERMISSIONS.get(CsConstants.Role.READERS),
            new HashMap<>());

    bookmarkContentRoot.putChild(CsConstants.Tree.CONTENT, contentTree, true);
    bookmarkContentRoot.putChild(CsConstants.Tree.STRUCTURE, structureTree, true);
    SnapshotContentTree ui =
        new SnapshotContentTree(
            null,
            true,
            ROOT_ONLY_PERMISSIONS.get(CsConstants.Role.OWNERS),
            ROOT_ONLY_PERMISSIONS.get(CsConstants.Role.READERS),
            new HashMap<>());
    ui.putChild(CsConstants.Tree.BOOKMARKS, bookmarkContentRoot, true);
    return ui;
  }

  /**
   * Creates a SnapshotContentTree representing a File. This is done by loading in the .json file.
   * The dir flag is used to handle root files which may have their directory flag set to true.
   *
   * @param file The file containing the json content.
   * @param dir A flag which sets isDirectory to true/false.
   * @param parentPermissions The permissions of the parent folder.
   * @return SnapshotContentTree representing the given json file and the associated meta file, and
   * its key, if contained in the metadata file.
   */
  private static IPair<SnapshotContentTree, String> createFileNode(
      File file, Boolean dir, Map<String, List<String>> parentPermissions) {
    // Fill the bookmark template from the JSON file.
    ObjectNode bookmarkTemplate;
    try {
      bookmarkTemplate =
          (ObjectNode)
              loadStreamIntoNode(
                  bookmarkTreeResolver
                      .getResource(
                          ResourceLoader.CLASSPATH_URL_PREFIX
                              + CsConstants.Paths.BOOKMARK_TEMPLATE_FILE)
                      .getInputStream());
    } catch (IOException ioe) {
      LOGGER.error(
          "Unable to retrieve bookmark template. The {} bookmark will be skipped.", file.getName());
      return null;
    }
    bookmarkTemplate.put(CsConstants.Tree.NAME, file.getName().replace(CsConstants.Paths.JSON, ""));
    JsonNode jsonNodeContent = loadFileIntoNode(file);
    bookmarkTemplate.set(CsConstants.Tree.VALUE, jsonNodeContent);
    String type = jsonNodeContent.path(CsConstants.Tree.TYPE).asText();
    bookmarkTemplate.put(CsConstants.Tree.TYPE, CsConstants.getBookmarkType(type));

    BookmarkMetadata metadata = getMetadata(file, parentPermissions);
    String description = metadata.getDescription();
    String key = metadata.getKey();
    Map<String, List<String>> permissions = metadata.getPermissions();

    if (description != null && !description.isEmpty()) {
      bookmarkTemplate.put(CsConstants.Tree.DESCRIPTION, description);
    } else {
      bookmarkTemplate.remove(CsConstants.Tree.DESCRIPTION);
    }

    String content = bookmarkTemplate.toString();
    SnapshotContentTree structure =
        new SnapshotContentTree(
            content,
            dir,
            permissions.get(CsConstants.Role.OWNERS),
            permissions.get(CsConstants.Role.READERS),
            new HashMap<>());

    return new Pair<>(structure, key);
  }

  /**
   * Creates a SnapshotContentTree representing a directory. Permissions are loaded from a
   * _permissions.json file, or, if unavailable, from the permissions of the parent folder.
   *
   * @param folder The folder.
   * @param directoryFlag A flag which sets isDirectory to true/false.
   * @param contentFlag A flag which determines whether content should be loaded for the folder.
   * @param parentPermissions The permissions of the parent folder.
   * @return The SnapshotContentTree and its key, if available in the metadata file.
   */
  private static IPair<SnapshotContentTree, String> createDirectoryNode(
      File folder,
      Boolean directoryFlag,
      Boolean contentFlag,
      Map<String, List<String>> parentPermissions) {
    // get the contents of the given files
    InputStream folderTemplateInputStream;
    try {
      folderTemplateInputStream =
          bookmarkTreeResolver
              .getResource(
                  ResourceLoader.CLASSPATH_URL_PREFIX + CsConstants.Paths.FOLDER_TEMPLATE_FILE)
              .getInputStream();
    } catch (IOException ioe) {
      LOGGER.error(
          "Unable to retrieve the folder template. The {} folder will be skipped.",
          folder.getName());
      return null;
    }
    JsonNode templateNode = loadStreamIntoNode(folderTemplateInputStream);
    ObjectNode contentNode = (ObjectNode) templateNode.path(CsConstants.Tree.CONTENT);

    BookmarkMetadata metadata = getMetadata(folder, parentPermissions);
    String description = metadata.getDescription();
    String key = metadata.getKey();
    Map<String, List<String>> permissions = metadata.getPermissions();

    if (description != null && !description.isEmpty()) {
      contentNode.put(CsConstants.Tree.DESCRIPTION, description);
    } else {
      contentNode.remove(CsConstants.Tree.DESCRIPTION);
    }

    Map<String, SnapshotContentTree> children = new HashMap<>();

    /*
     * Set isDirectory to true if flag is true, load content if the content flag is
     * true
     */
    String content = contentNode.put(CsConstants.Tree.NAME, folder.getName()).toString();
    SnapshotContentTree structure =
        new SnapshotContentTree(
            directoryFlag && !contentFlag ? null : content,
            directoryFlag,
            permissions.get(CsConstants.Role.OWNERS),
            permissions.get(CsConstants.Role.READERS),
            children);

    return new Pair<>(structure, key);
  }

  /**
   * Recursively generates the structure SnapshotContentTree, based on the folder passed in.
   *
   * @param root The current directory to add to the structureTree.
   * @param content A map of index to node for the content.
   * @param structure A map of index to node for the structure.
   * @param parentPermissions The permissions of the parent folder.
   */
  private static void createStructure(
      File root,
      Map<String, SnapshotContentTree> content,
      Map<String, SnapshotContentTree> structure,
      Map<String, List<String>> parentPermissions) {

    File[] files =
        root.listFiles(
            file ->
                (file.isDirectory() || file.isFile())
                    && !file.getName().contains(CsConstants.Paths.METADATA_FILE));

    BookmarkMetadata metadata;
    try {
      metadata = getMetadata(root, parentPermissions);
    } catch (Exception e) {
      LOGGER.warn("Could not retrieve permissions");
      return;
    }

    for (File file : files) {
      boolean directory = file.isDirectory();
      Map<String, SnapshotContentTree> localStructureChildren = new HashMap<>();
      if (directory) {
        createStructure(file, content, localStructureChildren, metadata.getPermissions());
      }

      IPair<SnapshotContentTree, String> structureNode =
          createDirectoryNode(file, true, false, metadata.getPermissions());
      IPair<SnapshotContentTree, String> contentNode =
          directory
              ? createDirectoryNode(file, false, false, metadata.getPermissions())
              : createFileNode(file, false, metadata.getPermissions());

      if (file.isDirectory()) {
        addChildrenToNode(structureNode.getLeft(), localStructureChildren);
      }

      SnapshotContentTree structureTree = structureNode.getLeft();
      SnapshotContentTree contentTree = contentNode.getLeft();

      String persistedStructureKey = structureNode.getRight();
      String persistedContentKey = contentNode.getRight();
      String generatedKey =
          String.valueOf(Objects.hash(root.getName(), contentTree, structureTree));

      String structureKey =
          persistedStructureKey == null || persistedStructureKey.isEmpty()
              ? generatedKey
              : persistedStructureKey;
      String contentKey =
          persistedContentKey == null || persistedContentKey.isEmpty()
              ? generatedKey
              : persistedContentKey;

      if (!structureKey.equals(contentKey)) {
        LOGGER.error(
            "Bookmark import will fail due to bookmark {} having separate ids in structure and"
                + " content.",
            root.getName());
      }

      structure.put(structureKey, structureTree);
      content.put(contentKey, contentTree);
    }
  }

  /**
   * Adds all entries in a map as children, in a given node.
   *
   * @param node The SnapshotContentTree node.
   * @param children The SnapshotContentTree children.
   */
  private static void addChildrenToNode(
      SnapshotContentTree node, Map<String, SnapshotContentTree> children) {
    children.forEach((index, childNode) -> node.putChild(index, childNode, true));
  }

  /**
   * Attempts to retrieve file/folder permissions from a file. If the file does not exist, it
   * defaults to the parent permissions.
   *
   * @param file The file or folder.
   * @param parentPermissions The owners and readers of the parent folder.
   */
  private static BookmarkMetadata getMetadata(
      File file, Map<String, List<String>> parentPermissions) {
    BookmarkMetadata metadata = new BookmarkMetadata();
    metadata.permissions = parentPermissions;
    final File[] foundMetadataFiles =
        file.getParentFile()
            .listFiles(
                child ->
                    child
                        .getName()
                        .equals(
                            file.getName().replace(CsConstants.Paths.JSON, "")
                                + CsConstants.Paths.METADATA_FILE));

    if (foundMetadataFiles != null && foundMetadataFiles.length > 0) {
      JsonNode metadataJsonNode = loadFileIntoNode(foundMetadataFiles[0]);
      Optional<IPair<JsonNode, JsonNode>> pair = retrievePermissions(metadataJsonNode);
      pair.ifPresent(
          jsonNodeJsonNodeIPair -> metadata.setPermissions(getPermissions(jsonNodeJsonNodeIPair)));
      metadata.setDescription(metadataJsonNode.path(CsConstants.Tree.DESCRIPTION).asText());
      metadata.setKey(metadataJsonNode.path(CsConstants.Tree.KEY).asText());
    }

    return metadata;
  }

  /** Metadata for a bookmark. */
  @Data
  protected static class BookmarkMetadata {

    /** Description of the bookmark. */
    private String description;
    /** List of permissions for the bookmark. */
    private Map<String, List<String>> permissions;
    /** Key of the bookmark. */
    private String key;
  }

  /**
   * Gets the arrays of owners and readers from a pair of JsonNodes.
   *
   * @param permissions the pair of JsonNodes to parse
   * @return a map containing the owners and readers
   */
  private static Map<String, List<String>> getPermissions(IPair<JsonNode, JsonNode> permissions) {
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, List<String>> ownAndRead = new HashMap<>();
    List<String> owners = null;
    List<String> readers = null;
    try {
      owners =
          Arrays.asList(objectMapper.readValue(permissions.getLeft().toString(), String[].class));
      readers =
          Arrays.asList(objectMapper.readValue(permissions.getRight().toString(), String[].class));
    } catch (IOException e) {
      LOGGER.warn(e.toString());
    }

    ownAndRead.put(CsConstants.Role.OWNERS, owners);
    ownAndRead.put(CsConstants.Role.READERS, readers);
    return ownAndRead;
  }

  /**
   * Gets the OWNERS and READERS portion of a given JsonNode and returns this as a pair of
   * JsonNodes.
   *
   * @param jsonNodeMeta the JsonNode to parse
   * @return a pair of JsonNodes representing owners and readers
   */
  private static Optional<IPair<JsonNode, JsonNode>> retrievePermissions(JsonNode jsonNodeMeta) {
    Optional<IPair<JsonNode, JsonNode>> pair = Optional.empty();
    if (jsonNodeMeta.hasNonNull(CsConstants.Role.OWNERS)
        || jsonNodeMeta.hasNonNull(CsConstants.Role.READERS)) {
      pair =
          Optional.of(
              new Pair<>(
                  jsonNodeMeta.path(CsConstants.Role.OWNERS),
                  jsonNodeMeta.path(CsConstants.Role.READERS)));
    }

    return pair;
  }

  /**
   * Loads the contents of a stream into a JsonNode.
   *
   * @param stream The stream to load.
   * @return The contents of the stream, as a JsonNode.
   */
  private static JsonNode loadStreamIntoNode(InputStream stream) {
    JsonNode loadedStream;
    try {
      ObjectMapper mapper = new ObjectMapper();
      loadedStream = mapper.readTree(stream);
    } catch (Exception e) {
      LOGGER.warn("Unable to retrieve contents of the input stream.");
      return JsonNodeFactory.instance.objectNode();
    }
    return loadedStream;
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
      loadedFile = loadStreamIntoNode(new FileInputStream(file));
    } catch (FileNotFoundException fnfe) {
      LOGGER.warn("Unable to find file " + file.getName());
    }
    return loadedFile;
  }

  /**
   * Sets the bookmark tree resource resolver to use for the lifetime of the application.
   *
   * @param toSet The resource resolver to set.
   */
  public static void setBookmarkTreeResolver(PathMatchingResourcePatternResolver toSet) {
    bookmarkTreeResolver = toSet;
  }

  /**
   * Sets the standalone file resource resolver to use for the lifetime of the application.
   *
   * @param toSet The resource resolver to set.
   */
  public static void setStandaloneResolver(PathMatchingResourcePatternResolver toSet) {
    standaloneResolver = toSet;
  }
}
