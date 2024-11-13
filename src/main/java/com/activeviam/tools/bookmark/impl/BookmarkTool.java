/*
 * (C) ActiveViam 2020
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.tools.bookmark.impl;

import com.activeviam.tech.contentserver.storage.api.ContentServiceSnapshotter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
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
   * @param defaultPermissions The permissions to use when not explicitly defining permissions for
   *     any bookmark path.
   */
  public static void importBookmarks(
      ContentServiceSnapshotter snapshotter, Map<String, List<String>> defaultPermissions) {
    PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver();
    JsonUiToContentServer.setResourcePatternResolver(resolver);
    JsonUiToContentServer.importIntoContentServer(snapshotter, defaultPermissions);
  }

  /**
   * Exports content server bookmarks into a directory structure representing the UI bookmark
   * structure.
   *
   * @param snapshotter The content service snapshotter to use for the export.
   * @param folderName The folder into which the bookmarks should be exported.
   */
  public static void exportBookmarks(ContentServiceSnapshotter snapshotter, String folderName) {
    ObjectMapper mapper = new ObjectMapper();
    ContentServerToJsonUi.setMapper(mapper);
    ContentServerToJsonUi.setWriter(mapper.writer(new DefaultPrettyPrinter()));
    ContentServerToJsonUi.export(snapshotter, folderName);
  }
}
