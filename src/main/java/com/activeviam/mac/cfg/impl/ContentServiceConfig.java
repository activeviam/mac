/*
 * (C) ActiveViam 2015-2016
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use,
 * reproduction or transfer of this material is strictly prohibited
 */

package com.activeviam.mac.cfg.impl;

import static com.activeviam.tech.contentserver.storage.api.ContentServiceSnapshotter.create;

import com.activeviam.mac.cfg.security.impl.SecurityConfig;
import com.activeviam.tech.contentserver.storage.api.IContentService;
import com.activeviam.tech.core.internal.monitoring.JmxOperation;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants.Paths;
import com.activeviam.tools.bookmark.constant.impl.ContentServerConstants.Role;
import com.activeviam.tools.bookmark.impl.BookmarkTool;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

/**
 * Spring configuration managing the bookmarks stored in the Content Service.
 *
 * <p>The Content Service itself is created by the Atoti Server starter, and configured through the
 * {@code atoti.server.content-service.*} properties.
 *
 * @author ActiveViam
 */
@Configuration
@RequiredArgsConstructor
public class ContentServiceConfig {

  /**
   * The name of the property that controls whether or not to force the reloading of the predefined
   * bookmarks even if they were already loaded previously.
   */
  public static final String FORCE_BOOKMARK_RELOAD_PROPERTY = "bookmarks.reloadOnStartup";

  /** The name of the property that precise the name of the folder the bookmarks are in. */
  public static final String UI_FOLDER_PROPERTY = "bookmarks.folder";

  private final Environment env;

  private final IContentService contentService;

  private Map<String, List<String>> defaultBookmarkPermissions() {
    return Map.of(
        Role.OWNERS,
        List.of(SecurityConfig.ROLE_USER),
        Role.READERS,
        List.of(SecurityConfig.ROLE_USER));
  }

  /**
   * Exports the bookmarks from the Content Service.
   *
   * <p>This is used to back up the defined bookmarks to load them at boot time.
   */
  @JmxOperation(
      name = "exportBookMarks",
      desc = "Export the current bookmark structure",
      params = {"destination"})
  public void exportBookMarks(String destination) {
    BookmarkTool.exportBookmarks(create(this.contentService.withRootPrivileges()), destination);
  }

  /** Loads the bookmarks packaged with the application. */
  public void loadPredefinedBookmarks() {
    final var service = this.contentService.withRootPrivileges();
    if (!service.exists("/" + Paths.UI) || shouldReloadBookmarks()) {
      BookmarkTool.importBookmarks(create(service), defaultBookmarkPermissions());
    }
  }

  /** Returns true if the bookmarks must be reloaded even if already present. */
  private boolean shouldReloadBookmarks() {
    return this.env.getProperty(FORCE_BOOKMARK_RELOAD_PROPERTY, Boolean.class, false);
  }
}
