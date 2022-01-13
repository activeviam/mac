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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
	 * @param snapshotter        The ContentServiceSnapshotter.
	 * @param folderName         The ui folder name.
	 * @param defaultPermissions The default permissions to use when no parent permissions are found.
	 */
	static void importIntoContentServer(
			ContentServiceSnapshotter snapshotter,
			String folderName,
			Map<String, List<String>> defaultPermissions) {
		PERMISSIONS.putAll(defaultPermissions != null ? defaultPermissions : DEFAULT_PERMISSIONS);
		snapshotter.eraseAndImport(ContentServerConstants.Paths.UI, Paths.INITIAL_CONTENT);
		snapshotter.eraseAndImport(folderName + Paths.SEPARATOR + Tree.DASHBOARDS,
				loadDashboards(Paths.DASHBOARDS));
	}

	/**
	 * Generates a SnapshotContentTree representing the subtree containing the dashboards, from a
	 * given folder.
	 *
	 * @param dashboardFolder the dashboard folder.
	 * @return the SnapshotContentTree.
	 */
	static SnapshotContentTree loadDashboards(
			final String dashboardFolder) {
		if (getSubDirectory(dashboardFolder) == null) {
			return null;
		}
		final File contentDirectory = getSubDirectory(dashboardFolder + Paths.CONTENT);
		final SnapshotContentTree contentTree = createOneLevelDirectoryTree(contentDirectory);
		final File structureDirectory = getSubDirectory(dashboardFolder + Paths.STRUCTURE);
		final SnapshotContentTree structureTree = createTwoLevelsDirectoryTree(structureDirectory);
		final File thumbnailsDirectory = getSubDirectory(dashboardFolder + Paths.THUMBNAILS);
		final SnapshotContentTree thumbnailsTree = createOneLevelDirectoryTree(thumbnailsDirectory);

		final SnapshotContentTree dashboardsTree =
				new SnapshotContentTree(
						null,
						true,
						DEFAULT_PERMISSIONS.get(ContentServerConstants.Role.OWNERS),
						DEFAULT_PERMISSIONS.get(ContentServerConstants.Role.READERS),
						new HashMap<>());
		dashboardsTree.putChild(ContentServerConstants.Tree.CONTENT, contentTree, true);
		dashboardsTree.putChild(ContentServerConstants.Tree.STRUCTURE, structureTree, true);
		dashboardsTree.putChild(ContentServerConstants.Tree.THUMBNAILS, thumbnailsTree, true);
		return dashboardsTree;
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
	 * Creates a {@link SnapshotContentTree} from the files of a directory. For each file, we add a
	 * leaf to the root {@link SnapshotContentTree}. The key of the leaf is the name of the file
	 * without the json extension. The content of the leaf is a leaf {@link SnapshotContentTree}
	 * containing the content of the file.
	 *
	 * @param root The current directory to add to the structureTree.
	 */
	private static SnapshotContentTree createOneLevelDirectoryTree(File root) {
		final File[] files = root.listFiles(File::isFile);
		if (files == null) {
			return null;
		}
		Map<String, SnapshotContentTree> nodes = new HashMap<>();
		for (File file : files) {
			final JsonNode jsonNodeContent = loadFileIntoNode(file);
			final SnapshotContentTree contentNode =
					new SnapshotContentTree(
							jsonNodeContent.toString(),
							false,
							PERMISSIONS.get(ContentServerConstants.Role.OWNERS),
							PERMISSIONS.get(ContentServerConstants.Role.READERS),
							new HashMap<>());
			nodes.put(file.getName().replace(Paths.JSON, ""), contentNode);
		}
		return
				new SnapshotContentTree(
						null,
						true,
						PERMISSIONS.get(ContentServerConstants.Role.OWNERS),
						PERMISSIONS.get(ContentServerConstants.Role.READERS),
						nodes);
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
	private static SnapshotContentTree createTwoLevelsDirectoryTree(
			File root) {
		final File[] subdirectories = root.listFiles(File::isFile);
		if (subdirectories == null) {
			return null;
		}
		Map<String, SnapshotContentTree> structureNodes = new HashMap<>();
		for (File file : subdirectories) {
			final File[] files = file.listFiles(File::isFile);
			if (files != null && files.length > 1) {
				final JsonNode jsonNodeContent = loadFileIntoNode(files[0]);
				final SnapshotContentTree metadataNode =
						new SnapshotContentTree(
								jsonNodeContent.toString(),
								false,
								PERMISSIONS.get(ContentServerConstants.Role.OWNERS),
								DEFAULT_PERMISSIONS.get(ContentServerConstants.Role.READERS),
								new HashMap<>());
				Map<String, SnapshotContentTree> metadataNodes = new HashMap<>();
				metadataNodes.put(file.getName() + Paths.METADATA, metadataNode);
				final SnapshotContentTree structureNode =
						new SnapshotContentTree(
								null,
								true,
								PERMISSIONS.get(ContentServerConstants.Role.OWNERS),
								PERMISSIONS.get(ContentServerConstants.Role.READERS),
								metadataNodes);
				structureNodes.put(file.getName(), structureNode);
			}
		}
		return
				new SnapshotContentTree(
						null,
						true,
						PERMISSIONS.get(ContentServerConstants.Role.OWNERS),
						PERMISSIONS.get(ContentServerConstants.Role.READERS),
						structureNodes);
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
	 * Loads the contents of a stream into a JsonNode.
	 *
	 * @param stream The stream to load.
	 * @return The contents of the stream, as a JsonNode.
	 */
	private static JsonNode loadStreamIntoNode(InputStream stream) {
		final JsonNode loadedStream;
		try {
			final ObjectMapper mapper = new ObjectMapper();
			loadedStream = mapper.readTree(stream);
		} catch (Exception e) {
			LOGGER.warn("Unable to retrieve contents of the input stream.");
			return JsonNodeFactory.instance.objectNode();
		}
		return loadedStream;
	}

}
