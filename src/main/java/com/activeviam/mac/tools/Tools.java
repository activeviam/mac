/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac.tools;

import com.activeviam.activepivot.server.impl.private_.observability.memory.MemoryStatisticSerializerUtil;
import com.activeviam.tech.core.api.exceptions.ActiveViamRuntimeException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;

/**
 * Tool class providing CLI methods.
 *
 * @author ActiveViam
 */
public class Tools {

  private static final Logger LOGGER = Logger.getLogger("mac.tools");

  public static void main(final String[] args) {
    if ("extract".equals(args[0])) {
      extractSnappyFileOrDirectory(args[1]);
    } else {
      LOGGER.severe(() -> "Unsupported command. Got " + Arrays.toString(args));
    }
  }

  public static void extractSnappyFileOrDirectory(final String path) {
    Objects.requireNonNull(path, "file or directory to uncompress was not provided");

    final Path asPath = Paths.get(path);
    extractSnappyFileOrDirectory(asPath);
  }

  public static void extractSnappyFileOrDirectory(final Path path) {
    assertFileExists(path);

    if (Files.isDirectory(path)) {
      extractSnappyDirectory(path);
    } else {
      extractSnappyFile(path);
    }
  }

  protected static void assertFileExists(final Path path) {
    if (!Files.exists(path)) {
      throw new IllegalArgumentException(path + " does not point to a valid file or directory");
    }
  }

  public static void extractSnappyDirectory(final Path path) {
    try (final var fileStream = Files.list(path)) {
      fileStream.forEach(Tools::extractSnappyFileOrDirectory);
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
  }

  public static void extractSnappyFile(final Path path) {
    final String pathAsString = path.toString();
    final String extension = MemoryStatisticSerializerUtil.COMPRESSED_FILE_EXTENSION;
    boolean isCompressedFile = pathAsString.endsWith("." + extension);
    if (!isCompressedFile) {
      LOGGER.info(
          () ->
              "File "
                  + pathAsString
                  + " was skipped as it did not match the list of compressed extensions: "
                  + MemoryStatisticSerializerUtil.COMPRESSED_FILE_EXTENSION);
      return;
    }

    final String subpart =
        pathAsString.substring(0, pathAsString.length() - extension.length() - 1);
    final Path uncompressedPath =
        Paths.get(subpart.endsWith(".json") ? subpart : subpart + ".json");
    final InputStream rawInputStream;
    try {
      rawInputStream = new FileInputStream(path.toFile());
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(String.format("File `%s` does not exist", path), e);
    }
    try {
      final InputStream inputStream;
      try {
        inputStream = new FramedSnappyCompressorInputStream(rawInputStream);
      } catch (IOException e) {
        throw new ActiveViamRuntimeException(
            String.format("Cannot read `%s` as a Snappy file", path), e);
      }
      try {
        Files.copy(inputStream, uncompressedPath, StandardCopyOption.REPLACE_EXISTING);
      } catch (IOException e) {
        throw new ActiveViamRuntimeException(
            String.format("Failed to extract `%s` to `%s`", path, uncompressedPath), e);
      }
    } finally {
      try {
        rawInputStream.close();
      } catch (IOException e) {
        LOGGER.log(Level.SEVERE, "Failed to close the input stream", e);
      }
    }
    LOGGER.log(Level.INFO, "File `{0}` extracted to `{1}`", new Object[] {path, uncompressedPath});
  }
}
