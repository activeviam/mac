/*
 * (C) ActiveViam 2019
 * ALL RIGHTS RESERVED. This material is the CONFIDENTIAL and PROPRIETARY
 * property of ActiveViam. Any unauthorized use
 * reproduction or transfer of this material is strictly prohibited
 */
package com.activeviam.mac;

import com.qfs.pivot.monitoring.impl.MemoryStatisticSerializerUtil;
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
import org.xerial.snappy.SnappyFramedInputStream;

/**
 * Tool class providing CLI methods.
 *
 * @author ActiveViam
 */
public class Tools {

  public static void main(final String[] args) {
    if ("extract".equals(args[0])) {
      extractSnappyFileOrDirectory(args[1]);
    } else {
      System.err.println("Unsupported command. Got " + Arrays.toString(args));
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
      throw new RuntimeException(path + " does not point to a valid file or directory");
    }
  }

  public static void extractSnappyDirectory(final Path path) {
    try {
      Files.list(path).forEach(Tools::extractSnappyFileOrDirectory);
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
  }

  public static void extractSnappyFile(final Path path) {
    final String pathAsString = path.toString();
    final String extension = MemoryStatisticSerializerUtil.COMPRESSED_FILE_EXTENSION;
    boolean isCompressedFile = pathAsString.endsWith("." + extension);
    if (!isCompressedFile) {
      System.out.println(
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
    final InputStream inputStream;
    try {
      inputStream = new SnappyFramedInputStream(rawInputStream);
    } catch (IOException e) {
      throw new RuntimeException(String.format("Cannot read `%s` as a Snappy file", path), e);
    }
    try {
      Files.copy(inputStream, uncompressedPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to extract `%s` to `%s`", path, uncompressedPath), e);
    }
    System.out.printf("File `%s` extracted to `%s`%n", path, uncompressedPath);
  }
}
