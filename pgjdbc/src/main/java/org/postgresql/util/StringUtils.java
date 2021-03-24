/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

public class StringUtils {

  /**
   * Checks whether or not a string is null or empty
   * @param s The string to check
   * @return True if the string is null or empty
   */
  @EnsuresNonNullIf(expression = "#1", result = false)
  public static boolean isNullOrEmpty(@Nullable String s) {
    return s == null || s.equals("");
  }

  /**
   * Checks whether or not a string is safe to trim. It checks if the string is empty or null first
   * before attempting to trim.
   * @param toTrim The string to safe trim
   * @return A trimmed string if the string is not null or empty
   */
  public static @Nullable String safeTrim(@Nullable String toTrim) {
    return isNullOrEmpty(toTrim) ? toTrim : toTrim.trim();
  }
}
