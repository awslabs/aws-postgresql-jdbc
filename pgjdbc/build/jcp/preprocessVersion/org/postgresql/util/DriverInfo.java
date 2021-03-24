/*
 * Copyright (c) 2017, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

/**
 * Utility class with constants of Driver information.
 */
public final class DriverInfo {

  private DriverInfo() {
  }

  // Driver name
  public static final String DRIVER_NAME = "AWS JDBC Driver for PostgreSQL";
  public static final String DRIVER_SHORT_NAME = "AWS PgJDBC";
  public static final String DRIVER_VERSION = "42.3.0-SNAPSHOT";
  public static final String DRIVER_FULL_NAME = DRIVER_NAME + " " + DRIVER_VERSION;

  // Driver version
  public static final int MAJOR_VERSION = 42;
  public static final int MINOR_VERSION = 3;
  public static final int PATCH_VERSION = 0;

  // JDBC specification
  public static final String JDBC_VERSION = "4.2";
  public static final int JDBC_MAJOR_VERSION = JDBC_VERSION.charAt(0) - '0';
  public static final int JDBC_MINOR_VERSION = JDBC_VERSION.charAt(2) - '0';

}
