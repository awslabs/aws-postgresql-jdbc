/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Util {

  private static @Nullable String shadingPrefix = null;
  private static final Object lockObj = new Object();

  /**
   * Returns the package name of the given class.
   * Using clazz.getPackage().getName() is not an alternative because under some class loaders the method getPackage() just returns null.
   *
   * @param clazz
   *            the Class from which to get the package name
   * @return the package name
   */
  public static String getPackageName(Class<?> clazz) {
    String fqcn = clazz.getName();
    int classNameStartsAt = fqcn.lastIndexOf('.');
    if (classNameStartsAt > 0) {
      return fqcn.substring(0, classNameStartsAt);
    }
    return "";
  }

  /**
   * Adds the shading package prefix to a class name and returns it.
   *
   * @param clazzName Class name.
   *
   * @return the shading prefix "software.aws.rds.jdbc.shading.{clazzName}" or just {clazzName}
   */
  public static String shadingPrefix(String clazzName) {
    if (shadingPrefix == null) {
      // lazy init
      synchronized (lockObj) {
        if (shadingPrefix == null) {
          shadingPrefix = getPackageName(Util.class).replaceAll("org.postgresql.util", "");
        }
      }
    }
    if ("".equals(shadingPrefix)) {
      return clazzName;
    }
    return shadingPrefix + clazzName;
  }

  /** Cache for the JDBC interfaces already verified */
  private static final ConcurrentMap<Class<?>, Boolean> isJdbcInterfaceCache = new ConcurrentHashMap<>();

  /**
   * Recursively checks for interfaces on the given class to determine if it implements a java.sql, javax.sql or org.postgresql interface.
   *
   * @param clazz
   *            The class to investigate.
   * @return boolean
   */
  public static boolean isJdbcInterface(Class<?> clazz) {
    if (Util.isJdbcInterfaceCache.containsKey(clazz)) {
      return (Util.isJdbcInterfaceCache.get(clazz));
    }

    if (clazz.isInterface()) {
      try {
        Package classPackage = clazz.getPackage();
        if (classPackage != null && isJdbcPackage(classPackage.getName())) {
          Util.isJdbcInterfaceCache.putIfAbsent(clazz, true);
          return true;
        }
      } catch (Exception ex) {
        /*
         * We may experience a NPE from getPackage() returning null, or class-loading facilities.
         * This happens when this class is instrumented to implement runtime-generated interfaces.
         */
      }
    }

    for (Class<?> iface : clazz.getInterfaces()) {
      if (isJdbcInterface(iface)) {
        Util.isJdbcInterfaceCache.putIfAbsent(clazz, true);
        return true;
      }
    }

    if (clazz.getSuperclass() != null && isJdbcInterface(clazz.getSuperclass())) {
      Util.isJdbcInterfaceCache.putIfAbsent(clazz, true);
      return true;
    }

    Util.isJdbcInterfaceCache.putIfAbsent(clazz, false);
    return false;
  }

  /**
   * Check if the package name is a known JDBC package.
   *
   * @param packageName
   *            The package name to check.
   * @return boolean
   */
  public static boolean isJdbcPackage(@Nullable String packageName) {
    return packageName != null
        && (packageName.startsWith("java.sql")
        || packageName.startsWith("javax.sql")
        || packageName.startsWith(shadingPrefix("org.postgresql")));
  }

  /** Cache for the implemented interfaces searched. */
  private static final ConcurrentMap<Class<?>, Class<?>[]> implementedInterfacesCache = new ConcurrentHashMap<>();

  /**
   * Retrieves a list with all interfaces implemented by the given class. If possible gets this information from a cache instead of navigating through the
   * object hierarchy. Results are stored in a cache for future reference.
   *
   * @param clazz
   *            The class from which the interface list will be retrieved.
   * @return
   *         An array with all the interfaces for the given class.
   */
  public static Class<?>[] getImplementedInterfaces(Class<?> clazz) {
    Class<?>[] implementedInterfaces = Util.implementedInterfacesCache.get(clazz);
    if (implementedInterfaces != null) {
      return implementedInterfaces;
    }

    Set<Class<?>> interfaces = new LinkedHashSet<>();
    Class<?> superClass = clazz;
    do {
      Collections.addAll(interfaces, superClass.getInterfaces());
    } while ((superClass = superClass.getSuperclass()) != null);

    implementedInterfaces = interfaces.toArray(new Class<?>[0]);
    Class<?>[] oldValue = Util.implementedInterfacesCache.putIfAbsent(clazz, implementedInterfaces);
    if (oldValue != null) {
      implementedInterfaces = oldValue;
    }

    return implementedInterfaces;
  }

  /**
   * Method used to build a a string for a log message. It lists out the stack trace to keep track of
   * which methods were called before the exception.
   *
   * @param t The throwable error message
   * @param callingClass The class in which the method was called
   * @return A string for a log message
   */
  public static String stackTraceToString(Throwable t, Class callingClass) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("\n\n[");
    buffer.append(callingClass.getName());
    buffer.append("]: EXCEPTION STACK TRACE:\n\n");

    buffer.append(t.getClass().getName());

    String exceptionMessage = t.getMessage();

    if (exceptionMessage != null) {
      buffer.append("MESSAGE: ");
      buffer.append(exceptionMessage);
    }

    StringWriter out = new StringWriter();

    PrintWriter printOut = new PrintWriter(out);

    t.printStackTrace(printOut);

    buffer.append("STACKTRACE:\n\n");
    buffer.append(out.toString());

    return buffer.toString();
  }

  /**
   * Check if the supplied string is null or empty
   *
   * @param s the string to analyze
   * @return true if the supplied string is null or empty
   */
  @EnsuresNonNullIf(expression = "#1", result = false)
  public static boolean isNullOrEmpty(@Nullable String s) {
    return s == null || s.equals("");
  }
}
