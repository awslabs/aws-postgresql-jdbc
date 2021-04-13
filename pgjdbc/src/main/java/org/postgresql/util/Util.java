/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

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
   * Get the name of the package that the supplied class belongs to
   *
   * @param clazz the {@link Class} to analyze
   * @return the name of the package that the supplied class belongs to
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

  private static final ConcurrentMap<Class<?>, Boolean> isJdbcInterfaceCache = new ConcurrentHashMap<>();

  /**
   * Check whether the given class implements a JDBC interface defined in a JDBC package. See {@link #isJdbcPackage(String)}
   * Calls to this function are cached for improved efficiency.
   *
   * @param clazz the class to analyze
   * @return true if the given class implements a JDBC interface
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
   * Check whether the given package is a JDBC package
   *
   * @param packageName the name of the package to analyze
   * @return true if the given package is a JDBC package
   */
  public static boolean isJdbcPackage(@Nullable String packageName) {
    return packageName != null
        && (packageName.startsWith("java.sql")
        || packageName.startsWith("javax.sql")
        || packageName.startsWith(shadingPrefix("org.postgresql")));
  }

  private static final ConcurrentMap<Class<?>, Class<?>[]> getImplementedInterfacesCache = new ConcurrentHashMap<>();

  /**
   * Get the {@link Class} objects corresponding to the interfaces implemented by the given class. Calls to this function
   * are cached for improved efficiency.
   *
   * @param clazz the class to analyze
   * @return the interfaces implemented by the given class
   */
  public static Class<?>[] getImplementedInterfaces(Class<?> clazz) {
    Class<?>[] implementedInterfaces = Util.getImplementedInterfacesCache.get(clazz);
    if (implementedInterfaces != null) {
      return implementedInterfaces;
    }

    Set<Class<?>> interfaces = new LinkedHashSet<>();
    Class<?> superClass = clazz;
    do {
      Collections.addAll(interfaces, superClass.getInterfaces());
    } while ((superClass = superClass.getSuperclass()) != null);

    implementedInterfaces = interfaces.toArray(new Class<?>[0]);
    Class<?>[] oldValue = Util.getImplementedInterfacesCache.putIfAbsent(clazz, implementedInterfaces);
    if (oldValue != null) {
      implementedInterfaces = oldValue;
    }

    return implementedInterfaces;
  }

  /**
   * For the given {@link Throwable}, return a formatted string representation of the stack trace. This method is
   * provided for logging purposes.
   *
   * @param t the throwable containing the stack trace that we want to transform into a string
   * @param callingClass the class that is calling this method
   * @return the formatted string representation of the stack trace attached to the given {@link Throwable}
   */
  public static String stackTraceToString(Throwable t, Class callingClass) {
    StringBuilder buffer = new StringBuilder();
    buffer.append("\n\n========== [");
    buffer.append(callingClass.getName());
    buffer.append("]: Exception Detected: ==========\n\n");

    buffer.append(t.getClass().getName());

    String exceptionMessage = t.getMessage();

    if (exceptionMessage != null) {
      buffer.append("Message: ");
      buffer.append(exceptionMessage);
    }

    StringWriter out = new StringWriter();

    PrintWriter printOut = new PrintWriter(out);

    t.printStackTrace(printOut);

    buffer.append("Stack Trace:\n\n");
    buffer.append(out.toString());
    buffer.append("============================\n\n\n");

    return buffer.toString();
  }
}
