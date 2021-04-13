/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

import org.checkerframework.checker.nullness.qual.Nullable;
import software.aws.rds.jdbc.postgresql.ca.HostInfo;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HostUtils {
  private static final Pattern CONNECTION_STRING_PATTERN = Pattern.compile("^(?<host>.*?)(?::(?<port>[^:]*))?$");

  /**
   * Splits a URL into its host/port components and returns this information as a {@link HostSpec}
   *
   * @param url the URL to process
   * @return a {@link HostSpec} representing the host/port components of the given URL, or null if
   *         there was a problem parsing the URL
   */
  public static @Nullable HostSpec parse(@Nullable String url) {
    if (StringUtils.isNullOrEmpty(url)) {
      return null;
    }

    Matcher matcher = CONNECTION_STRING_PATTERN.matcher(url);
    if (!matcher.matches()) {
      return null;
    }

    String hostName = matcher.group("host");
    String portAsString = getUtf(StringUtils.safeTrim(matcher.group("port")));

    if (StringUtils.isNullOrEmpty(hostName)) {
      return null;
    }

    int portAsInteger = HostInfo.NO_PORT;
    if (!StringUtils.isNullOrEmpty(portAsString)) {
      try {
        portAsInteger = Integer.parseInt(portAsString);
      } catch (NumberFormatException e) {
        return null;
      }
    }

    return new HostSpec(hostName, portAsInteger);
  }

  /**
   * Convert the supplied URL to UTF string
   *
   * @param url the URL to convert
   * @return the converted URL
   */
  private static @Nullable String getUtf(@Nullable String url) {
    if (StringUtils.isNullOrEmpty(url)) {
      return url;
    }

    try {
      return URLDecoder.decode(url, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      return "";
    }
  }
}
