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

public class ConnectionUrlParser {
  private static final Pattern GENERIC_HOST_PTRN = Pattern.compile("^(?<host>.*?)(?::(?<port>[^:]*))?$");

  /**
   * Splits a URL into its host/port components and returns this information as a {@link HostSpec}
   *
   * @param url the URL to process
   * @return a {@link HostSpec} representing the host/port components of the given URL, or null if
   *         there was a problem parsing the URL
   */
  public static @Nullable HostSpec parseHostPortPair(@Nullable String url) {
    if (StringUtils.isNullOrEmpty(url)) {
      return null;
    }
    Matcher matcher = GENERIC_HOST_PTRN.matcher(url);
    if (matcher.matches()) {
      String host = matcher.group("host");
      String portAsString = decode(StringUtils.safeTrim(matcher.group("port")));
      int portAsInteger = HostInfo.NO_PORT;
      if (!StringUtils.isNullOrEmpty(portAsString)) {
        try {
          portAsInteger = Integer.parseInt(portAsString);
        } catch (NumberFormatException e) {
          return null;
        }
      }
      if (host != null) {
        return new HostSpec(host, portAsInteger);
      } else {
        return null;
      }
    }
    return null;
  }

  /**
   * Decode the supplied URL
   *
   * @param url the URL to decode
   * @return the decoded URL
   */
  private static @Nullable String decode(@Nullable String url) {
    if (StringUtils.isNullOrEmpty(url)) {
      return url;
    }
    try {
      return URLDecoder.decode(url, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      // Ignore - this exception will not occur
    }
    return "";
  }
}
