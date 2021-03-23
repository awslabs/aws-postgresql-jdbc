/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.util;

import java.util.regex.Pattern;

public class IpAddressUtils {
  private static final Pattern IP_V4 =
      Pattern.compile(
          "^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){1}"
              + "(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){2}"
              + "([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");
  private static final Pattern IP_V6 = Pattern.compile("^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$");
  private static final Pattern IP_V6_COMPRESSED =
      Pattern.compile(
          "^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)"
              + "::(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)$");

  /**
   * Checks if the IP address is IPv4
   *
   * @param ip The IP address to check
   * @return True if the address is IPv4
   */
  public static boolean isIPv4(final String ip) {
    return IP_V4.matcher(ip).matches();
  }

  /**
   * Checks if the IP address is IPv6
   *
   * @param ip The IP address to check
   * @return True if the address is IPv6
   */
  public static boolean isIPv6(final String ip) {
    return IP_V6.matcher(ip).matches() || IP_V6_COMPRESSED.matcher(ip).matches();
  }
}
