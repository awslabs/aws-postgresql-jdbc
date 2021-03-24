package org.postgresql.util;

// import org.checkerframework.checker.nullness.qual.Nullable;
import software.aws.rds.jdbc.postgresql.ca.HostInfo;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConnectionUrlParser {
  private static final Pattern GENERIC_HOST_PTRN = Pattern.compile("^(?<host>.*?)(?::(?<port>[^:]*))?$");

  /**
   * Parses a host:port pair and returns the two elements in a {@link HostSpec}
   *
   * @param hostInfo
   *            the host:pair to parse
   * @return a {@link HostSpec} containing the host and port information or null if the host information can't be parsed
   */
  public static /* @Nullable */ HostSpec parseHostPortPair(/* @Nullable */ String hostInfo) {
    if (StringUtils.isNullOrEmpty(hostInfo)) {
      return null;
    }
    Matcher matcher = GENERIC_HOST_PTRN.matcher(hostInfo);
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
      return new HostSpec(host, portAsInteger);
    }
    return null;
  }

  /**
   * URL-decode the given string.
   *
   * @param text
   *            the string to decode
   * @return
   *         the decoded string
   */
  private static String decode(String text) {
    if (StringUtils.isNullOrEmpty(text)) {
      return text;
    }
    try {
      return URLDecoder.decode(text, StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      // Won't happen.
    }
    return "";
  }
}
