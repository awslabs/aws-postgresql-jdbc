/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3.plugins;

import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;
import org.checkerframework.checker.initialization.qual.UnderInitialization;

import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AwsIamAuthenticationPlugin implements IAuthenticationPlugin {
  private static final Logger LOGGER = Logger.getLogger(AwsIamAuthenticationPlugin.class.getName());
  private static final int REGION_MATCHER_GROUP = 3;
  private String password = "";
  private final String region;
  private final String hostname;
  private final int port;

  public AwsIamAuthenticationPlugin(final String hostname, final int port) throws PSQLException {
    this.hostname = hostname;
    this.port = port;
    this.region = parseRdsRegion(this.hostname);
  }

  @Override
  public byte[] getEncodedPassword(final String user, final String password) {
    if (this.password.isEmpty()) {
      this.password = generateAuthenticationToken(user);
    }

    return this.password.getBytes(StandardCharsets.UTF_8);
  }

  private String generateAuthenticationToken(final String user) {
    final RdsIamAuthTokenGenerator generator = RdsIamAuthTokenGenerator
        .builder()
        .region(this.region)
        .credentials(new DefaultAWSCredentialsProviderChain())
        .build();

    return generator.getAuthToken(GetIamAuthTokenRequest
        .builder()
        .hostname(this.hostname)
        .port(this.port)
        .userName(user)
        .build());
  }

  private String parseRdsRegion(
      @UnderInitialization AwsIamAuthenticationPlugin this,
      final String hostname) throws PSQLException {
    final Pattern auroraDnsPattern =
        Pattern.compile(
            "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-)?[a-zA-Z0-9]+\\.([a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com",
            Pattern.CASE_INSENSITIVE);
    final Matcher matcher = auroraDnsPattern.matcher(hostname);
    matcher.find();
    final String region = matcher.group(REGION_MATCHER_GROUP);
    if (region == null) {
      LOGGER.log(Level.FINEST, "Failed to parse the AWS region from the given hostname. This error should not happen.");
      throw new PSQLException(
          GT.tr("Provided hostname does not contain an AWS region."),
          null);
    }
    return region;
  }
}
