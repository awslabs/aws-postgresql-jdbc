/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3.plugins;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.rds.auth.GetIamAuthTokenRequest;
import com.amazonaws.services.rds.auth.RdsIamAuthTokenGenerator;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AwsIamAuthenticationPlugin implements IAuthenticationPlugin {

  private static final int REGION_MATCHER_GROUP = 3;
  private String password = "";
  private final String region;
  private final String hostname;
  private final int port;

  public AwsIamAuthenticationPlugin(final String hostname, final int port) {
    this.hostname = hostname;
    this.port = port;
    this.region = this.getRdsRegion();
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

  private String getRdsRegion() {
    final Pattern auroraDnsPattern =
        Pattern.compile(
            "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-)?[a-zA-Z0-9]+\\.([a-zA-Z0-9\\-]+)\\.rds\\.amazonaws\\.com",
            Pattern.CASE_INSENSITIVE);
    final Matcher matcher = auroraDnsPattern.matcher(this.hostname);
    matcher.find();
    return matcher.group(REGION_MATCHER_GROUP);
  }
}
