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

public class AwsIamAuthenticationPlugin implements AuthenticationPlugin {

  private String password = "";

  @Override
  public byte[] getEncodedPassword(String user, String password) {
    if (this.password.isEmpty()) {
      this.password = generateAuthenticationToken(user);
    }
    return this.password.getBytes(StandardCharsets.UTF_8);
  }

  private String generateAuthenticationToken(String user) {
    final RdsIamAuthTokenGenerator generator = RdsIamAuthTokenGenerator
        .builder()
        .region("us-east-2")
        .credentials(new DefaultAWSCredentialsProviderChain())
        .build();

    return generator.getAuthToken(GetIamAuthTokenRequest
        .builder()
        .hostname("database-aurora-postgres-instance-1.czygpppufgy4.us-east-2.rds.amazonaws.com")
        .port(5432)
        .userName(user)
        .build());
  }
}
