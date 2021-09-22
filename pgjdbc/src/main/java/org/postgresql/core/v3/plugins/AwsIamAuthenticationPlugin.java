/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
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
