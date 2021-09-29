/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3.plugins;

public interface IAuthenticationPlugin {
  byte[] getEncodedPassword(String user, String password);
}
