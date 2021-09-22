/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3.plugins;

public class AuthenticationPluginManager {

  // Use native password plugin by default.
  private AuthenticationPlugin plugin = new NativePasswordPlugin();

  public void setPlugin(final AuthenticationPlugin plugin) {
    this.plugin = plugin;
  }

  public byte[] getPassword(final String user, final String password) {
    return this.plugin.getEncodedPassword(user, password);
  }
}
