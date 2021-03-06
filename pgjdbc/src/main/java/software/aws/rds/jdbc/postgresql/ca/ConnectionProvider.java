/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.HostSpec;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/** Implement this interface in order to handle physical connection creation process. */
public interface ConnectionProvider {
  /**
   * Called once per connection that needs to be created.
   *
   * @param hostSpec The HostSpec containing the host-port information for the host to connect to
   * @param props The Properties to use for the connection
   * @param url The connection URL
   *
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  BaseConnection connect(HostSpec hostSpec, Properties props, @Nullable String url) throws SQLException;
}
