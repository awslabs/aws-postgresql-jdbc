/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.core.BaseConnection;

/** This class holds results of Reader Failover Process. */
public class ReaderFailoverResult {
  private final @Nullable BaseConnection newConnection;
  private final @Nullable HostInfo newHost;
  private final boolean isConnected;

  /**
   * ConnectionAttemptResult constructor.
   *
   * @param newConnection The new connection created by the reader failover process
   * @param newHost The {@link HostInfo} associated with the new connection
   * @param isConnected Indicates whether reader failover was successful
   */
  public ReaderFailoverResult(
      @Nullable BaseConnection newConnection, @Nullable HostInfo newHost, boolean isConnected) {
    this.newConnection = newConnection;
    this.newHost = newHost;
    this.isConnected = isConnected;
  }

  /**
   * Get new connection to a host.
   *
   * @return {@link BaseConnection} New connection to a host. Returns null if no connection is
   *     established.
   */
  public @Nullable BaseConnection getConnection() {
    return newConnection;
  }

  /**
   * Get the {@link HostInfo} associated with the new connection
   *
   * @return The {@link HostInfo} associated with the new connection, or null if no connection
   *     was established.
   */
  public @Nullable HostInfo getHost() {
    return newHost;
  }

  /**
   * Checks if process result is successful and new connection to host is established.
   *
   * @return True, if process successfully connected to a host.
   */
  public boolean isConnected() {
    return isConnected;
  }
}
