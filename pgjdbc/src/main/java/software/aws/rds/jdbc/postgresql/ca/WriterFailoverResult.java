/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.core.BaseConnection;

import java.sql.Connection;
import java.util.List;

/** This class holds results of Writer Failover Process. */
public class WriterFailoverResult {
  private final boolean isConnected;
  private final boolean isNewHost;
  private final List<HostInfo> topology;
  private final @Nullable BaseConnection newConnection;

  /**
   * Constructor for WriterFailoverResult
   * @param isConnected True if writer failover was successful and is now connected to a host
   * @param isNewHost True if failover resulted in connecting to a new host
   * @param topology The Topolgoy containing information about the available hosts
   * @param newConnection The current {@link Connection} object. This is null if there isn't a current connection
   *     and failover failed.
   */
  public WriterFailoverResult(
      boolean isConnected,
      boolean isNewHost,
      List<HostInfo> topology,
      @Nullable BaseConnection newConnection) {
    this.isConnected = isConnected;
    this.isNewHost = isNewHost;
    this.topology = topology;
    this.newConnection = newConnection;
  }

  /**
   * Checks if process result is successful and new connection to host is established.
   *
   * @return True, if process successfully connected to a host.
   */
  public boolean isConnected() {
    return this.isConnected;
  }

  /**
   * Checks if process successfully connected to a new host.
   *
   * @return True, if process successfully connected to a new host. False, if process successfully
   *     re-connected to the same host.
   */
  public boolean isNewHost() {
    return this.isNewHost;
  }

  /**
   * Get latest topology.
   *
   * @return List of hosts that represent the latest topology. Returns null if no connection is
   *     established.
   */
  public List<HostInfo> getTopology() {
    return this.topology;
  }

  /**
   * Get new connection to a host.
   *
   * @return {@link Connection} New connection to a host. Returns null if no connection is
   *     established.
   */
  public @Nullable BaseConnection getNewConnection() {
    return this.newConnection;
  }
}
