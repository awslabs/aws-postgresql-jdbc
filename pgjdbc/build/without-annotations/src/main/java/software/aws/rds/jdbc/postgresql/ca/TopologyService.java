/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

// import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.util.List;
import java.util.Set;

/**
 * It's a generic interface for all topology services. It's expected that every implementation of
 * topology service covers different cluster types or cluster deployment.
 *
 * <p>It's expected that each instance of ClusterAwareConnectionProxy uses it's own instance of
 * topology service.
 */
public interface TopologyService {

  /**
   * Set unique cluster identifier for topology service instance.
   *
   * <p>Cluster could be accessed through different connection strings like IP address, cluster dns
   * endpoint, instance dns endpoint, custom domain alias (CNAME), etc. Cluster Id can be any string
   * that unique identify a cluster despite the way it's been accessed.
   *
   * @param clusterId Cluster unique identifier.
   */
  void setClusterId(String clusterId);

  /**
   * Sets host details common to each instance in the cluster, including the host dns pattern. "?"
   * (question mark) in a host dns pattern will be replaced with a host instance name to form a
   * fully qualified dns host endpoint.
   *
   * <p>Examples: "?.mydomain.com", "db-instance.?.mydomain.com"
   *
   * @param clusterInstanceTemplate Cluster instance details including host dns pattern.
   */
  void setClusterInstanceTemplate(HostInfo clusterInstanceTemplate);

  /**
   * Get cluster topology. A writer host is always at position 0.
   *
   * @param conn A connection to database to fetch the latest topology, if needed.
   * @param forceUpdate If true, it forces a service to ignore cached copy of topology and to fetch
   *     a fresh one.
   * @return A list of hosts that describes cluster topology. A writer is always at position 0.
   *     Returns null if topology isn't available.
   */
  /* @Nullable */ List</* @Nullable */ HostInfo> getTopology(Connection conn, boolean forceUpdate);

  /**
   * Get cached topology.
   *
   * @return List of hosts that represents topology. If there's no topology in cache, it returns
   *     null.
   */
  /* @Nullable */ List</* @Nullable */ HostInfo> getCachedTopology();

  /**
   * Get details about the most recent reader that the driver has successfully connected to.
   *
   * @return The host details of the most recent reader connection.
   */
  /* @Nullable */ HostInfo getLastUsedReaderHost();

  /**
   * Set details about the most recent reader that the driver has connected to.
   *
   * @param reader A reader host.
   */
  void setLastUsedReaderHost(/* @Nullable */ HostInfo reader);

  /**
   * Get a set of host names that were marked down.
   *
   * @return A set of host dns names with port. For example: "instance-1.my-domain.com:3306".
   */
  Set<String> getDownHosts();

  /**
   * Mark host as down. Host stays marked down until next topology refresh.
   *
   * @param downHost The {@link HostInfo} object representing the host to mark as down
   */
  void addToDownHostList(/* @Nullable */ HostInfo downHost);

  /**
   * Unmark host as down. The host is removed from the list of down hosts
   *
   * @param host The {@link HostInfo} object representing the host to remove from the list of down hosts
   */
  void removeFromDownHostList(HostInfo host);

  /**
   * Set new topology refresh rate. Topology is considered valid (up to date) within provided
   * duration of time.
   *
   * @param refreshRate Topology refresh rate in millis.
   */
  void setRefreshRate(int refreshRate);

  /** Clear topology service for all clusters. */
  void clearAll();

  /** Clear topology service for the current cluster. */
  void clear();
}
