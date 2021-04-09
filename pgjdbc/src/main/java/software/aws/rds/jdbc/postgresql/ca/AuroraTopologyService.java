/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import software.aws.rds.jdbc.postgresql.ca.metrics.ClusterAwareMetrics;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.util.ExpiringCache;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of TopologyService created for Amazon Aurora
 */
public class AuroraTopologyService implements TopologyService {
  static final int DEFAULT_REFRESH_RATE_IN_MILLISECONDS = 30000;
  static final int DEFAULT_CACHE_EXPIRE_MS = 5 * 60 * 1000; // 5 min
  static final int WRITER_CONNECTION_INDEX = 0;

  private int refreshRateInMilliseconds;
  static final String RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID, SESSION_ID FROM aurora_replica_status() "
          // filter out nodes that haven't been updated in the last 5 minutes
          + "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
          + "ORDER BY LAST_UPDATE_TIMESTAMP DESC";
  static final String WRITER_SESSION_ID = "MASTER_SESSION_ID";

  static final String SERVER_ID_COL = "SERVER_ID";
  static final String SESSION_ID_COL = "SESSION_ID";

  protected static final ExpiringCache<String, ClusterTopologyInfo> topologyCache =
      new ExpiringCache<>(DEFAULT_CACHE_EXPIRE_MS);
  private static final Object cacheLock = new Object();
  private static final Logger LOGGER = Logger.getLogger(AuroraTopologyService.class.getName());

  protected String clusterId;
  protected HostInfo clusterInstanceTemplate;

  protected @MonotonicNonNull ClusterAwareMetrics metrics = null;
  protected boolean gatherPerfMetrics;

  /** Initializes a service with topology default refresh rate. */
  public AuroraTopologyService() {
    this(DEFAULT_REFRESH_RATE_IN_MILLISECONDS);
  }

  /**
   * Initializes a service with provided topology refresh rate.
   *
   * @param refreshRateInMilliseconds Topology refresh rate in millis
   */
  public AuroraTopologyService(int refreshRateInMilliseconds) {
    this.refreshRateInMilliseconds = refreshRateInMilliseconds;
    this.clusterId = UUID.randomUUID().toString();
    this.clusterInstanceTemplate = new HostInfo("?", null, HostInfo.NO_PORT, false);
  }

  /**
   * Initializes the performance metrics
   *
   * @param metrics The ClusterAwareMetrics instance that will be used to record metrics
   * @param gatherMetrics The metric settings that decides whether or not the class should gather metrics
   */
  public void setPerformanceMetrics(ClusterAwareMetrics metrics, boolean gatherMetrics) {
    this.metrics = metrics;
    this.gatherPerfMetrics = gatherMetrics;
  }

  /**
   * Service hosts with the same cluster Id share cluster topology. Shared topology is cached
   * for a specified period of time. This method sets cache expiration time in millis.
   *
   * @param expireTimeMs Topology cache expiration time in millis
   */
  public static void setExpireTime(int expireTimeMs) {
    topologyCache.setExpireTime(expireTimeMs);
  }

  /**
   * Sets cluster Id for a  host Different service hosts with the same cluster Id
   * share topology cache.
   *
   * @param clusterId Topology cluster Id
   */
  @Override
  public void setClusterId(String clusterId) {
    LOGGER.log(Level.FINER, "[AuroraTopologyService] clusterId=''{0}''", clusterId);
    this.clusterId = clusterId;
  }

  /**
   * Sets host details common to each host in the cluster, including the host dns pattern. "?"
   * (question mark) in a host dns pattern will be replaced with a host name to form a
   * fully qualified dns host endpoint.
   *
   * <p>Examples: "?.mydomain.com", "db-host.?.mydomain.com"
   *
   * @param clusterInstanceTemplate Cluster host details including host dns pattern.
   */
  @Override
  public void setClusterInstanceTemplate(HostInfo clusterInstanceTemplate) {
    LOGGER.log(Level.FINER, "[AuroraTopologyService] clusterInstance host=''{0}'', port={1,number,#}",
        new Object[] {clusterInstanceTemplate.getHost(), clusterInstanceTemplate.getPort()});
    this.clusterInstanceTemplate = clusterInstanceTemplate;
  }

  /**
   * Get cluster topology. It may require an extra call to database to fetch the latest topology. A
   * cached copy of topology is returned if it's not yet outdated (controlled by {@link
   * #refreshRateInMilliseconds }).
   *
   * @param conn A connection to database to fetch the latest topology, if needed.
   * @param forceUpdate If true, it forces a service to ignore cached copy of topology and to fetch
   *     a fresh one.
   * @return A list of hosts that describes cluster topology. A writer is always at position 0.
   *     Returns an empty list if topology isn't available or is invalid (doesn't contain a writer).
   */
  @Override
  public List<HostInfo> getTopology(Connection conn, boolean forceUpdate) {
    ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);

    if (clusterTopologyInfo == null
        || clusterTopologyInfo.hosts.isEmpty()
        || forceUpdate
        || refreshNeeded(clusterTopologyInfo)) {

      ClusterTopologyInfo latestTopologyInfo = queryForTopology(conn);

      if (!latestTopologyInfo.hosts.isEmpty()) {
        clusterTopologyInfo = updateCache(clusterTopologyInfo, latestTopologyInfo);
      } else {
        return (clusterTopologyInfo == null || forceUpdate) ? new ArrayList<>() : clusterTopologyInfo.hosts;
      }
    }

    return clusterTopologyInfo.hosts;
  }

  /**
   * Checks whether or not it is necessary to refresh and update the topology.
   *
   * @param info ClusterTopologyInfo instance that contains information about the topology including
   *     the last updated time
   * @return True if the topology is empty (may still need to be initialized) or if the time since the
   *     last query has exceeded
   */
  private boolean refreshNeeded(ClusterTopologyInfo info) {
    Instant lastUpdateTime = info.lastUpdated;
    return info.hosts.isEmpty() || Duration.between(lastUpdateTime, Instant.now()).toMillis() > refreshRateInMilliseconds;
  }

  /**
   * Obtain a cluster topology from database.
   *
   * @param conn A connection to database to fetch the latest topology.
   * @return A {@link ClusterTopologyInfo} instance which contains details of the fetched topology
   */
  protected ClusterTopologyInfo queryForTopology(Connection conn) {
    long startTimeMs = this.gatherPerfMetrics ? System.currentTimeMillis() : 0;

    List<HostInfo> hosts = new ArrayList<>();
    try (Statement stmt = conn.createStatement()) {
      try (ResultSet resultSet = stmt.executeQuery(RETRIEVE_TOPOLOGY_SQL)) {
        hosts = processQueryResults(resultSet);
      }
    } catch (SQLException e) {
      // ignore
    }

    if (this.metrics != null && this.gatherPerfMetrics) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerTopologyQueryTime(currentTimeMs - startTimeMs);
    }
    return new ClusterTopologyInfo(hosts, new HashSet<>(), null, Instant.now());
  }

  /**
   * Form a list of hosts from the results of the topology query
   *
   * @param resultSet The results of the topology query
   * @return A list of {@link HostInfo} objects representing the topology that was returned by the topology query. The
   *         list will be empty if the topology query returned an invalid topology (no writer instance).
   */
  private List<HostInfo> processQueryResults(ResultSet resultSet) throws SQLException {
    int writerCount = 0;
    List<HostInfo> hosts = new ArrayList<>();

    while (resultSet.next()) {
      if (!WRITER_SESSION_ID.equalsIgnoreCase(resultSet.getString(SESSION_ID_COL))) {
        hosts.add(createHost(resultSet));
        continue;
      }

      if (writerCount == 0) {
        // store the first writer to its expected position [0]
        hosts.add(WRITER_CONNECTION_INDEX, createHost(resultSet));
      } else {
        // during failover, there could temporarily be two writers. Because we sorted by the last
        // updated timestamp, this host should be the obsolete writer, and it is about to become a reader
        hosts.add(createHost(resultSet, false));
      }
      writerCount++;
    }

    if (writerCount == 0) {
      LOGGER.log(Level.SEVERE, "[AuroraTopologyService] The topology query returned an invalid topology - no writer instance detected");
      hosts.clear();
    }
    return hosts;
  }

  /**
   * Creates an instance of HostInfo which captures details about a connectable host
   *
   * @param resultSet the result set from querying the topology
   * @return A {@link HostInfo} instance for a specific instance from the cluster
   * @throws SQLException If unable to retrieve the hostName from the result set
   */
  private HostInfo createHost(ResultSet resultSet) throws SQLException {
    return createHost(resultSet, WRITER_SESSION_ID.equals(resultSet.getString(SESSION_ID_COL)));
  }

  /**
   * Creates an instance of HostInfo which captures details about a connectable host
   *
   * @param resultSet the result set from querying the topology
   * @param isWriter true if the session_ID is the writer, else it will return false
   * @return A {@link HostInfo} instance for a specific instance from the cluster
   * @throws SQLException If unable to retrieve the hostName from the result set
   */
  private HostInfo createHost(ResultSet resultSet, boolean isWriter) throws SQLException {
    String hostName = resultSet.getString(SERVER_ID_COL);
    hostName = hostName == null ? "NULL" : hostName;
    return new HostInfo(
        getHostEndpoint(hostName),
        hostName,
        this.clusterInstanceTemplate.getPort(),
        isWriter);
  }

  /**
   * Build an host dns endpoint based on host/node name.
   *
   * @param nodeName An host name.
   * @return Host dns endpoint
   */
  private String getHostEndpoint(String nodeName) {
    String host = this.clusterInstanceTemplate.getHost();
    return host.replace("?", nodeName);

  }

  /**
   * Store the information for the topology in the cache, creating the information object if it did not previously exist
   * in the cache
   *
   * @param clusterTopologyInfo The cluster topology info that existed in the cache before the topology query. This parameter
   *                            will be null if no topology info for the cluster has been created in the cache yet.
   * @param latestTopologyInfo The results of the current topology query
   * @return The {@link ClusterTopologyInfo} stored in the cache by this method, representing the most up-to-date
   *         information we have about the topology.
   */
  private ClusterTopologyInfo updateCache(@Nullable ClusterTopologyInfo clusterTopologyInfo, ClusterTopologyInfo latestTopologyInfo) {
    if (clusterTopologyInfo == null) {
      clusterTopologyInfo = latestTopologyInfo;
    } else {
      clusterTopologyInfo.hosts = latestTopologyInfo.hosts;
      clusterTopologyInfo.downHosts = latestTopologyInfo.downHosts;
    }
    clusterTopologyInfo.lastUpdated = Instant.now();

    synchronized (cacheLock) {
      topologyCache.put(this.clusterId, clusterTopologyInfo);
    }
    return clusterTopologyInfo;
  }

  /**
   * Get cached topology.
   *
   * @return List of hosts that represents topology. If there's no topology in the cache or the
   *     cached topology is outdated, it returns null.
   */
  @Override
  public @Nullable List<HostInfo> getCachedTopology() {
    ClusterTopologyInfo info = topologyCache.get(this.clusterId);
    return info == null || refreshNeeded(info) ? null : info.hosts;
  }

  /**
   * Get details about the most recent reader that the driver has successfully connected to.
   *
   * @return The host details of the most recent reader connection. Returns null if the driver has
   *     not connected to a reader within the refresh rate period.
   */
  @Override
  public @Nullable HostInfo getLastUsedReaderHost() {
    ClusterTopologyInfo info = topologyCache.get(this.clusterId);
    return info == null || refreshNeeded(info) ? null : info.lastUsedReader;
  }

  /**
   * Set details about the most recent reader that the driver has connected to.
   *
   * @param reader A reader host.
   */
  @Override
  public void setLastUsedReaderHost(@Nullable HostInfo reader) {
    if (reader != null) {
      synchronized (cacheLock) {
        ClusterTopologyInfo info = topologyCache.get(this.clusterId);
        if (info != null) {
          info.lastUsedReader = reader;
        }
      }
    }
  }

  /**
   * Get a set of host names that were marked down.
   *
   * @return A set of host dns names with port (example: "host-1.my-domain.com:3306")
   */
  @Override
  public Set<String> getDownHosts() {
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      return clusterTopologyInfo != null ? clusterTopologyInfo.downHosts : new HashSet<>();
    }
  }

  /**
   * Mark host as down. Host stays marked down until next topology refresh.
   *
   * @param downHost The {@link HostInfo} object representing the host to mark as down
   */
  @Override
  public void addToDownHostList(@Nullable HostInfo downHost) {
    if (downHost == null) {
      return;
    }

    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      if (clusterTopologyInfo == null) {
        clusterTopologyInfo = new ClusterTopologyInfo(new ArrayList<>(), new HashSet<>(), null, Instant.now());
        topologyCache.put(this.clusterId, clusterTopologyInfo);
      }
      clusterTopologyInfo.downHosts.add(downHost.getHostPortPair());
    }
  }

  /**
   * Unmark host as down. The host is removed from the list of down hosts
   *
   * @param host The {@link HostInfo} object representing the host to remove from the list of down hosts
   */
  @Override
  public void removeFromDownHostList(@Nullable HostInfo host) {
    if (host == null) {
      return;
    }
    synchronized (cacheLock) {
      ClusterTopologyInfo clusterTopologyInfo = topologyCache.get(this.clusterId);
      if (clusterTopologyInfo != null) {
        clusterTopologyInfo.downHosts.remove(host.getHostPortPair());
      }
    }
  }

  /**
   * Set new topology refresh rate. Different service hosts may have different topology refresh
   * rate while sharing the same topology cache.
   *
   * @param refreshRate Topology refresh rate in millis.
   */
  @Override
  public void setRefreshRate(int refreshRate) {
    this.refreshRateInMilliseconds = refreshRate;
    if (topologyCache.getExpireTime() < this.refreshRateInMilliseconds) {
      synchronized (cacheLock) {
        if (topologyCache.getExpireTime() < this.refreshRateInMilliseconds) {
          topologyCache.setExpireTime(this.refreshRateInMilliseconds);
        }
      }
    }
  }

  /** Clear topology cache for all clusters. */
  @Override
  public void clearAll() {
    synchronized (cacheLock) {
      topologyCache.clear();
    }
  }

  /** Clear topology cache for the current cluster. */
  @Override
  public void clear() {
    synchronized (cacheLock) {
      topologyCache.remove(this.clusterId);
    }
  }

  /** Class that holds the topology and additional information about the topology. */
  private static class ClusterTopologyInfo {
    public List<HostInfo> hosts;
    public Set<String> downHosts;
    public @Nullable HostInfo lastUsedReader;
    public Instant lastUpdated;

    /**
     * Constructor for ClusterTopologyInfo
     *
     * @param hosts List of available instance hosts
     * @param downHosts List of hosts that area marked as down
     * @param lastUsedReader The previously used reader host
     * @param lastUpdated Last updated topology time
     */
    ClusterTopologyInfo(List<HostInfo> hosts, Set<String> downHosts,
        @Nullable HostInfo lastUsedReader, Instant lastUpdated) {
      this.hosts = hosts;
      this.downHosts = downHosts;
      this.lastUsedReader = lastUsedReader;
      this.lastUpdated = lastUpdated;
    }
  }
}
