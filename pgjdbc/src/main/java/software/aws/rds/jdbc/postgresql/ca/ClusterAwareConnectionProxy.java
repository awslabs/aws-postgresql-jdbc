/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import software.aws.rds.jdbc.postgresql.ca.metrics.ClusterAwareMetrics;

import org.checkerframework.checker.initialization.qual.UnderInitialization;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.RequiresNonNull;
import org.postgresql.PGProperty;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.TransactionState;
import org.postgresql.util.ConnectionUrlParser;
import org.postgresql.util.HostSpec;
import org.postgresql.util.IpAddressUtils;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.StringUtils;
import org.postgresql.util.Util;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A proxy for a dynamic org.postgresql.core.Connection implementation that provides cluster-aware
 * failover features. Connection switching occurs on communication-related exceptions and/or
 * cluster topology changes.
 */
public class ClusterAwareConnectionProxy implements InvocationHandler {
  static final String METHOD_ABORT = "abort";
  static final String METHOD_CLOSE = "close";
  static final String METHOD_COMMIT = "commit";
  static final String METHOD_EQUALS = "equals";
  static final String METHOD_GET_AUTO_COMMIT = "getAutoCommit";
  static final String METHOD_GET_CATALOG = "getCatalog";
  static final String METHOD_GET_SCHEMA = "getSchema";
  static final String METHOD_GET_TRANSACTION_ISOLATION = "getTransactionIsolation";
  static final String METHOD_HASH_CODE = "hashCode";
  static final String METHOD_IS_CLOSED = "isClosed";
  static final String METHOD_ROLLBACK = "rollback";
  static final String METHOD_SET_AUTO_COMMIT = "setAutoCommit";
  static final String METHOD_SET_READ_ONLY = "setReadOnly";
  protected static final int DEFAULT_SOCKET_TIMEOUT = 10;
  protected static final int DEFAULT_CONNECT_TIMEOUT = 30;

  protected static final int WRITER_CONNECTION_INDEX = 0; // writer host is always stored at index 0
  private static final transient Logger LOGGER = Logger.getLogger(ClusterAwareConnectionProxy.class.getName());

  protected final String originalUrl;
  protected boolean explicitlyReadOnly = false;
  protected boolean inTransaction = false;
  protected boolean isClusterTopologyAvailable = false;
  protected boolean isRdsProxy = false;
  protected boolean isRds = false;
  protected TopologyService topologyService;
  protected List<HostInfo> hosts = new ArrayList<>();
  protected Properties initialConnectionProps;
  protected WriterFailoverHandler writerFailoverHandler;
  protected ReaderFailoverHandler readerFailoverHandler;
  protected RdsDnsAnalyzer rdsDnsAnalyzer;
  protected ConnectionProvider connectionProvider;
  protected @Nullable BaseConnection currentConnection;
  protected @Nullable HostInfo currentHost;
  protected boolean isClosed = false;
  protected boolean closedExplicitly = false;
  protected @Nullable String closedReason = null;

  protected ClusterAwareMetrics metrics = new ClusterAwareMetrics();
  private long invokeStartTimeMs;
  private long failoverStartTimeMs;

  // Configuration settings
  protected boolean enableFailoverSetting = true;
  protected int clusterTopologyRefreshRateMsSetting;
  protected @Nullable String clusterIdSetting;
  protected @Nullable String clusterInstanceHostPatternSetting;
  protected boolean gatherPerfMetricsSetting;
  protected int failoverTimeoutMsSetting;
  protected int failoverClusterTopologyRefreshRateMsSetting;
  protected int failoverWriterReconnectIntervalMsSetting;
  protected int failoverReaderConnectTimeoutMsSetting;
  protected int failoverConnectTimeout; //sec
  protected int failoverSocketTimeout; //sec

  protected @Nullable Throwable lastExceptionDealtWith = null;

  /**
   * Instantiates a new AuroraConnectionProxy for the given host and properties.
   *
   * @param hostSpec The HostSpec info for the host to connect to
   * @param props The Properties specifying the connection and failover configuration
   * @param url The URL to connect to
   *
   * @throws SQLException if an error occurs
   */
  public ClusterAwareConnectionProxy(HostSpec hostSpec, Properties props, String url) throws SQLException {
    this.originalUrl = url;

    initSettings(props);
    initProxyFields(props);
    initProxy(hostSpec, props, url);
  }

  ClusterAwareConnectionProxy(HostSpec hostSpec, Properties props, String url, ConnectionProvider connectionProvider,
                              TopologyService service, WriterFailoverHandler writerFailoverHandler,
                              ReaderFailoverHandler readerFailoverHandler, RdsDnsAnalyzer rdsDnsAnalyzer) throws SQLException {
    this.originalUrl = url;

    initSettings(props);
    initProxyFields(props, connectionProvider, service, writerFailoverHandler, readerFailoverHandler, rdsDnsAnalyzer);
    initProxy(hostSpec, props, url);
  }

  /**
   * Configure the failover settings as well as the connect and socket timeout values
   *
   * @param props The {@link Properties} containing the settings for failover and socket/connect timeouts
   *
   * @throws PSQLException if an integer property's string representation can't be converted to an int. This should only
   *     happen if the property's string is not an integer representation
   */
  private synchronized void initSettings(@UnderInitialization ClusterAwareConnectionProxy this, Properties props)
          throws PSQLException {

    this.clusterIdSetting = PGProperty.CLUSTER_ID.get(props);
    this.clusterInstanceHostPatternSetting = PGProperty.CLUSTER_INSTANCE_HOST_PATTERN.get(props);
    this.clusterTopologyRefreshRateMsSetting = PGProperty.CLUSTER_TOPOLOGY_REFRESH_RATE_MS.getInt(props);

    this.failoverClusterTopologyRefreshRateMsSetting =
            PGProperty.FAILOVER_CLUSTER_TOPOLOGY_REFRESH_RATE_MS.getInt(props);
    this.failoverReaderConnectTimeoutMsSetting =
            PGProperty.FAILOVER_READER_CONNECT_TIMEOUT_MS.getInt(props);
    this.failoverTimeoutMsSetting =
            PGProperty.FAILOVER_TIMEOUT_MS.getInt(props);
    this.failoverWriterReconnectIntervalMsSetting =
            PGProperty.FAILOVER_WRITER_RECONNECT_INTERVAL_MS.getInt(props);

    if (props.getProperty(PGProperty.CONNECT_TIMEOUT.getName(), null) != null) {
      this.failoverConnectTimeout = PGProperty.CONNECT_TIMEOUT.getInt(props);
    } else {
      this.failoverConnectTimeout = DEFAULT_CONNECT_TIMEOUT;
    }

    if (props.getProperty(PGProperty.SOCKET_TIMEOUT.getName(), null) != null) {
      this.failoverSocketTimeout = PGProperty.SOCKET_TIMEOUT.getInt(props);
    } else {
      this.failoverSocketTimeout = DEFAULT_SOCKET_TIMEOUT;
    }

    this.enableFailoverSetting = PGProperty.ENABLE_CLUSTER_AWARE_FAILOVER.getBoolean(props);
    this.gatherPerfMetricsSetting = PGProperty.GATHER_PERF_METRICS.getBoolean(props);
  }

  /**
   * Initialize the internal fields of this proxy instance
   *
   * @param props The Properties specifying the connection and failover configuration
   */
  @EnsuresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler", "this.initialConnectionProps", "this.rdsDnsAnalyzer"})
  @RequiresNonNull({"this.metrics"})
  private synchronized void initProxyFields(@UnderInitialization ClusterAwareConnectionProxy this, Properties props) {

    this.initialConnectionProps = (Properties)props.clone();
    PGProperty.CONNECT_TIMEOUT.set(this.initialConnectionProps, this.failoverConnectTimeout);
    PGProperty.SOCKET_TIMEOUT.set(this.initialConnectionProps, this.failoverSocketTimeout);

    AuroraTopologyService topologyService = new AuroraTopologyService();
    topologyService.setPerformanceMetrics(metrics, this.gatherPerfMetricsSetting);
    topologyService.setRefreshRate(this.clusterTopologyRefreshRateMsSetting);
    this.topologyService = topologyService;

    this.connectionProvider = new BasicConnectionProvider();
    this.readerFailoverHandler = new ClusterAwareReaderFailoverHandler(
            this.topologyService,
            this.connectionProvider,
            this.initialConnectionProps,
            this.failoverTimeoutMsSetting,
            this.failoverReaderConnectTimeoutMsSetting);
    this.writerFailoverHandler = new ClusterAwareWriterFailoverHandler(
            this.topologyService,
            this.connectionProvider,
            this.initialConnectionProps,
            this.readerFailoverHandler,
            this.failoverTimeoutMsSetting,
            this.failoverClusterTopologyRefreshRateMsSetting,
            this.failoverWriterReconnectIntervalMsSetting);
    this.rdsDnsAnalyzer = new RdsDnsAnalyzer();
  }

  /**
   * Initialize the internal fields of this proxy instance
   *
   * @param props The Properties specifying the connection and failover configuration
   * @param connectionProvider The {@link ConnectionProvider} to use to establish connections
   * @param service The {@link TopologyService} to use to fetch the cluster topology
   * @param writerFailoverHandler The {@link WriterFailoverHandler} to use to handle failovers to the writer instance
   * @param readerFailoverHandler The {@link ReaderFailoverHandler} to use to handle failovers that can be made to any
   *                              other instance, whether reader or writer
   */
  @EnsuresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler", "this.initialConnectionProps", "this.rdsDnsAnalyzer"})
  private synchronized void initProxyFields(@UnderInitialization ClusterAwareConnectionProxy this, Properties props,
                               ConnectionProvider connectionProvider, TopologyService service,
                               WriterFailoverHandler writerFailoverHandler, ReaderFailoverHandler readerFailoverHandler,
                               RdsDnsAnalyzer rdsDnsAnalyzer) {

    this.initialConnectionProps = (Properties)props.clone();
    PGProperty.CONNECT_TIMEOUT.set(this.initialConnectionProps, this.failoverConnectTimeout);
    PGProperty.SOCKET_TIMEOUT.set(this.initialConnectionProps, this.failoverSocketTimeout);

    this.topologyService = service;
    this.topologyService.setRefreshRate(this.clusterTopologyRefreshRateMsSetting);

    this.writerFailoverHandler = writerFailoverHandler;
    this.readerFailoverHandler = readerFailoverHandler;
    this.rdsDnsAnalyzer = rdsDnsAnalyzer;

    this.connectionProvider = connectionProvider;
  }

  /**
   * Initializes the connection proxy
   *
   *
   * @param hostSpec The HostSpec info for the host to connect to
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler", "this.rdsDnsAnalyzer", "this.initialConnectionProps", "this.metrics", "this.hosts"})
  private synchronized void initProxy(@UnderInitialization ClusterAwareConnectionProxy this, HostSpec hostSpec, Properties props,
                         String url) throws SQLException {
    if (!this.enableFailoverSetting) {
      // Use a standard default connection - no further initialization required
      this.currentConnection = this.connectionProvider.connect(hostSpec, props, url);
      return;
    }
    LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Cluster-aware failover is enabled.");
    LOGGER.log(Level.FINER,
            "[ClusterAwareConnectionProxy] 'clusterId' configuration setting: {0}", this.clusterIdSetting);
    LOGGER.log(Level.FINER,
            "[ClusterAwareConnectionProxy] 'clusterInstanceHostPatternSetting' configuration setting: {0}",
            this.clusterInstanceHostPatternSetting);

    if (!StringUtils.isNullOrEmpty(this.clusterInstanceHostPatternSetting)) {
      initFromHostPatternSetting(hostSpec, props, url);
    } else if (IpAddressUtils.isIPv4(hostSpec.getHost())
            || IpAddressUtils.isIPv6(hostSpec.getHost())) {
      initExpectingNoTopology(hostSpec, props, url);
    } else {
      identifyRdsType(hostSpec.getHost());
      if (!this.isRds) {
        initExpectingNoTopology(hostSpec, props, url);
      } else {
        initFromConnectionString(hostSpec, props, url);
      }
    }
  }

  /**
   * Initializes connection from host pattern
   *
   * @param connectionStringHostSpec The HostSpec info for the host to connect to
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler", "this.rdsDnsAnalyzer", "this.initialConnectionProps", "this.metrics", "this.hosts"})
  private void initFromHostPatternSetting(@UnderInitialization ClusterAwareConnectionProxy this,
                                          HostSpec connectionStringHostSpec,
                                          Properties props,
                                          String url) throws SQLException {
    HostSpec patternSettingHostSpec = getHostSpecFromHostPatternSetting();
    String instanceHostPattern = patternSettingHostSpec.getHost();
    int instanceHostPort = patternSettingHostSpec.getPort() != HostInfo.NO_PORT
            ? patternSettingHostSpec.getPort() : connectionStringHostSpec.getPort();

    setClusterId(instanceHostPattern, instanceHostPort);
    this.topologyService.setClusterInstanceTemplate(
            createClusterInstanceTemplate(instanceHostPattern, instanceHostPort));
    createConnectionAndInitializeTopology(connectionStringHostSpec, props, url);
  }

  /**
   * Retrieves the host and port number from host pattern
   *
   * @return A {@link HostSpec} object containing the host and port number
   * @throws SQLException if an error occurs
   */
  @RequiresNonNull("this.rdsDnsAnalyzer")
  private HostSpec getHostSpecFromHostPatternSetting(@UnderInitialization ClusterAwareConnectionProxy this) throws SQLException {
    HostSpec hostSpec = ConnectionUrlParser.parseHostPortPair(this.clusterInstanceHostPatternSetting);
    if (hostSpec == null) {
      throw new SQLException("Invalid value in 'clusterInstanceHostPattern' configuration property.");
    }
    validateHostPatternSetting(hostSpec);
    return hostSpec;
  }

  /**
   * Validate that the host pattern setting is usable and in the correct format
   *
   * @param hostSpec The {@link HostSpec} containing the info defined by the host pattern setting
   * @throws SQLException if an invalid value was used for the host pattern setting
   */
  @RequiresNonNull("this.rdsDnsAnalyzer")
  private void validateHostPatternSetting(@UnderInitialization ClusterAwareConnectionProxy this, HostSpec hostSpec) throws SQLException {
    String hostPattern = hostSpec.getHost();

    if (!isDnsPatternValid(hostPattern)) {
      String invalidDnsPatternError = "Invalid value set for the 'clusterInstanceHostPattern' configuration property.";
      LOGGER.log(Level.SEVERE, invalidDnsPatternError);
      throw new SQLException(invalidDnsPatternError);
    }

    identifyRdsType(hostPattern);
    if (this.isRdsProxy) {
      String rdsProxyInstancePatternError =
              "RDS Proxy url can't be used for the 'clusterInstanceHostPattern' configuration property";
      LOGGER.log(Level.SEVERE, rdsProxyInstancePatternError);
      throw new SQLException(rdsProxyInstancePatternError);
    }

    if (this.rdsDnsAnalyzer.isRdsCustomClusterDns(hostPattern)) {
      String rdsCustomClusterInstancePatternError =
              "RDS Custom Cluster endpoint can't be used for the 'clusterInstanceHostPattern' configuration property";
      LOGGER.log(Level.SEVERE, rdsCustomClusterInstancePatternError);
      throw new SQLException(rdsCustomClusterInstancePatternError);
    }
  }

  /**
   * Checks if the DNS pattern is valid
   *
   * @param pattern The string to check
   * @return True if the pattern is valid
   */
  private boolean isDnsPatternValid(@UnderInitialization ClusterAwareConnectionProxy this, String pattern) {
    return pattern.contains("?");
  }

  /**
   * Set isRds and isRdsProxy according to the host string
   *
   * @param host The endpoint for this connection
   */
  @RequiresNonNull("this.rdsDnsAnalyzer")
  private void identifyRdsType(@UnderInitialization ClusterAwareConnectionProxy this, String host) {
    this.isRds = this.rdsDnsAnalyzer.isRdsDns(host);
    LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] isRds={0}", this.isRds);

    this.isRdsProxy = this.rdsDnsAnalyzer.isRdsProxyDns(host);
    LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] isRdsProxy={0}", this.isRdsProxy);
  }

  /**
   * Initializes connection without expecting a current topology
   *
   * @param hostSpec The HostSpec info for the host to connect to
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler", "this.rdsDnsAnalyzer", "this.initialConnectionProps", "this.metrics", "this.hosts"})
  private void initExpectingNoTopology(@UnderInitialization ClusterAwareConnectionProxy this, HostSpec hostSpec,
                                       Properties props, String url) throws SQLException {
    setClusterId(hostSpec.getHost(), hostSpec.getPort());
    this.topologyService.setClusterInstanceTemplate(createClusterInstanceTemplate(hostSpec.getHost(), hostSpec.getPort()));
    createConnectionAndInitializeTopology(hostSpec, props, url);

    if (this.isClusterTopologyAvailable) {
      String instanceHostPatternRequiredError =
              "The 'clusterInstanceHostPattern' configuration property is required when an IP address or custom "
                      + "domain is used to connect to the cluster.";
      LOGGER.log(Level.SEVERE, instanceHostPatternRequiredError);
      throw new SQLException(instanceHostPatternRequiredError);
    }
  }

  /**
   * Initializes connection from string
   *
   * @param hostSpec The HostSpec info for the host to connect to
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException If an error occurs
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler", "this.rdsDnsAnalyzer", "this.initialConnectionProps", "this.metrics", "this.hosts"})
  private void initFromConnectionString(@UnderInitialization ClusterAwareConnectionProxy this, HostSpec hostSpec, Properties props, String url) throws SQLException {
    String rdsInstanceHostPattern = this.rdsDnsAnalyzer.getRdsInstanceHostPattern(hostSpec.getHost());
    if (rdsInstanceHostPattern == null) {
      String unexpectedConnectionStringPattern =
              "The provided connection string does not appear to match an expected Aurora DNS pattern. Please set the "
                      + "'clusterInstanceHostPattern' configuration property to specify the host pattern for the cluster "
                      + "you are trying to connect to.";
      LOGGER.log(Level.SEVERE, unexpectedConnectionStringPattern);
      throw new SQLException(unexpectedConnectionStringPattern);
    }

    setClusterId(hostSpec.getHost(), hostSpec.getPort());
    this.topologyService.setClusterInstanceTemplate(
            createClusterInstanceTemplate(rdsInstanceHostPattern, hostSpec.getPort()));
    createConnectionAndInitializeTopology(hostSpec, props, url);
  }

  /**
   * Sets the clusterID in the topology service
   *
   * @param host The host that will be used for the connection, or the instanceHostPattern setting
   * @param port The port that will be used for the connection
   */
  @RequiresNonNull({"this.topologyService", "this.rdsDnsAnalyzer"})
  private void setClusterId(@UnderInitialization ClusterAwareConnectionProxy this, String host, int port) {
    if (!StringUtils.isNullOrEmpty(this.clusterIdSetting)) {
      this.topologyService.setClusterId(this.clusterIdSetting);
    } else if (this.isRdsProxy) {
      // Each proxy is associated with a single cluster so it's safe to use the RDS Proxy Url as the cluster ID
      this.topologyService.setClusterId(host + ":" + port);
    } else if (this.isRds) {
      // If it's a cluster endpoint or reader cluster endpoint, then let's use it as the cluster ID
      String clusterRdsHostUrl = this.rdsDnsAnalyzer.getRdsClusterHostUrl(host);
      if (!StringUtils.isNullOrEmpty(clusterRdsHostUrl)) {
        this.topologyService.setClusterId(clusterRdsHostUrl + ":" + port);
      }
    }
  }

  /**
   * Creates an instance template for the topology service
   *
   * @param host The new host for the {@link HostInfo} object
   * @param port The port for the connection
   * @return A {@link HostInfo} class for cluster instance template
   */
  private HostInfo createClusterInstanceTemplate(@UnderInitialization ClusterAwareConnectionProxy this, String host, int port) {
    return new HostInfo(host, null, port,  false);
  }

  /**
   * Creates a connection and initializes the topology
   *
   * @param hostSpec The HostSpec info for the host to connect to
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler", "this.rdsDnsAnalyzer", "this.initialConnectionProps", "this.metrics", "this.hosts"})
  private synchronized void createConnectionAndInitializeTopology(@UnderInitialization ClusterAwareConnectionProxy this,
                                                                  HostSpec hostSpec,
                                                                  Properties props,
                                                                  String url) throws SQLException {
    boolean connectedUsingCachedTopology = createInitialConnection(hostSpec, props, url);
    initTopology(hostSpec, connectedUsingCachedTopology);
    finalizeConnection(connectedUsingCachedTopology);
  }

  /**
   * Establish the initial connection according to the given details. When the given host is the writer cluster or reader
   * cluster endpoint and cached topology information is available, the writer or reader instance to connect to will
   * be chosen based on this cached topology and verified later in {@link #validateInitialConnection(boolean)}
   *
   * @param hostSpec The {@link HostSpec} containing the information for the desired connection
   * @param props The {@link Properties} for the connection
   * @param url The URL to connect to in the case that the hostSpec host is not the writer cluster or reader cluster or
   *            cached topology information doesn't exist
   * @return true if the connection was established using cached topology; otherwise, false
   * @throws SQLException if a connection could not be established
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.rdsDnsAnalyzer", "this.initialConnectionProps"})
  private boolean createInitialConnection(@UnderInitialization ClusterAwareConnectionProxy this, HostSpec hostSpec,
                                          Properties props, String url) throws SQLException {
    String host = hostSpec.getHost();
    boolean connectedUsingCachedTopology = false;

    if (this.rdsDnsAnalyzer.isRdsClusterDns(host)) {
      this.explicitlyReadOnly = this.rdsDnsAnalyzer.isReaderClusterDns(host);
      LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] explicitlyReadOnly={0}", this.explicitlyReadOnly);

      try {
        attemptConnectionUsingCachedTopology();
        if (this.currentConnection != null && this.currentHost != null) {
          connectedUsingCachedTopology = true;
        }
      } catch (SQLException e) {
        // do nothing - attempt to connect directly will be made below
      }
    }

    if (!isConnected()) {
      // Either URL was not a cluster endpoint or cached topology did not exist - connect directly
      // to URL
      this.currentConnection = this.connectionProvider.connect(hostSpec, props, url);
    }
    return connectedUsingCachedTopology;
  }

  /**
   * Checks if there is a underlying connection for this proxy.
   *
   * @return true if there is a connection
   */
  synchronized boolean isConnected(@UnknownInitialization ClusterAwareConnectionProxy this) {
    try {
      return this.currentConnection != null && !this.currentConnection.isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * Attempts a connection using a cached topology
   *
   * @throws SQLException if an error occurs
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.topologyService", "this.connectionProvider"})
  private void attemptConnectionUsingCachedTopology(@UnderInitialization ClusterAwareConnectionProxy this) throws SQLException {
    @Nullable List<HostInfo> cachedHosts = topologyService.getCachedTopology();
    if (cachedHosts != null && !cachedHosts.isEmpty()) {
      this.hosts = cachedHosts;
      HostInfo candidateHost = getCandidateHostForInitialConnection();
      if (candidateHost != null) {
        connectTo(candidateHost);
      }
    }
  }

  /**
   * Retrieves a host for initial connection
   *
   * @return {@link HostInfo} for a candidate host
   */
  @RequiresNonNull({"this.hosts", "this.topologyService"})
  private @Nullable HostInfo getCandidateHostForInitialConnection(@UnderInitialization ClusterAwareConnectionProxy this) {
    if (this.explicitlyReadOnly) {
      HostInfo candidateReader = getCandidateReaderForInitialConnection();
      if (candidateReader != null) {
        return candidateReader;
      }
    }
    return this.hosts.get(WRITER_CONNECTION_INDEX);
  }

  /**
   * Retrieves a reader for initial connection
   *
   * @return {@link HostInfo} for a candidate reader
   */
  @RequiresNonNull({"this.topologyService", "this.hosts"})
  private @Nullable HostInfo getCandidateReaderForInitialConnection(@UnderInitialization ClusterAwareConnectionProxy this) {
    HostInfo lastUsedReader = topologyService.getLastUsedReaderHost();
    if (topologyContainsHost(lastUsedReader)) {
      return lastUsedReader;
    }

    return clusterContainsReader() ? getRandomReaderHost() : null;
  }

  /**
   * Checks if the topology in the proxy class contains a host
   *
   * @param host The host to check
   * @return True if the host is part of the topology
   */
  @RequiresNonNull({"this.hosts"})
  private boolean topologyContainsHost(@UnderInitialization ClusterAwareConnectionProxy this, @Nullable HostInfo host) {
    if (host == null) {
      return false;
    }
    for (HostInfo potentialMatch : this.hosts) {
      if (potentialMatch != null && potentialMatch.equalsHostPortPair(host)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if the cluster contains a reader
   *
   * @return True if there are readers in the cluster
   */
  @RequiresNonNull({"this.hosts"})
  private boolean clusterContainsReader(@UnknownInitialization ClusterAwareConnectionProxy this)  {
    return this.hosts.size() > 1;
  }

  /**
   * Retrieves a random host from topology
   *
   * @return A random {@link HostInfo} from the topology
   */
  @RequiresNonNull({"this.hosts"})
  private HostInfo getRandomReaderHost(@UnknownInitialization ClusterAwareConnectionProxy this) {
    int max = this.hosts.size() - 1;
    int min = WRITER_CONNECTION_INDEX + 1;
    int readerIndex = (int) (Math.random() * ((max - min) + 1)) + min;
    return this.hosts.get(readerIndex);
  }

  /**
   * Initializes topology
   *
   * @param hostSpec The current host to connect if the proxy does not hae a current host
   * @param connectedUsingCachedTopology True if connection is using a cached topology
   */
  @RequiresNonNull({"this.topologyService", "this.rdsDnsAnalyzer", "this.hosts"})
  private synchronized void initTopology(@UnderInitialization ClusterAwareConnectionProxy this, HostSpec hostSpec,
                                         boolean connectedUsingCachedTopology) {
    if (this.currentConnection != null) {
      List<HostInfo> topology = this.topologyService.getTopology(this.currentConnection, false);
      this.hosts = topology == null ? this.hosts : topology;
    }
    this.isClusterTopologyAvailable = !this.hosts.isEmpty();
    LOGGER.log(Level.FINER,
            "[ClusterAwareConnectionProxy] isClusterTopologyAvailable={0}", this.isClusterTopologyAvailable);

    // if we connected using the cached topology, there's a chance that the currentHost details are stale,
    // let's update it here. Otherwise, we should try to set the currentHost if it isn't set already.
    if (connectedUsingCachedTopology || (this.currentConnection != null && this.currentHost == null)) {
      updateInitialHost(this.currentHost == null ? hostSpec : this.currentHost.toHostSpec(), connectedUsingCachedTopology);
    }
  }

  /**
   * Updates the current host in the class. Will set the currentHost to null if the hostSpec is not
   * part of the cluster
   *
   * @param hostSpec The current host to set as current
   * @param connectedUsingCachedTopology True if connected using a cached topology
   */
  @RequiresNonNull({"this.hosts", "this.rdsDnsAnalyzer"})
  private void updateInitialHost(@UnderInitialization ClusterAwareConnectionProxy this, HostSpec hostSpec,
                                 boolean connectedUsingCachedTopology) {
    String connUrlHost = hostSpec.getHost();
    if (!connectedUsingCachedTopology && this.rdsDnsAnalyzer.isWriterClusterDns(connUrlHost) && !this.hosts.isEmpty()) {
      this.currentHost = this.hosts.get(WRITER_CONNECTION_INDEX);
    } else {
      for (HostInfo host : this.hosts) {
        if (hostSpec.toString().equals(host.getHostPortPair())) {
          this.currentHost = host;
          return;
        }
      }
      this.currentHost = null;
    }
  }

  /**
   * Finalizing the connection
   *
   * @param connectedUsingCachedTopology True if connection is using cached topology
   * @throws SQLException if an error occurs
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler", "this.initialConnectionProps", "this.metrics", "this.hosts"})
  private void finalizeConnection(@UnderInitialization ClusterAwareConnectionProxy this,
                                  boolean connectedUsingCachedTopology) throws SQLException {
    if (this.isFailoverEnabled()) {
      logTopology();
      if (this.gatherPerfMetricsSetting) {
        this.failoverStartTimeMs = System.currentTimeMillis();
      }
      validateInitialConnection(connectedUsingCachedTopology);
      if (this.currentHost != null && this.explicitlyReadOnly) {
        topologyService.setLastUsedReaderHost(this.currentHost);
      }
    }
  }

  /**
   * Checks if cluster-aware failover is enabled/possible.
   *
   * @return true if cluster-aware failover is enabled
   */
  public synchronized boolean isFailoverEnabled(@UnknownInitialization ClusterAwareConnectionProxy this) {
    return this.enableFailoverSetting
            && !this.isRdsProxy
            && this.isClusterTopologyAvailable;
  }

  /**
   * Validates initial connection
   *
   * @param connectedUsingCachedTopology True if using cached topology
   * @throws SQLException if an error occurs during failover
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.topologyService", "this.metrics", "this.readerFailoverHandler", "this.writerFailoverHandler", "this.connectionProvider", "this.hosts"})
  private synchronized void validateInitialConnection(@UnderInitialization ClusterAwareConnectionProxy this,
                                                      boolean connectedUsingCachedTopology) throws SQLException {
    if (!isConnected()) {
      pickNewConnection(true);
      return;
    }

    if (!invalidCachedWriterConnection(connectedUsingCachedTopology)) {
      return;
    }

    if (this.hosts.get(WRITER_CONNECTION_INDEX) == null) {
      failover();
      return;
    }

    try {
      connectTo(this.hosts.get(WRITER_CONNECTION_INDEX));
    } catch (SQLException e) {
      failover();
    }
  }

  /**
   * If we used cached topology information to make the initial connection, checks whether we
   * connected to a host that was marked as a writer in the cache but is in fact a reader according
   * to the fresh topology. This could happen if the cached topology was stale.
   *
   * @param connectedUsingCachedTopology Boolean indicating if we established the initial connection
   *          using cached topology information
   * @return True if we expected to be connected to a writer according to cached topology information,
   *         but that information was stale and we are actually incorrectly connected to a reader
   */
  private boolean invalidCachedWriterConnection(@UnderInitialization ClusterAwareConnectionProxy this,
                                                boolean connectedUsingCachedTopology) {
    if (this.explicitlyReadOnly || !connectedUsingCachedTopology) {
      return false;
    }

    return this.currentHost == null || !this.currentHost.isWriter();
  }

  /**
   * Picks a new connection. Conducts failover if needed.
   *
   * @param inInitialization True if driver is initializing
   * @throws SQLException if failover fails
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.topologyService", "this.metrics", "this.readerFailoverHandler", "this.writerFailoverHandler", "this.connectionProvider", "this.hosts"})
  protected synchronized void pickNewConnection(@UnknownInitialization ClusterAwareConnectionProxy this,
                                                boolean inInitialization) throws SQLException {
    if (this.isClosed && this.closedExplicitly) {
      LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Connection was closed by the user");
      return;
    }

    if (isConnected() || !inInitialization) {
      failover();
      return;
    }

    if (shouldAttemptReaderConnection()) {
      failoverReader();
      return;
    }

    if (this.hosts.get(WRITER_CONNECTION_INDEX) == null) {
      failover();
      return;
    }

    try {
      connectTo(this.hosts.get(WRITER_CONNECTION_INDEX));
      if (this.explicitlyReadOnly) {
        topologyService.setLastUsedReaderHost(this.currentHost);
      }
    } catch (SQLException e) {
      failover();
    }
  }

  /**
   * Checks if there should be an attempt to connect to a reader
   *
   * @return True if a reader exists and the connection is read-only
   */
  @RequiresNonNull({"this.hosts"})
  private boolean shouldAttemptReaderConnection(@UnknownInitialization ClusterAwareConnectionProxy this) {
    return this.explicitlyReadOnly && clusterContainsReader();
  }

  /**
   * Connects this dynamic failover connection proxy to the given host
   *
   * @param host The {@link HostInfo} representing the host to connect to
   *
   * @throws SQLException if an error occurs
   */

  @RequiresNonNull({"this.initialConnectionProps", "this.connectionProvider"})
  private synchronized void connectTo(@UnknownInitialization ClusterAwareConnectionProxy this, HostInfo host) throws SQLException {
    try {
      BaseConnection connection = createConnectionForHost(host, this.initialConnectionProps);
      LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Connected to: {0}", host);
      invalidateCurrentConnection();

      boolean readOnly;
      if (this.explicitlyReadOnly) {
        readOnly = true;
      } else if (this.currentConnection != null) {
        readOnly = this.currentConnection.isReadOnly();
      } else {
        readOnly = false;
      }
      syncSessionState(this.currentConnection, connection, readOnly);
      this.currentConnection = connection;
      this.currentHost = host;
      this.inTransaction = false;
    } catch (SQLException e) {
      if (this.currentConnection != null && host != null) {
        logConnectionFailure(host, e);
      }
      throw e;
    }
  }

  /**
   * Creates a new physical connection for the given {@link HostInfo}.
   *
   * @param hostInfo The host info instance
   * @param props The properties that should be passed to the new connection
   *
   * @return The new Connection instance
   * @throws SQLException if an error occurs
   */
  @RequiresNonNull({"this.connectionProvider"})
  protected synchronized @Nullable BaseConnection createConnectionForHost(
          @UnknownInitialization ClusterAwareConnectionProxy this, HostInfo hostInfo, Properties props) throws SQLException {
    String dbname = props.getProperty("PGDBNAME", "");
    if (StringUtils.isNullOrEmpty(dbname) && this.currentConnection != null) {
      String currentDbName = this.currentConnection.getCatalog();
      if (!StringUtils.isNullOrEmpty(currentDbName)) {
        dbname = currentDbName;
      }
    }

    return this.connectionProvider.connect(hostInfo.toHostSpec(), props, hostInfo.getUrl(dbname));
  }

  /**
   * Invalidates the current connection.
   */
  private synchronized void invalidateCurrentConnection(@UnknownInitialization ClusterAwareConnectionProxy this) {
    if (this.currentConnection == null) {
      return;
    }

    this.inTransaction = this.currentConnection.getQueryExecutor().getTransactionState() != TransactionState.IDLE;
    if (this.inTransaction) {
      try {
        if (this.currentConnection != null) {
          this.currentConnection.rollback();
        }
      } catch (SQLException e) {
        // ignore
      }
    }
    invalidateConnection(this.currentConnection);
  }

  /**
   * Invalidates the specified connection by closing it.
   *
   * @param conn The connection instance to invalidate
   */
  protected synchronized void invalidateConnection(@UnknownInitialization ClusterAwareConnectionProxy this,
                                                   @Nullable BaseConnection conn) {
    try {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    } catch (SQLException e) {
      // swallow this exception, current connection should be useless anyway.
    }
  }

  /**
   * Synchronizes session state between two connections, allowing to override the read-only status.
   *
   * @param source The connection where to get state from
   * @param target The connection where to set state
   * @param readOnly The new read-only status
   *
   * @throws SQLException if an error occurs
   */
  protected void syncSessionState(@UnknownInitialization ClusterAwareConnectionProxy this, @Nullable BaseConnection source,
                                  @Nullable BaseConnection target, boolean readOnly) throws SQLException {
    if (target != null) {
      target.setReadOnly(readOnly);
    }

    if (source == null || target == null) {
      return;
    }

    target.setAutoCommit(source.getAutoCommit());
    target.setTransactionIsolation(source.getTransactionIsolation());
  }

  /**
   * Initiates a default failover procedure starting at the given host index. This process tries to
   * connect, sequentially, to the next host in the list. The primary host may or may not be
   * excluded from the connection attempts.
   *
   * @throws SQLException if an error occurs during failover
   */
  @RequiresNonNull({"this.topologyService", "this.initialConnectionProps", "this.readerFailoverHandler", "this.writerFailoverHandler", "this.metrics", "this.connectionProvider", "this.hosts"})
  protected synchronized void failover(@UnknownInitialization ClusterAwareConnectionProxy this) throws SQLException {
    if (this.currentConnection != null) {
      this.inTransaction = this.currentConnection.getQueryExecutor().getTransactionState() != TransactionState.IDLE;
    }

    if (shouldPerformWriterFailover()) {
      failoverWriter();
    } else {
      failoverReader();
    }

    if (this.inTransaction) {
      this.inTransaction = false;
      String transactionResolutionUnknownError = "Transaction resolution unknown. Please re-configure session state if "
              + "required and try restarting the transaction.";
      LOGGER.log(Level.SEVERE, transactionResolutionUnknownError);
      throw new SQLException(transactionResolutionUnknownError, PSQLState.CONNECTION_FAILURE_DURING_TRANSACTION.getState());
    } else {
      String connectionChangedError =
              "The active SQL connection has changed due to a connection failure. Please re-configure session state if required.";
      LOGGER.log(Level.SEVERE, connectionChangedError);
      throw new SQLException(connectionChangedError, PSQLState.COMMUNICATION_LINK_CHANGED.getState());
    }
  }

  /**
   * Checks if the driver should conduct a writer failover
   *
   * @return True if the connection is not explicitly read only
   */
  private boolean shouldPerformWriterFailover(@UnknownInitialization ClusterAwareConnectionProxy this) {
    return !this.explicitlyReadOnly;
  }

  /**
   * Initiates writer failover procedure
   *
   * @throws SQLException If failover failed
   */
  @RequiresNonNull({"this.topologyService", "this.initialConnectionProps", "this.writerFailoverHandler", "this.metrics", "this.hosts"})
  protected void failoverWriter(@UnknownInitialization ClusterAwareConnectionProxy this) throws SQLException {
    LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Starting writer failover procedure.");
    WriterFailoverResult failoverResult = this.writerFailoverHandler.failover(this.hosts);

    if (this.gatherPerfMetricsSetting) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerWriterFailoverProcedureTime(currentTimeMs - this.failoverStartTimeMs);
      this.failoverStartTimeMs = 0;
    }

    if (!failoverResult.isConnected()) {
      processFailoverFailureAndThrowException("Unable to establish SQL connection to the writer instance");
      return;
    }

    if (failoverResult.isNewHost()) {
      // connected to a new writer host; refresh the topology
      List<HostInfo> topology = failoverResult.getTopology();
      if (topology != null) {
        this.hosts = topology;
      }
    }

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(true);
    }

    this.currentHost = this.hosts.get(WRITER_CONNECTION_INDEX);
    this.currentConnection = failoverResult.getNewConnection();
    LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Connected to: {0}", this.currentHost);
  }

  /**
   * Register and log a failover failure, and throw an exception
   *
   * @param message The message that should be logged and included in the thrown exception
   * @throws SQLException representing the failover failure error. This will always be thrown when this method is called
   */
  @RequiresNonNull("this.metrics")
  private void processFailoverFailureAndThrowException(@UnknownInitialization ClusterAwareConnectionProxy this,
                                                       String message) throws SQLException {
    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(false);
    }

    LOGGER.log(Level.SEVERE, message);
    throw new SQLException(message, PSQLState.CONNECTION_UNABLE_TO_CONNECT.getState());
  }

  /**
   * Initiates reader failover procedure
   *
   * @throws SQLException If failover failed
   */
  @RequiresNonNull({"this.topologyService", "this.initialConnectionProps", "this.readerFailoverHandler", "this.metrics", "this.writerFailoverHandler", "this.connectionProvider", "this.hosts"})
  protected void failoverReader(@UnknownInitialization ClusterAwareConnectionProxy this) throws SQLException {
    LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Starting reader failover procedure.");
    ReaderFailoverResult result = readerFailoverHandler.failover(this.hosts, this.currentHost);

    if (this.gatherPerfMetricsSetting) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerReaderFailoverProcedureTime(currentTimeMs - this.failoverStartTimeMs);
      this.failoverStartTimeMs = 0;
    }

    if (result == null || !result.isConnected()) {
      processFailoverFailureAndThrowException("Unable to establish a read-only connection");
      return;
    }

    this.currentConnection = result.getConnection();
    this.currentHost = result.getHost();
    updateTopologyAndConnectIfNeeded(true);
    topologyService.setLastUsedReaderHost(this.currentHost);
    LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Connected to: {0}", this.currentHost);

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(true);
    }
  }

  /**
   * Method that handles whether or not a topology needs to be updated and if a new connection is needed
   *
   * @param forceUpdate If an update must occur
   * @throws SQLException if an error occurs fetching the topology or while conducting failover
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.topologyService", "this.metrics", "this.readerFailoverHandler", "this.writerFailoverHandler", "this.connectionProvider", "this.hosts"})
  protected void updateTopologyAndConnectIfNeeded(@UnknownInitialization ClusterAwareConnectionProxy this,
                                                  boolean forceUpdate) throws SQLException {
    if (!isFailoverEnabled()) {
      return;
    }

    if (!isConnected()) {
      pickNewConnection(false);
      return;
    }

    List<HostInfo> latestTopology = null;
    if (this.currentConnection != null) {
      latestTopology = this.topologyService.getTopology(this.currentConnection, forceUpdate);
    }
    if (latestTopology == null) {
      return;
    }

    this.hosts = latestTopology;
    if (this.currentHost == null) {
      // Most likely the currentHost is not established because the original connection URL specified an IP address,
      // reader cluster endpoint, custom domain, or custom cluster; skip scanning the new topology for the currentHost
      return;
    }

    updateCurrentHost(latestTopology);
  }

  /**
   * Search through the given topology for an instance matching the host details of this.currentHost, and update
   * this.currentHost accordingly. If no match is found, switch to another connection
   *
   * @param latestTopology The topology to scan for a match to this.currentHost
   * @throws SQLException if a match for the current host wasn't found in the given topology and the resulting attempt
   *                      to switch connections encounters an error
   */
  @RequiresNonNull({"this.connectionProvider", "this.topologyService", "this.readerFailoverHandler", "this.writerFailoverHandler", "this.initialConnectionProps", "this.hosts", "this.metrics"})
  private void updateCurrentHost(@UnknownInitialization ClusterAwareConnectionProxy this,
                                 List<HostInfo> latestTopology) throws SQLException {
    HostInfo latestHost = null;
    for (HostInfo host : latestTopology) {
      if (host != null && host.equalsHostPortPair(this.currentHost)) {
        latestHost = host;
        break;
      }
    }

    if (latestHost == null) {
      // current connection host isn't found in the latest topology
      // switch to another connection;
      this.currentHost = null;
      pickNewConnection(false);
    } else {
      // found the same node at different position in the topology
      // adjust current host only; connection is still valid
      this.currentHost = latestHost;
    }
  }

  /**
   * Checks if proxy is connected to RDS-hosted cluster.
   *
   * @return true if proxy is connected to RDS-hosted cluster
   */
  public boolean isRds() {
    return this.isRds;
  }

  /**
   * Checks if proxy is connected to cluster through RDS proxy.
   *
   * @return true if proxy is connected to cluster through RDS proxy
   */
  public synchronized boolean isRdsProxy() {
    return this.isRdsProxy;
  }

  /**
   * Proxies method invocation on connection object. Called when a method is called on the connection object.
   * Changes member variables depending on the Method parameter and initiates failover if needed.
   *
   * @param proxy The proxy object
   * @param method The method being called
   * @param args The arguments to the method
   * @return The results of the method call
   * @throws Throwable if an error occurs
   */
  public synchronized @Nullable Object invoke(Object proxy, Method method, @Nullable Object[] args) throws Throwable {
    this.invokeStartTimeMs = this.gatherPerfMetricsSetting ? System.currentTimeMillis() : 0;
    String methodName = method.getName();

    if (!isForwardingRequired(methodName, args)) {
      return executeMethodWithoutForwarding(methodName, args);
    }

    try {
      updateTopologyAndConnectIfNeeded(false);

      if (this.isClosed && !allowedOnClosedConnection(method)) {
        throw getInvalidInvocationOnClosedConnectionException();
      }

      Object result = null;
      try {
        result = method.invoke(this.currentConnection, args);
        result = proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
      } catch (InvocationTargetException e) {
        dealWithInvocationException(e);
      } catch (IllegalStateException e) {
        dealWithIllegalStateException(e);
      }

      performSpecialMethodHandlingIfRequired(methodName, args);
      return result;
    } catch (InvocationTargetException e) {
      throw e.getCause() != null ? e.getCause() : e;
    } catch (Exception e) {
      throw wrapExceptionIfRequired(method, e);
    }
  }

  /**
   * Check if the method that is about to be invoked requires forwarding to the connection underlying this proxy. The
   * methods indicated below can be handled without needing to perform an invocation against the underlying connection,
   * provided the arguments are valid when required (eg for METHOD_EQUALS and METHOD_ABORT)
   *
   * @param methodName The name of the method that is being called
   * @param args The argument parameters of the method that is being called
   * @return true if we need to explicitly invoke the method indicated by methodName on the underlying connection
   */
  private boolean isForwardingRequired(String methodName, @Nullable Object[] args) {
    return (!METHOD_EQUALS.equals(methodName) || args == null || args.length <= 0 || args[0] == null)
            && !METHOD_HASH_CODE.equals(methodName)
            && !METHOD_CLOSE.equals(methodName)
            && (!METHOD_ABORT.equals(methodName) || args == null || args.length != 1 || args[0] == null)
            && !METHOD_IS_CLOSED.equals(methodName);
  }

  /**
   * Checks if the given method is allowed on closed connections.
   *
   * @param method method name to check
   *
   * @return true if the given method is allowed on closed connections
   */
  protected boolean allowedOnClosedConnection(Method method) {
    String methodName = method.getName();

    return methodName.equals(METHOD_GET_AUTO_COMMIT) || methodName.equals(METHOD_GET_CATALOG)
            || methodName.equals(METHOD_GET_SCHEMA) || methodName.equals(METHOD_GET_TRANSACTION_ISOLATION);
  }

  /**
   * Special handling of method calls that can be handled without making an explicit invocation against the connection
   * underlying this proxy. See {@link #isForwardingRequired(String, Object[])}
   *
   * @param methodName The name of the method being called
   * @param args The argument parameters of the method that is being called
   * @return The results of the special method handling, according to which method was called
   * @throws Throwable if the method called was abort or close and an error occurs while performing these actions
   */
  private @Nullable Object executeMethodWithoutForwarding(String methodName, @Nullable Object[] args) throws Throwable {
    if (METHOD_EQUALS.equals(methodName) && args != null && args.length > 0 && args[0] != null) {
      // Let args[0] "unwrap" to its InvocationHandler if it is a proxy.
      return args[0].equals(this);
    }

    if (METHOD_HASH_CODE.equals(methodName)) {
      return this.hashCode();
    }

    if (METHOD_CLOSE.equals(methodName)) {
      doClose();
      if (this.gatherPerfMetricsSetting) {
        this.metrics.reportMetrics(LOGGER);
      }
      this.isClosed = true;
      this.closedReason = "Connection explicitly closed.";
      this.closedExplicitly = true;
      return null;
    }

    if (METHOD_ABORT.equals(methodName) && args != null && args.length == 1 && args[0] != null) {
      doAbort((Executor) args[0]);
      if (this.gatherPerfMetricsSetting) {
        this.metrics.reportMetrics(LOGGER);
      }
      this.isClosed = true;
      this.closedReason = "Connection explicitly closed.";
      this.closedExplicitly = true;
      return null;
    }

    if (METHOD_IS_CLOSED.equals(methodName)) {
      return this.isClosed;
    }
    // should never reach this statement, as the conditions in this method were previously checked in the method
    // calling this class using the isForwardingRequired method
    return null;
  }

  /**
   * Closes current connection.
   *
   * @throws SQLException if an error occurs
   */
  protected synchronized void doClose() throws SQLException {
    if (this.currentConnection != null) {
      this.currentConnection.close();
    }
  }

  /**
   * Aborts current connection using the given executor.
   *
   * @param executor  The {@link Executor} implementation which will
   *      be used by abort
   *
   * @throws SQLException if an error occurs
   */
  protected synchronized void doAbort(Executor executor) throws SQLException {
    if (this.currentConnection != null) {
      this.currentConnection.abort(executor);
    }
  }

  /**
   * Create a {@link SQLException} indicating that a method was invalidly called against a closed connection
   *
   * @return a {@link SQLException} indicating that a method was invalidly called against a closed connection
   */
  private SQLException getInvalidInvocationOnClosedConnectionException() {
    String reason = "No operations allowed after connection closed.";
    if (!StringUtils.isNullOrEmpty(this.closedReason)) {
      reason += ("  " + this.closedReason);
    }
    return new SQLException(reason, PSQLState.CONNECTION_DOES_NOT_EXIST.getState());
  }

  /**
   * Deals with InvocationException from proxied objects.
   *
   * @param e The Exception instance to check
   *
   * @throws Throwable if an error occurs
   * @throws InvocationTargetException if an error occurs
   */
  protected synchronized void dealWithInvocationException(InvocationTargetException e)
          throws Throwable, InvocationTargetException {
    dealWithOriginalException(e.getTargetException(), e);
  }

  /**
   *  Deals with illegal state exception from proxied objects.
   *
   * @param e The Exception instance to check
   * @throws Throwable if an error occurs
   */
  protected void dealWithIllegalStateException(IllegalStateException e) throws Throwable {
    dealWithOriginalException(e.getCause(), e);
  }

  /**
   * Method used to deal with the exception. Will decide if failover should happen, if the original
   * exception should be thrown or if the wrapper exception should be thrown.
   *
   * @param originalException The original cause of the exception
   * @param wrapperException The exception that wraps the original exception
   * @throws Throwable The original cause of the exception will be thrown when available,
   *     otherwise the exception itself will be thrown
   */
  @RequiresNonNull({"this.metrics"})
  private synchronized void dealWithOriginalException(@Nullable Throwable originalException, Exception wrapperException) throws Throwable {
    if (originalException != null) {
      LOGGER.log(Level.WARNING,
              "[ClusterAwareConnectionProxy] Detected an exception while executing a command: {0}",
              originalException.getMessage());
      LOGGER.log(Level.FINER, Util.stackTraceToString(originalException, this.getClass()));
      if (this.lastExceptionDealtWith != originalException && shouldExceptionTriggerConnectionSwitch(originalException)) {

        if (this.gatherPerfMetricsSetting) {
          long currentTimeMs = System.currentTimeMillis();
          this.metrics.registerFailureDetectionTime(currentTimeMs - this.invokeStartTimeMs);
          this.invokeStartTimeMs = 0;
          this.failoverStartTimeMs = currentTimeMs;
        }
        invalidateCurrentConnection();
        pickNewConnection(false);
        this.lastExceptionDealtWith = originalException;
      }
      throw originalException;
    }
    throw wrapperException;
  }

  /**
   * Checks whether or not the exception should result in failing over to a new connection
   *
   * @param t The exception to check
   * @return True if failover should happen
   */
  protected boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {

    if (!isFailoverEnabled()) {
      LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Cluster-aware failover is disabled");
      return false;
    }

    String sqlState = null;
    if (t instanceof SQLException) {
      sqlState = ((SQLException) t).getSQLState();
    }

    if (sqlState != null) {
      // connection error
      return PSQLState.isConnectionError(sqlState) || PSQLState.COMMUNICATION_ERROR.getState().equals(sqlState);
    }

    return false;
  }

  /**
   * Perform any additional required steps after a method invocation that has been made through this proxy. These steps
   * are only required for the methods that are indicated below.
   *
   * @param methodName The name of the method being called
   * @param args The argument parameters of the method that is being called
   * @throws SQLException if failover is invoked when calling {@link #connectToWriterIfRequired(boolean, boolean)}
   */
  private void performSpecialMethodHandlingIfRequired(String methodName, @Nullable Object[] args) throws SQLException {
    if (METHOD_SET_AUTO_COMMIT.equals(methodName)) {
      if (args[0] != null) {
        boolean autoCommit = (boolean) args[0];
        this.inTransaction = !autoCommit;
      }
    }

    if (METHOD_COMMIT.equals(methodName) || METHOD_ROLLBACK.equals(methodName)) {
      this.inTransaction = false;
    }

    if (METHOD_SET_READ_ONLY.equals(methodName) && args != null && args.length > 0 && args[0] != null) {
      boolean originalReadOnlyValue  = this.explicitlyReadOnly;
      this.explicitlyReadOnly = (boolean) args[0];
      LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] explicitlyReadOnly={0}", this.explicitlyReadOnly);
      connectToWriterIfRequired(originalReadOnlyValue, this.explicitlyReadOnly);
    }
  }

  /**
   * Checks if it's required to connect to a writer
   *
   * @param originalReadOnlyValue Original value of read only
   * @param newReadOnlyValue The new value of read only
   *
   * @throws SQLException if {@link #failover()} is invoked
   */
  private void connectToWriterIfRequired(boolean originalReadOnlyValue, boolean newReadOnlyValue) throws SQLException {
    if (!originalReadOnlyValue || newReadOnlyValue || (this.currentHost != null && this.currentHost.isWriter())) {
      return;
    }

    if (this.hosts.get(WRITER_CONNECTION_INDEX) == null) {
      if (this.gatherPerfMetricsSetting) {
        this.failoverStartTimeMs = System.currentTimeMillis();
      }
      failover();
      return;
    }

    try {
      connectTo(this.hosts.get(WRITER_CONNECTION_INDEX));
    } catch (SQLException e) {
      if (this.gatherPerfMetricsSetting) {
        this.failoverStartTimeMs = System.currentTimeMillis();
      }
      failover();
    }
  }

  /**
   * Check if the captured exception must be wrapped by an unchecked exception, and perform the wrapping if so
   *
   * @param method The method that was being invoked when the given exception occurred
   * @param e The exception that occurred while invoking the given method
   * @return an exception indicating the failure that occurred while invoking the given method
   */
  Throwable wrapExceptionIfRequired(Method method, Throwable e) {
    Class<?>[] declaredException = method.getExceptionTypes();
    for (Class<?> declEx : declaredException) {
      if (declEx.isAssignableFrom(e.getClass())) {
        return e;
      }
    }
    return new IllegalStateException(e.getMessage(), e);
  }

  /**
   * Checks if proxy is connected to cluster that can report its topology.
   *
   * @return true if proxy is connected to cluster that can report its topology
   */
  public boolean isClusterTopologyAvailable() {
    return this.isClusterTopologyAvailable;
  }

  /**
   * Accessor method for currentConnection
   *
   * @return the current connection object. Returns null if it doesn't exist.
   */
  public @Nullable Connection getConnection() {
    return this.currentConnection;
  }

  /**
   * Log a message indicating that a connection attempt to the given host failed
   *
   * @param host The host used in the failed connection attempt
   * @param e The exception that occurred while attempting to connect to the given host
   */
  private void logConnectionFailure(@UnknownInitialization ClusterAwareConnectionProxy this, HostInfo host, SQLException e) {
    String instanceType = host.isWriter() ? "writer" : "reader";
    String msg = "Connection to " + instanceType + " host '" + host.getHostPortPair() + "' failed";
    LOGGER.log(Level.WARNING, msg + ": " + e.getMessage());
    LOGGER.log(Level.FINER, Util.stackTraceToString(e, this.getClass()));
  }

  /**
   * Log information about the current topology
   */
  @RequiresNonNull({"this.hosts"})
  private void logTopology(@UnderInitialization ClusterAwareConnectionProxy this) {
    StringBuilder msg = new StringBuilder();
    for (int i = 0; i < this.hosts.size(); i++) {
      HostInfo hostInfo = this.hosts.get(i);
      msg.append("\n   [")
              .append(i)
              .append("]: ")
              .append(hostInfo == null ? "<null>" : hostInfo.getHost());
    }
    LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Topology obtained: {0}", msg.toString());
  }

  /**
   * Proxy class to intercept and deal with errors that may occur in any object bound to the current connection.
   */
  class JdbcInterfaceProxy implements InvocationHandler {
    Object invokeOn;

    JdbcInterfaceProxy(Object toInvokeOn) {
      this.invokeOn = toInvokeOn;
    }

    public @Nullable Object invoke(Object proxy, Method method, @Nullable Object[] args) throws Throwable {
      if (METHOD_EQUALS.equals(method.getName()) && args != null && args[0] != null) {
        // Let args[0] "unwrap" to its InvocationHandler if it is a proxy.
        return args[0].equals(this);
      }

      synchronized (ClusterAwareConnectionProxy.this) {
        Object result = null;

        try {
          result = method.invoke(this.invokeOn, args);
          result = proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
        } catch (InvocationTargetException e) {
          dealWithInvocationException(e);
        } catch (IllegalStateException e) {
          dealWithIllegalStateException(e);
        }

        return result;
      }
    }
  }

  /**
   * If the given return type is or implements a JDBC interface, proxies the given object so that we can catch SQL
   * errors and fire a connection switch.
   *
   * @param returnType The type the object instance to proxy is supposed to be
   * @param toProxy The object instance to proxy
   *
   * @return The proxied object or the original one if it does not implement a JDBC interface
   */
  protected @Nullable Object proxyIfReturnTypeIsJdbcInterface(Class<?> returnType, @Nullable Object toProxy) {
    if (toProxy != null) {
      if (Util.isJdbcInterface(returnType)) {
        Class<?> toProxyClass = toProxy.getClass();
        return Proxy.newProxyInstance(toProxyClass.getClassLoader(),
                Util.getImplementedInterfaces(toProxyClass),
                getNewJdbcInterfaceProxy(toProxy));
      }
    }
    return toProxy;
  }

  /**
   * Method to handle invocations
   *
   * @param toProxy The object to proxy
   * @return A {@link JdbcInterfaceProxy} that returns the result of the invocation
   */
  protected InvocationHandler getNewJdbcInterfaceProxy(Object toProxy) {
    return new JdbcInterfaceProxy(toProxy);
  }
}
