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
import org.postgresql.util.HostSpec;
import org.postgresql.util.IpAddressUtils;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.Util;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class is a proxy of org.postgresql.core.BaseConnection that contains functionality to monitor the underlying
 * connection status and fail over to other DB instances when the connection encounters communications exceptions
 * or cluster topology changes.
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

  protected @Nullable Throwable lastHandledException = null;

  /**
   * Constructor for ClusterAwareConnectionProxy
   *
   * @param hostSpec The {@link HostSpec} information for the desired connection
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   *
   * @throws SQLException if an error occurs while creating this instance
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
   * @param hostSpec The {@link HostSpec} info for the desired connection
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs while attempting to initialize the proxy connection
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

    if (!Util.isNullOrEmpty(this.clusterInstanceHostPatternSetting)) {
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
   * Initializes the connection proxy using the user-configured host pattern setting
   *
   * @param connectionStringHostSpec The {@link HostSpec} info for the desired connection
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs while attempting to initialize the proxy connection
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
   * Retrieves the host and port number from the user-configured host pattern setting
   *
   * @return A {@link HostSpec} object containing the host and port number
   * @throws SQLException if an invalid value was used for the host pattern setting
   */
  @RequiresNonNull("this.rdsDnsAnalyzer")
  private HostSpec getHostSpecFromHostPatternSetting(@UnderInitialization ClusterAwareConnectionProxy this) throws SQLException {
    HostSpec hostSpec = Util.parseUrl(this.clusterInstanceHostPatternSetting);
    if (hostSpec == null) {
      throw new SQLException("Invalid value in 'clusterInstanceHostPattern' configuration setting - the value could not be parsed");
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
      String invalidDnsPatternError = "Invalid value set for the 'clusterInstanceHostPattern' configuration setting - "
              + "the host pattern must contain a '?' character as a placeholder for the DB instance identifiers of the "
              + "instances in the cluster";
      LOGGER.log(Level.SEVERE, invalidDnsPatternError);
      throw new SQLException(invalidDnsPatternError);
    }

    identifyRdsType(hostPattern);
    if (this.isRdsProxy) {
      String rdsProxyInstancePatternError =
              "An RDS Proxy url can't be used as the 'clusterInstanceHostPattern' configuration setting.";
      LOGGER.log(Level.SEVERE, rdsProxyInstancePatternError);
      throw new SQLException(rdsProxyInstancePatternError);
    }

    if (this.rdsDnsAnalyzer.isRdsCustomClusterDns(hostPattern)) {
      String rdsCustomClusterInstancePatternError =
              "An RDS Custom Cluster endpoint can't be used as the 'clusterInstanceHostPattern' configuration setting";
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
   * Initializes the connection proxy expecting that the connection does not provide cluster information. This occurs when the
   * connection is to an IP address, custom domain, or non-RDS instance. In the first two cases, if cluster information
   * does exist, the user should have set the clusterInstanceHostPattern setting to enable use of the topology. An
   * exception will be thrown if this method is called but topology information was retrieved using the connection.
   *
   * @param hostSpec The {@link HostSpec} containing the information for the desired connection
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs while establishing the connection proxy, or if topology information was
   *         retrieved while establishing the connection
   */
  @RequiresNonNull({"this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler", "this.rdsDnsAnalyzer", "this.initialConnectionProps", "this.metrics", "this.hosts"})
  private void initExpectingNoTopology(@UnderInitialization ClusterAwareConnectionProxy this, HostSpec hostSpec,
                                       Properties props, String url) throws SQLException {
    setClusterId(hostSpec.getHost(), hostSpec.getPort());
    this.topologyService.setClusterInstanceTemplate(createClusterInstanceTemplate(hostSpec.getHost(), hostSpec.getPort()));
    createConnectionAndInitializeTopology(hostSpec, props, url);

    if (this.isClusterTopologyAvailable) {
      String instanceHostPatternRequiredError =
              "The 'clusterInstanceHostPattern' configuration property is required when an IP address or custom domain "
              + "is used to connect to a cluster that provides topology information. If you would instead like to connect "
              + "without failover functionality, set the 'enableClusterAwareFailover' configuration property to false.";
      LOGGER.log(Level.SEVERE, instanceHostPatternRequiredError);
      throw new SQLException(instanceHostPatternRequiredError);
    }
  }

  /**
   * Initializes the connection proxy using a connection URL
   *
   * @param hostSpec The {@link HostSpec} containing the information for the desired connection
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException If an error occurs while establishing the connection proxy
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
    if (!Util.isNullOrEmpty(this.clusterIdSetting)) {
      this.topologyService.setClusterId(this.clusterIdSetting);
    } else if (this.isRdsProxy) {
      // Each proxy is associated with a single cluster so it's safe to use the RDS Proxy Url as the cluster ID
      this.topologyService.setClusterId(host + ":" + port);
    } else if (this.isRds) {
      // If it's a cluster endpoint or reader cluster endpoint, then let's use it as the cluster ID
      String clusterRdsHostUrl = this.rdsDnsAnalyzer.getRdsClusterHostUrl(host);
      if (!Util.isNullOrEmpty(clusterRdsHostUrl)) {
        this.topologyService.setClusterId(clusterRdsHostUrl + ":" + port);
      }
    }
  }

  /**
   * Creates an instance template that the topology service will use to form topology information for each
   * instance in the cluster
   *
   * @param host The new host for the {@link HostInfo} object
   * @param port The port for the connection
   * @return A {@link HostInfo} class for cluster instance template
   */
  private HostInfo createClusterInstanceTemplate(@UnderInitialization ClusterAwareConnectionProxy this, String host, int port) {
    return new HostInfo(host, null, port,  false);
  }

  /**
   * Creates a connection and establishes topology information for connections that provide it
   *
   * @param hostSpec The {@link HostSpec} containing the information for the desired connection
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs while establishing the connection proxy
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
      // Either URL was not a cluster endpoint or cached topology did not exist - connect directly to URL
      this.currentConnection = this.connectionProvider.connect(hostSpec, props, url);
    }
    return connectedUsingCachedTopology;
  }

  /**
   * Checks if the connection proxied by this class is open and usable
   *
   * @return true if the connection is open and usable
   */
  synchronized boolean isConnected(@UnknownInitialization ClusterAwareConnectionProxy this) {
    try {
      return this.currentConnection != null && !this.currentConnection.isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * Attempts a connection using cached topology information
   *
   * @throws SQLException if an error occurs while establishing the connection
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.topologyService", "this.connectionProvider"})
  private void attemptConnectionUsingCachedTopology(@UnderInitialization ClusterAwareConnectionProxy this) throws SQLException {
    @Nullable List<HostInfo> cachedHosts = topologyService.getCachedTopology();
    if (cachedHosts != null && !cachedHosts.isEmpty()) {
      this.hosts = cachedHosts;
      HostInfo candidateHost = getCandidateHostForInitialConnection();
      if (candidateHost != null) {
        connectToHost(candidateHost);
      }
    }
  }

  /**
   * Retrieves a host to use for the initial connection
   *
   * @return {@link HostInfo} for a candidate to use for the initial connection
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
   * Retrieves a reader to use for the initial connection
   *
   * @return {@link HostInfo} for a reader candidate to use for the initial connection
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
   * Checks if the topology information contains the given host
   *
   * @param host the host to check
   * @return true if the host exists in the topology
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
   * @return true if there are readers in the cluster
   */
  @RequiresNonNull({"this.hosts"})
  private boolean clusterContainsReader(@UnknownInitialization ClusterAwareConnectionProxy this)  {
    return this.hosts.size() > 1;
  }

  /**
   * Retrieves a random host from the topology
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
   * Initializes topology information for the database we have connected to, when it is available
   *
   * @param hostSpec The {@link HostSpec} that we used to establish the current connection
   * @param connectedUsingCachedTopology a boolean representing whether the current connection was established using
   *                                     cached topology information
   */
  @RequiresNonNull({"this.topologyService", "this.rdsDnsAnalyzer", "this.hosts"})
  private synchronized void initTopology(@UnderInitialization ClusterAwareConnectionProxy this, HostSpec hostSpec,
                                         boolean connectedUsingCachedTopology) {
    if (this.currentConnection != null) {
      List<HostInfo> topology = this.topologyService.getTopology(this.currentConnection, false);
      this.hosts = topology.isEmpty() ? this.hosts : topology;
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
   * Updates the current host in the class. Will set the currentHost to null if the given hostSpec does not specify
   * the writer cluster and does not exist in the topology information.
   *
   * @param hostSpec The {@link HostSpec} that we used to establish the current connection
   * @param connectedUsingCachedTopology a boolean representing whether the current connection was established using
   *                                     cached topology information
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
   * If failover is enabled, validate that the current connection is valid
   *
   * @param connectedUsingCachedTopology a boolean representing whether the current connection was established using
   *                                     cached topology information
   * @throws SQLException if an error occurs while validating the connection
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
      if (this.currentConnection != null) {
        try {
          this.currentConnection.getQueryExecutor().setNetworkTimeout(this.failoverSocketTimeout * 1000);
        } catch (IOException e) {
          throw new SQLException(e.getMessage(), PSQLState.UNEXPECTED_ERROR.getState());
        }
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
   * Validates the initial connection by making sure that we are connected and that we are not connected to a reader
   * instance using a read-write connection, which could happen if we used stale cached topology information to establish
   * the connection.
   *
   * @param connectedUsingCachedTopology a boolean representing whether the current connection was established using
   *                                     cached topology information
   * @throws SQLException if an error occurs during failover
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.topologyService", "this.metrics", "this.readerFailoverHandler", "this.writerFailoverHandler", "this.connectionProvider", "this.hosts"})
  private synchronized void validateInitialConnection(@UnderInitialization ClusterAwareConnectionProxy this,
                                                      boolean connectedUsingCachedTopology) throws SQLException {
    if (!isConnected()) {
      switchConnection(true);
      return;
    }

    if (!invalidCachedWriterConnection(connectedUsingCachedTopology)) {
      return;
    }

    try {
      connectToHost(this.hosts.get(WRITER_CONNECTION_INDEX));
    } catch (SQLException e) {
      failover();
    }
  }

  /**
   * If we used cached topology information to make the initial connection, checks whether we
   * connected to a host that was marked as a writer in the cache but is in fact a reader according
   * to the fresh topology. This could happen if the cached topology was stale.
   *
   * @param connectedUsingCachedTopology a boolean representing whether the current connection was established using
   *                                     cached topology information
   * @return Ttue if we expected to be connected to a writer according to cached topology information,
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
  protected synchronized void switchConnection(@UnknownInitialization ClusterAwareConnectionProxy this,
                                               boolean inInitialization) throws SQLException {
    if (this.isClosed && this.closedExplicitly) {
      LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Connection was closed by the user");
      return;
    }

    if (isConnected() || !inInitialization || this.hosts.isEmpty()) {
      failover();
      return;
    }

    if (shouldAttemptReaderConnection()) {
      failoverReader();
      return;
    }

    try {
      connectToHost(this.hosts.get(WRITER_CONNECTION_INDEX));
      if (this.explicitlyReadOnly) {
        topologyService.setLastUsedReaderHost(this.currentHost);
      }
    } catch (SQLException e) {
      failover();
    }
  }

  /**
   * Checks if we should attempt failover to a reader instance in {@link #switchConnection(boolean)}
   *
   * @return True if a reader exists and the connection is read-only
   */
  @RequiresNonNull({"this.hosts"})
  private boolean shouldAttemptReaderConnection(@UnknownInitialization ClusterAwareConnectionProxy this) {
    return this.explicitlyReadOnly && clusterContainsReader();
  }

  /**
   * Connect to the host specified by the given {@link HostInfo} and set the resulting connection as the underlying connection
   * for this proxy
   *
   * @param host The {@link HostInfo} representing the host to connect to
   *
   * @throws SQLException if an error occurs while connecting
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.connectionProvider"})
  private synchronized void connectToHost(@UnknownInitialization ClusterAwareConnectionProxy this, HostInfo host) throws SQLException {
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
   * Creates a connection to the host specified by the given {@link HostInfo}
   *
   * @param hostInfo the {@link HostInfo} specifying the host to connect to
   * @param props The properties that should be passed to the new connection
   *
   * @return a connection to the given host
   * @throws SQLException if an error occurs while connecting to the given host
   */
  @RequiresNonNull({"this.connectionProvider"})
  protected synchronized @Nullable BaseConnection createConnectionForHost(
          @UnknownInitialization ClusterAwareConnectionProxy this, HostInfo hostInfo, Properties props) throws SQLException {
    String dbname = props.getProperty("PGDBNAME", "");
    if (Util.isNullOrEmpty(dbname) && this.currentConnection != null) {
      String currentDbName = this.currentConnection.getCatalog();
      if (!Util.isNullOrEmpty(currentDbName)) {
        dbname = currentDbName;
      }
    }

    return this.connectionProvider.connect(hostInfo.toHostSpec(), props, hostInfo.getUrl(dbname));
  }

  /**
   * Attempt rollback if the current connection is in a transaction, then close the connection. Rollback may not be
   * possible as the current connection might be broken or closed when this method is called.
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
   * Close the given connection if it is not already closed
   *
   * @param conn the connection to close
   */
  protected synchronized void invalidateConnection(@UnknownInitialization ClusterAwareConnectionProxy this,
                                                   @Nullable BaseConnection conn) {
    try {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    } catch (SQLException e) {
      // ignore
    }
  }

  /**
   * Set the target connection read-only status according to the given parameter, and the autocommit and transaction
   * isolation status according to the source connection state.
   *
   * @param source the source connection that holds the desired state for the target connection
   * @param target the target connection that should hold the desired state from the source connection at the end of this
   *               method call
   * @param readOnly The new read-only status
   *
   * @throws SQLException if an error occurs while setting the target connection's state
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
   * Initiates the failover procedure. This process tries to establish a new connection to an instance in the topology.
   *
   * @throws SQLException upon successful failover to indicate that failover has occurred and session state should be
   *                      reconfigured by the user. May also throw a SQLException if failover is unsuccessful.
   *
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
   * @return true if the connection is not explicitly read only
   */
  private boolean shouldPerformWriterFailover(@UnknownInitialization ClusterAwareConnectionProxy this) {
    return !this.explicitlyReadOnly;
  }

  /**
   * Initiates the writer failover procedure
   *
   * @throws SQLException if failover was unsuccessful
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

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(true);
    }

    if (!failoverResult.getTopology().isEmpty()) {
      this.hosts = failoverResult.getTopology();
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
   * Initiates the reader failover procedure
   *
   * @throws SQLException if failover was unsuccessful
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

    if (!result.isConnected()) {
      processFailoverFailureAndThrowException("Unable to establish a read-only connection");
      return;
    }

    if (this.gatherPerfMetricsSetting) {
      this.metrics.registerFailoverConnects(true);
    }

    this.currentHost = result.getHost();
    this.currentConnection = result.getConnection();
    updateTopologyAndConnectIfNeeded(true);
    topologyService.setLastUsedReaderHost(this.currentHost);
    LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Connected to: {0}", this.currentHost);
  }

  /**
   * Method that updates the topology when necessary and establishes a new connection if the proxy is not connected or
   * the current host is not found in the new topology.
   *
   * @param forceUpdate a boolean used to force an update instead of the default behavior of updating only when necessary
   * @throws SQLException if an error occurs fetching the topology or while conducting failover
   */
  @RequiresNonNull({"this.initialConnectionProps", "this.topologyService", "this.metrics", "this.readerFailoverHandler", "this.writerFailoverHandler", "this.connectionProvider", "this.hosts"})
  protected void updateTopologyAndConnectIfNeeded(@UnknownInitialization ClusterAwareConnectionProxy this,
                                                  boolean forceUpdate) throws SQLException {
    if (!isFailoverEnabled()) {
      return;
    }

    if (!isConnected()) {
      switchConnection(false);
      return;
    }

    List<HostInfo> latestTopology = null;
    if (this.currentConnection != null) {
      latestTopology = this.topologyService.getTopology(this.currentConnection, forceUpdate);
    }

    if (latestTopology == null || latestTopology.isEmpty()) {
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
      switchConnection(false);
    } else {
      // found the same node at different position in the topology
      // adjust current host only; connection is still valid
      this.currentHost = latestHost;
    }
  }

  /**
   * Checks if this proxy is connected to an RDS-hosted cluster.
   *
   * @return true if this proxy is connected to an RDS-hosted cluster
   */
  public boolean isRds() {
    return this.isRds;
  }

  /**
   * Checks if this proxy is connected to a cluster using RDS proxy.
   *
   * @return true if this proxy is connected to a cluster using RDS proxy
   */
  public synchronized boolean isRdsProxy() {
    return this.isRdsProxy;
  }

  /**
   * Invoke a method on the connection underlying this proxy. Called automatically when a method is called against the
   * proxied connection. Member variables are changed depending on the method parameter and failover is initiated if
   * a communications problem occurs while executing the invocation.
   *
   * @param proxy The proxied connection
   * @param method The method being called
   * @param args The arguments to the method
   * @return The results of the method call
   * @throws Throwable if an error occurs while invoking the method against the proxy
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
        result = wrapWithProxyIfNeeded(method.getReturnType(), result);
      } catch (InvocationTargetException e) {
        processInvocationException(e);
      } catch (IllegalStateException e) {
        processIllegalStateException(e);
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
   * Checks whether the given method can be executed against a closed connection
   *
   * @param method the method to check
   *
   * @return true if the given method can be executed against a closed connection
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
   * Close the current connection if it exists
   *
   * @throws SQLException if an error occurs while closing the connection
   */
  protected synchronized void doClose() throws SQLException {
    if (this.currentConnection != null) {
      this.currentConnection.close();
    }
  }

  /**
   * Abort the current connection if it exists
   *
   * @param executor  The {@link Executor} implementation which will
   *      be used by abort
   *
   * @throws SQLException if an error occurs while aborting the current connection
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
    if (!Util.isNullOrEmpty(this.closedReason)) {
      reason += ("  " + this.closedReason);
    }
    return new SQLException(reason, PSQLState.CONNECTION_DOES_NOT_EXIST.getState());
  }

  /**
   * Analyze the given exception and initiate failover if required, then throw the exception
   *
   * @param e the {@link InvocationTargetException} to analyze
   * @throws Throwable when the given exception contains a target exception, failover occurs, or another error is
   *                   encountered while handling the exception
   * @throws InvocationTargetException when the given exception does not contain a target exception
   */
  protected synchronized void processInvocationException(InvocationTargetException e)
          throws Throwable, InvocationTargetException {
    processException(e.getTargetException(), e);
  }

  /**
   * Analyze the given exception and initiate failover if required, then throw the exception
   *
   * @param e the {@link IllegalStateException} to analyze
   * @throws Throwable this error or the cause of this error. May also throw an error if failover occurs or another error
   *                   is encountered while handling the exception
   */
  protected void processIllegalStateException(IllegalStateException e) throws Throwable {
    processException(e.getCause(), e);
  }

  /**
   * Method used to deal with an exception. Will decide if failover should happen, if the original
   * exception should be thrown, or if the wrapper exception should be thrown.
   *
   * @param originalException The original cause of the exception
   * @param wrapperException The exception that wraps the original exception
   * @throws Throwable The original cause of the exception will be thrown when available,
   *     otherwise the exception itself will be thrown
   */
  @RequiresNonNull({"this.metrics"})
  private synchronized void processException(@Nullable Throwable originalException, Exception wrapperException) throws Throwable {
    if (originalException != null) {
      LOGGER.log(Level.WARNING,
              "[ClusterAwareConnectionProxy] Detected an exception while executing a command: {0}",
              originalException.getMessage());
      LOGGER.log(Level.FINER, Util.stackTraceToString(originalException, this.getClass()));
      if (this.lastHandledException != originalException && isConnectionSwitchRequired(originalException)) {

        if (this.gatherPerfMetricsSetting) {
          long currentTimeMs = System.currentTimeMillis();
          this.metrics.registerFailureDetectionTime(currentTimeMs - this.invokeStartTimeMs);
          this.invokeStartTimeMs = 0;
          this.failoverStartTimeMs = currentTimeMs;
        }
        invalidateCurrentConnection();
        switchConnection(false);
        this.lastHandledException = originalException;
      }
      throw originalException;
    }
    throw wrapperException;
  }

  /**
   * Checks whether or not the exception indicates that we should initiate failover to a new connection
   *
   * @param t The exception to check
   * @return True if failover should be initiated
   */
  protected boolean isConnectionSwitchRequired(Throwable t) {

    if (!isFailoverEnabled()) {
      LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Cluster-aware failover is disabled");
      return false;
    }

    String sqlState = null;
    if (t instanceof SQLException) {
      sqlState = ((SQLException) t).getSQLState();
    }

    if (sqlState != null) {
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
   * Connect to the writer if setReadOnly(false) was called and we are currently connected to a reader instance.
   *
   * @param originalReadOnlyValue the original read-only value, before setReadOnly was called
   * @param newReadOnlyValue the new read-only value, as passed to the setReadOnly call
   *
   * @throws SQLException if {@link #failover()} is invoked
   */
  private void connectToWriterIfRequired(boolean originalReadOnlyValue, boolean newReadOnlyValue) throws SQLException {
    if (!shouldReconnectToWriter(originalReadOnlyValue, newReadOnlyValue) || this.hosts.isEmpty()) {
      return;
    }

    try {
      connectToHost(this.hosts.get(WRITER_CONNECTION_INDEX));
    } catch (SQLException e) {
      if (this.gatherPerfMetricsSetting) {
        this.failoverStartTimeMs = System.currentTimeMillis();
      }
      failover();
    }
  }

  /**
   * Checks if the connection needs to be switched to a writer connection when setReadOnly(false) is called. This should
   * only be true the read only value was switched from true to false and we aren't connected to a writer.
   *
   * @param originalReadOnlyValue the original read-only value, before setReadOnly was called
   * @param newReadOnlyValue the new read-only value, as passed to the setReadOnly call
   * @return true if we should reconnect to the writer instance, according to the setReadOnly call
   */
  private boolean shouldReconnectToWriter(boolean originalReadOnlyValue, boolean newReadOnlyValue) {
    return originalReadOnlyValue && !newReadOnlyValue && (this.currentHost == null || !this.currentHost.isWriter());
  }

  /**
   * Wrap the given throwable in an IllegalStateException if required; otherwise, just return the throwable
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
   * This class is a proxy for objects created through the proxied connection (for example, {@link java.sql.Statement} and
   * {@link java.sql.ResultSet}. Similarly to ClusterAwareConnectionProxy, this proxy class monitors the underlying object
   * for communications exceptions and initiates failover when required.
   */
  class Proxy implements InvocationHandler {
    Object invocationTarget;

    Proxy(Object invocationTarget) {
      this.invocationTarget = invocationTarget;
    }

    public @Nullable Object invoke(Object proxy, Method method, @Nullable Object[] args) throws Throwable {
      if (METHOD_EQUALS.equals(method.getName()) && args != null && args[0] != null) {
        return args[0].equals(this);
      }

      synchronized (ClusterAwareConnectionProxy.this) {
        Object result = null;

        try {
          result = method.invoke(this.invocationTarget, args);
          result = wrapWithProxyIfNeeded(method.getReturnType(), result);
        } catch (InvocationTargetException e) {
          processInvocationException(e);
        } catch (IllegalStateException e) {
          processIllegalStateException(e);
        }

        return result;
      }
    }
  }

  /**
   * Checks whether the return type of an invoked method implements or is a JDBC interface, and
   * returns a proxy of the object returned by the method if so. This is done so that we can monitor the
   * object for communications exceptions and initiate failovers when required (see JdbcInterfaceProxy).
   *
   * @param returnType the return type of the invoked method
   * @param toProxy the object returned by the invoked method
   *
   * @return a proxy of the object returned by the invoked method, if it implements or is
   *         a JDBC interface. Otherwise, simply returns the object itself.
   */
  protected @Nullable Object wrapWithProxyIfNeeded(Class<?> returnType, @Nullable Object toProxy) {
    if (toProxy == null || !Util.isJdbcInterface(returnType)) {
      return toProxy;
    }

    Class<?> toProxyClass = toProxy.getClass();
    return java.lang.reflect.Proxy.newProxyInstance(toProxyClass.getClassLoader(),
            Util.getImplementedInterfaces(toProxyClass),
            new Proxy(toProxy));
  }
}
