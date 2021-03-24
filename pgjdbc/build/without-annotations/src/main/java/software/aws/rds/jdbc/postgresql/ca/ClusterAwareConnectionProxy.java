/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;


import org.postgresql.core.BaseConnection;
import org.postgresql.core.TransactionState;
import org.postgresql.util.*;
import org.postgresql.PGProperty;

// import org.checkerframework.checker.initialization.qual.UnderInitialization;
// import org.checkerframework.checker.nullness.qual.EnsuresNonNull;
// import org.checkerframework.checker.nullness.qual.Nullable;
// import org.checkerframework.checker.nullness.qual.RequiresNonNull;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import software.aws.rds.jdbc.postgresql.ca.metrics.ClusterAwareMetrics;


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
  protected static final int DEFAULT_SOCKET_TIMEOUT_MS = 10000;
  protected static final int DEFAULT_CONNECT_TIMEOUT_MS = 30000;

  private final Pattern auroraDnsPattern =
      Pattern.compile(
          "(.+)\\.(proxy-|cluster-|cluster-ro-|cluster-custom-)?([a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private final Pattern auroraCustomClusterPattern =
      Pattern.compile(
          "(.+)\\.(cluster-custom-[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);
  private final Pattern auroraProxyDnsPattern =
      Pattern.compile(
          "(.+)\\.(proxy-[a-zA-Z0-9]+\\.[a-zA-Z0-9\\-]+\\.rds\\.amazonaws\\.com)",
          Pattern.CASE_INSENSITIVE);

  protected static final int WRITER_CONNECTION_INDEX = 0; // writer host is always stored at index 0
  private static transient final Logger LOGGER = Logger.getLogger(ClusterAwareConnectionProxy.class.getName());

  protected final String originalUrl;
  protected boolean explicitlyReadOnly = false;
  protected boolean inTransaction = false;
  protected boolean isClusterTopologyAvailable = false;
  protected boolean isRdsProxy = false;
  protected boolean isRds = false;
  protected TopologyService topologyService;
  protected List<HostInfo> hosts = new ArrayList<>();
  protected Properties originalProperties;
  protected WriterFailoverHandler writerFailoverHandler;
  protected ReaderFailoverHandler readerFailoverHandler;
  protected ConnectionProvider connectionProvider;
  protected /* @Nullable */ BaseConnection currentConnection;
  protected /* @Nullable */ HostInfo currentHost;
  protected boolean isClosed = false;
  protected boolean closedExplicitly = false;
  protected /* @Nullable */ String closedReason = null;

  protected ClusterAwareMetrics metrics = new ClusterAwareMetrics();
  private long invokeStartTimeMs;
  private long failoverStartTimeMs;

  // Configuration settings
  protected boolean enableFailoverSetting = true;
  protected int clusterTopologyRefreshRateMsSetting;
  protected /* @Nullable */ String clusterIdSetting;
  protected /* @Nullable */ String clusterInstanceHostPatternSetting;
  protected boolean gatherPerfMetricsSetting;
  protected int failoverTimeoutMsSetting;
  protected int failoverClusterTopologyRefreshRateMsSetting;
  protected int failoverWriterReconnectIntervalMsSetting;
  protected int failoverReaderConnectTimeoutMsSetting;
  protected int failoverConnectTimeoutMs;
  protected int failoverSocketTimeoutMs;

  protected /* @Nullable */ Throwable lastExceptionDealtWith = null;

  /**
   * Proxy class to intercept and deal with errors that may occur in any object bound to the current connection.
   */
  class JdbcInterfaceProxy implements InvocationHandler {
    Object invokeOn;

    JdbcInterfaceProxy(Object toInvokeOn) {
      this.invokeOn = toInvokeOn;
    }

    public /* @Nullable */ Object invoke(Object proxy, Method method, /* @Nullable */ Object[] args) throws Throwable {
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
   * Method to handle invocations
   *
   * @param toProxy The object to proxy
   * @return A {@link JdbcInterfaceProxy} that returns the result of the invocation
   */
  protected InvocationHandler getNewJdbcInterfaceProxy(Object toProxy) {
    return new JdbcInterfaceProxy(toProxy);
  }

  /**
   * If the given return type is or implements a JDBC interface, proxies the given object so that we can catch SQL errors and fire a connection switch.
   *
   * @param returnType The type the object instance to proxy is supposed to be
   * @param toProxy The object instance to proxy
   *
   * @return The proxied object or the original one if it does not implement a JDBC interface
   */
  protected /* @Nullable */ Object proxyIfReturnTypeIsJdbcInterface(Class<?> returnType, /* @Nullable */ Object toProxy) {
    if (toProxy != null) {
      if (Util.isJdbcInterface(returnType)) {
        Class<?> toProxyClass = toProxy.getClass();
        return Proxy.newProxyInstance(toProxyClass.getClassLoader(), Util.getImplementedInterfaces(toProxyClass), getNewJdbcInterfaceProxy(toProxy));
      }
    }
    return toProxy;
  }

  /**
   * Instantiates a new AuroraConnectionProxy for the given host and properties.
   *
   * @param hostSpec The HostSpec info for the host to connect to
   * @param props The Properties specifying the connection and failover configuration
   * @param url The URL to connect to
   *
   * @throws SQLException if an error occurs
   */
  /* @EnsuresNonNull({"this.originalUrl", "this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler"}) */
  public ClusterAwareConnectionProxy(HostSpec hostSpec, Properties props, String url) throws SQLException {
    this.originalUrl = url;
    this.originalProperties = new Properties(props);
    initSettings(props);

    AuroraTopologyService topologyService = new AuroraTopologyService();
    topologyService.setPerformanceMetrics(metrics, this.gatherPerfMetricsSetting);
    topologyService.setRefreshRate(this.clusterTopologyRefreshRateMsSetting);
    this.topologyService = topologyService;

    this.connectionProvider = new BasicConnectionProvider();
    this.readerFailoverHandler = new ClusterAwareReaderFailoverHandler(
        this.topologyService,
        this.connectionProvider,
        this.failoverTimeoutMsSetting,
        this.failoverReaderConnectTimeoutMsSetting);
    this.writerFailoverHandler = new ClusterAwareWriterFailoverHandler(
        this.topologyService,
        this.connectionProvider,
        this.readerFailoverHandler,
        this.failoverTimeoutMsSetting,
        this.failoverClusterTopologyRefreshRateMsSetting,
        this.failoverWriterReconnectIntervalMsSetting);

    initProxy(hostSpec, props, url);
  }

  /* @EnsuresNonNull({"this.originalUrl", "this.topologyService", "this.connectionProvider", "this.writerFailoverHandler", "this.readerFailoverHandler"}) */
  ClusterAwareConnectionProxy(HostSpec hostSpec, Properties props, String url, ConnectionProvider connectionProvider, TopologyService service, WriterFailoverHandler writerFailoverHandler, ReaderFailoverHandler readerFailoverHandler) throws SQLException {
    this.originalUrl = url;
    initSettings(props);

    this.topologyService = service;
    this.topologyService.setRefreshRate(this.clusterTopologyRefreshRateMsSetting);

    this.writerFailoverHandler = writerFailoverHandler;
    this.readerFailoverHandler = readerFailoverHandler;

    this.connectionProvider = connectionProvider;
    initProxy(hostSpec, props, url);
  }

  /**
   * Validates properties and set default properties for Failover.
   */
  private synchronized void initSettings(/* @UnderInitialization */ ClusterAwareConnectionProxy this, Properties props)
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

    if (props.getProperty("socketTimeout") != null) {
      this.failoverSocketTimeoutMs = Integer.valueOf(props.getProperty("socketTimeout"));
    } else {
      this.failoverSocketTimeoutMs = DEFAULT_SOCKET_TIMEOUT_MS;
    }

    if (props.getProperty("connectTimeout") != null) {
      this.failoverConnectTimeoutMs = Integer.valueOf(props.getProperty("connectTimeout"));
    } else {
      this.failoverConnectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
    }

    PGProperty.CONNECT_TIMEOUT.set(props, this.failoverConnectTimeoutMs);
    PGProperty.SOCKET_TIMEOUT.set(props, this.failoverSocketTimeoutMs);

    this.enableFailoverSetting = PGProperty.ENABLE_CLUSTER_AWARE_FAILOVER.getBoolean(props);
    this.gatherPerfMetricsSetting = PGProperty.GATHER_PERF_METRICS.getBoolean(props);
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
  public boolean isRdsProxy() {
    return this.isRdsProxy;
  }

  /**
   * Checks if cluster-aware failover is enabled/possible.
   *
   * @return true if cluster-aware failover is enabled
   */
  public boolean isFailoverEnabled() {
    return this.enableFailoverSetting
        && !this.isRdsProxy
        && this.isClusterTopologyAvailable;
  }

  /**
   * Initializes the connection proxy
   *
   * @param hostSpec The HostSpec info for the host to connect to
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs
   */
  /* @RequiresNonNull({"this.connectionProvider", "this.topologyService"}) */
  private void initProxy(/* @UnderInitialization */ ClusterAwareConnectionProxy this, HostSpec hostSpec, Properties props, String url) throws SQLException {
    if (!this.enableFailoverSetting) {
      // Use a standard default connection - no further initialization required
      this.currentConnection = this.connectionProvider.connect(hostSpec, props, url);
      return;
    }
    LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Cluster-aware failover is enabled.");
    LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] 'clusterId' configuration setting: {0}", this.clusterIdSetting);
    LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] 'clusterInstanceHostPatternSetting' configuration setting: {0}", this.clusterInstanceHostPatternSetting);

    if (!StringUtils.isNullOrEmpty(this.clusterInstanceHostPatternSetting)) {
      initFromHostPatternSetting(hostSpec, props, url);
    } else if (IpAddressUtils.isIPv4(hostSpec.getHost())
        || IpAddressUtils.isIPv6(hostSpec.getHost())) {
      initExpectingNoTopology(hostSpec, props, url);
    } else {
      this.isRds = isRdsDns(hostSpec.getHost());
      LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] isRds={0}", this.isRds);

      this.isRdsProxy = isRdsProxyDns(hostSpec.getHost());
      LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] isRdsProxy={0}", this.isRdsProxy);

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
  /* @RequiresNonNull("this.topologyService") */
  private void initFromHostPatternSetting(/* @UnderInitialization */ ClusterAwareConnectionProxy this, HostSpec connectionStringHostSpec, Properties props, String url) throws SQLException {
    HostSpec patternSettingHostSpec = getHostSpecFromHostPatternSetting();
    String instanceHostPattern = patternSettingHostSpec.getHost();
    int instanceHostPort = patternSettingHostSpec.getPort() != HostInfo.NO_PORT ? patternSettingHostSpec.getPort() : connectionStringHostSpec.getPort();
    this.isRds = isRdsDns(instanceHostPattern);
    LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] isRds={0}", this.isRds);

    this.topologyService.setClusterInstanceTemplate(
        createClusterInstanceTemplate(instanceHostPattern, props));

    setClusterId(patternSettingHostSpec);
    createConnectionAndInitializeTopology(connectionStringHostSpec, this.originalProperties, url);
  }

  /**
   * Retrieves the host and port number from host pattern
   *
   * @return A {@link HostSpec} object containing the host and port number
   * @throws SQLException if an error occurs
   */
  private HostSpec getHostSpecFromHostPatternSetting(/* @UnderInitialization */ ClusterAwareConnectionProxy this) throws SQLException {
    HostSpec hostSpec = ConnectionUrlParser.parseHostPortPair(this.clusterInstanceHostPatternSetting);
    if (hostSpec == null) {
      throw new SQLException("Invalid value in 'clusterInstanceHostPattern' configuration property.");
    }
    String hostPattern = hostSpec.getHost();

    if (!isDnsPatternValid(hostPattern)) {
      String invalidDnsPatternError = "Invalid value set for the 'clusterInstanceHostPattern' configuration property.";
      LOGGER.log(Level.SEVERE, invalidDnsPatternError);
      throw new SQLException(invalidDnsPatternError);
    }

    if (isRdsProxyDns(hostPattern)) {
      String rdsProxyInstancePatternError = "RDS Proxy url can't be used for the 'clusterInstanceHostPattern' configuration property";
      LOGGER.log(Level.SEVERE, rdsProxyInstancePatternError);
      throw new SQLException(rdsProxyInstancePatternError);
    }

    this.isRdsProxy = false;
    LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] isRdsProxy={0}", this.isRdsProxy);

    if (isRdsCustomClusterDns(hostPattern)) {
      LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] isRdsCustomCluster={0}", true);
      String rdsCustomClusterInstancePatternError = "RDS Custom Cluster endpoint can't be used for the 'clusterInstanceHostPattern' configuration property";
      LOGGER.log(Level.SEVERE, rdsCustomClusterInstancePatternError);
      throw new SQLException(rdsCustomClusterInstancePatternError);
    }
    return hostSpec;
  }

  /**
   * Initializes connection without expecting a current topology
   *
   * @param hostSpec The HostSpec info for the host to connect to
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs
   */
  private void initExpectingNoTopology(/* @UnderInitialization */ ClusterAwareConnectionProxy this, HostSpec hostSpec, Properties props, String url) throws SQLException {
    this.topologyService.setClusterInstanceTemplate(createClusterInstanceTemplate(hostSpec.getHost(), props));

    if (!StringUtils.isNullOrEmpty(this.clusterIdSetting)) {
      this.topologyService.setClusterId(this.clusterIdSetting);
    }

    createConnectionAndInitializeTopology(hostSpec, props, url);

    if (this.isClusterTopologyAvailable) {
      String instanceHostPatternRequiredError = "The 'clusterInstanceHostPattern' configuration property is required when an IP address or custom domain is used to connect to the cluster.";
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
  private void initFromConnectionString(/* @UnderInitialization */ ClusterAwareConnectionProxy this, HostSpec hostSpec, Properties props, String url) throws SQLException {
    String rdsInstanceHostPattern = getRdsInstanceHostPattern(hostSpec.getHost());
    if (rdsInstanceHostPattern == null) {
      String unexpectedConnectionStringPattern = "The provided connection string does not appear to match an expected Aurora DNS pattern. Please set the 'clusterInstanceHostPattern' configuration property to specify the host pattern for the cluster you are trying to connect to.";
      LOGGER.log(Level.SEVERE, unexpectedConnectionStringPattern);
      throw new SQLException(unexpectedConnectionStringPattern);
    }

    this.topologyService.setClusterInstanceTemplate(
        createClusterInstanceTemplate(rdsInstanceHostPattern, props));

    setClusterId(hostSpec);
    createConnectionAndInitializeTopology(hostSpec, props, url);
  }

  /**
   * Sets the clusterID in the topology service
   *
   * @param hostSpec The host and port number
   */
  private void setClusterId(HostSpec hostSpec) {
    if (!StringUtils.isNullOrEmpty(this.clusterIdSetting)) {
      this.topologyService.setClusterId(this.clusterIdSetting);
    } else if (this.isRdsProxy) {
      // Each proxy is associated with a single cluster so it's safe to use RDS Proxy Url as cluster identification
      this.topologyService.setClusterId(hostSpec.getHost() + ":" + hostSpec.getPort());
    } else if (this.isRds) {
      // If it's cluster endpoint or reader cluster endpoint,
      // then let's use as cluster identification
      String clusterRdsHostUrl = getRdsClusterHostUrl(hostSpec.getHost());
      if (!StringUtils.isNullOrEmpty(clusterRdsHostUrl)) {
        this.topologyService.setClusterId(clusterRdsHostUrl + ":" + hostSpec.getPort());
      }
    }
  }

  /**
   * Creates an instance template for the topology service
   *
   * @param host The new host for the {@link HostInfo} object
   * @param props The {@link Properties} for the connection
   * @return
   */
  private HostInfo createClusterInstanceTemplate(/* @UnderInitialization */ ClusterAwareConnectionProxy this, String host, Properties props) {
    PGProperty.CONNECT_TIMEOUT.set(props, this.failoverConnectTimeoutMs);
    PGProperty.SOCKET_TIMEOUT.set(props, this.failoverSocketTimeoutMs);

    HostInfo hostInfo = new HostInfo(this.originalUrl, null,  false, props);
    hostInfo.updateHostProp(host);
    return hostInfo;
  }

  /**
   * Creates a connection and initializes the topology
   *
   * @param hostSpec The HostSpec info for the host to connect to
   * @param props The {@link Properties} specifying the connection and failover configuration
   * @param url The URL to connect to
   * @throws SQLException if an error occurs
   */
  private synchronized void createConnectionAndInitializeTopology(HostSpec hostSpec, Properties props, String url) throws SQLException {
    String host = hostSpec.getHost();
    boolean connectedUsingCachedTopology = false;

    if (isRdsClusterDns(host)) {
      this.explicitlyReadOnly = isReaderClusterDns(host);
      LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] explicitlyReadOnly={0}", this.explicitlyReadOnly);
      try {
        attemptConnectionUsingCachedTopology(props);
        if(this.currentConnection != null && this.currentHost != null) {
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
    initTopology(hostSpec, connectedUsingCachedTopology);
    finalizeConnection(props, connectedUsingCachedTopology);

    PGProperty.CONNECT_TIMEOUT.set(props, this.failoverConnectTimeoutMs);
    PGProperty.SOCKET_TIMEOUT.set(props, this.failoverSocketTimeoutMs);

    this.currentConnection.setClientInfo(props);
    this.currentConnection.setNetworkTimeout(null, this.failoverSocketTimeoutMs);
  }

  /**
   * Finalizing the connection
   *
   * @param props The {@link Properties} for the connection
   * @param connectedUsingCachedTopology True if connection is using cached topology
   * @throws SQLException if an error occurs
   */
  private void finalizeConnection(Properties props, boolean connectedUsingCachedTopology) throws SQLException {
    if (this.isFailoverEnabled()) {
      logTopology();
      if (this.gatherPerfMetricsSetting) {
        this.failoverStartTimeMs = System.currentTimeMillis();
      }
      validateInitialConnection(props, connectedUsingCachedTopology);
      if(this.currentHost != null && this.explicitlyReadOnly) {
        topologyService.setLastUsedReaderHost(this.currentHost);
      }
    }
  }

  private void logTopology() {
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
   * Attempts a connection using a cached topology
   *
   * @param props The {@link Properties} for the connection
   * @throws SQLException if an error occurs
   */
  private void attemptConnectionUsingCachedTopology(Properties props) throws SQLException {
    List<HostInfo> cachedHosts = topologyService.getCachedTopology();
    if (cachedHosts == null || cachedHosts.isEmpty()) {
      return;
    }

    this.hosts = cachedHosts;
    HostInfo candidateHost = getCandidateHostForInitialConnection();
    if (candidateHost != null) {
      connectTo(candidateHost);
    }
  }

  /**
   * Retrieves a host for initial connection
   *
   * @return {@link HostInfo} for a candidate host
   */
  private /* @Nullable */ HostInfo getCandidateHostForInitialConnection() {
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
  private HostInfo getCandidateReaderForInitialConnection() {
    HostInfo lastUsedReader = topologyService.getLastUsedReaderHost();
    if (topologyContainsHost(lastUsedReader)) {
      return lastUsedReader;
    }

    return clusterContainsReader() ? getRandomReaderHost() : null;
  }

  /**
   * Initializes topology
   *
   * @param hostSpec The current host to connect if the proxy does not hae a current host
   * @param connectedUsingCachedTopology True if connection is using a cached topology
   */
  private synchronized void initTopology(HostSpec hostSpec, boolean connectedUsingCachedTopology) {
    if (this.currentConnection != null) {
      List<HostInfo> topology = this.topologyService.getTopology(this.currentConnection, false);
      this.hosts = topology == null ? this.hosts : topology;
    }
    this.isClusterTopologyAvailable = !this.hosts.isEmpty();
    LOGGER.log(Level.FINER, "[ClusterAwareConnectionProxy] isClusterTopologyAvailable={0}", this.isClusterTopologyAvailable);

    // if we connected using the cached topology, there's a chance that the currentHost details are stale,
    // let's update it here. Otherwise, we should try to set the currentHost if it isn't set already.
    if (connectedUsingCachedTopology || (this.currentConnection != null && this.currentHost == null)) {
      updateCurrentHost(this.currentHost == null ? hostSpec : this.currentHost.toHostSpec(), connectedUsingCachedTopology);
    }
  }

  /**
   * Validates initial connection
   *
   * @param props The {@link Properties} object used for the connection
   * @param connectedUsingCachedTopology True if using cached topology
   * @throws SQLException if an error occurs during failover
   */
  private synchronized void validateInitialConnection(Properties props, boolean connectedUsingCachedTopology) throws SQLException {
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
   * Updates the current host in the class. Will set the currentHost to null if the hostSpec is not
   * part of the cluster
   *
   * @param hostSpec The current host to set as current
   * @param connectedUsingCachedTopology True if connected using a cached topology
   */
  private void updateCurrentHost(HostSpec hostSpec, boolean connectedUsingCachedTopology) {
    String connUrlHost = hostSpec.getHost();
    if(!connectedUsingCachedTopology && isWriterClusterDns(connUrlHost) && !this.hosts.isEmpty()) {
      this.currentHost = this.hosts.get(WRITER_CONNECTION_INDEX);
    } else {
      for(HostInfo host : this.hosts) {
        if(hostSpec.toString().equals(host.getHostPortPair())) {
          this.currentHost = host;
          return;
        }
      }
      this.currentHost = null;
    }
  }

  /**
   * Checks if the DNS pattern is valid
   *
   * @param pattern The string to check
   * @return True if the pattern is valid
   */
  private boolean isDnsPatternValid(String pattern) {
    return pattern.contains("?");
  }

  /**
   * Checks if the connection is a standard RDS DNS connection
   *
   * @param host The host of the connection
   * @return True if the connection contains the standard RDS DNS pattern
   */
  private boolean isRdsDns(/* @UnderInitialization */ ClusterAwareConnectionProxy this, String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    return matcher.find();
  }

  /**
   * Checks if the connection is Proxy DNS connection
   *
   * @param host The host of the connection
   * @return True if the connection contains a Proxy DNS pattern
   */
  private boolean isRdsProxyDns(/* @UnderInitialization */ ClusterAwareConnectionProxy this, String host) {
    Matcher matcher = auroraProxyDnsPattern.matcher(host);
    return matcher.find();
  }

  /**
   * Checks if the connection is a custom cluster name
   *
   * @param host The host of the connection
   * @return True if the host is a custom cluster name
   */
  private boolean isRdsCustomClusterDns(String host) {
    Matcher matcher = auroraCustomClusterPattern.matcher(host);
    return matcher.find();
  }

  /**
   * Retrieves the cluster host URl from a connection string
   *
   * @param host The host of the connection
   * @return the cluster host URL from the connection string
   */
  private /* @Nullable */ String getRdsClusterHostUrl(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    String clusterKeyword = getClusterKeyword(matcher);
    if ("cluster-".equalsIgnoreCase(clusterKeyword)
        || "cluster-ro-".equalsIgnoreCase(clusterKeyword)) {
      return matcher.group(1) + ".cluster-" + matcher.group(3); // always RDS cluster endpoint
    }
    return null;
  }

  /**
   * Retrieve the instance host pattern from the host
   *
   * @param host The host of the connection
   * @return The instance host pattern that will be used to set the cluster instance template in the
   *     topology service
   */
  private /* @Nullable */ String getRdsInstanceHostPattern(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    if (matcher.find()) {
      return "?." + matcher.group(3);
    }
    return null;
  }

  /**
   * Checks if the host is an RDS cluster using DNS
   *
   * @param host The host of the connection
   * @return True if the host is an RDS cluster using DNS
   */
  private boolean isRdsClusterDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    String clusterKeyword = getClusterKeyword(matcher);
    return "cluster-".equalsIgnoreCase(clusterKeyword)
        || "cluster-ro-".equalsIgnoreCase(clusterKeyword);
  }

  /**
   * Checks if the host is connected to a writer cluster
   *
   * @param host The host of the connection
   * @return True if the host is connected to the writer cluster
   */
  private boolean isWriterClusterDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    return "cluster-".equalsIgnoreCase(getClusterKeyword(matcher));
  }

  /**
   * Checks if the host is connected to a read-only cluster
   *
   * @param host The host of the connection
   * @return True if the host is read-only
   */
  private boolean isReaderClusterDns(String host) {
    Matcher matcher = auroraDnsPattern.matcher(host);
    return "cluster-ro-".equalsIgnoreCase(getClusterKeyword(matcher));
  }

  /**
   * Used to get a specific keyword from an instance
   *
   * @param matcher The matcher object that contains the string to parse
   * @return The cluster keyword
   */
  private /* @Nullable */ String getClusterKeyword(Matcher matcher) {
    if (matcher.find()
        && matcher.group(2) != null
        && matcher.group(1) != null
        && !matcher.group(1).isEmpty()) {
      return matcher.group(2);
    }
    return null;
  }

  /**
   * Retrieves a random host from topology
   *
   * @return A random {@link HostInfo} from the topology
   */
  private HostInfo getRandomReaderHost() {
    int max = this.hosts.size() - 1;
    int min = WRITER_CONNECTION_INDEX + 1;
    int readerIndex = (int) (Math.random() * ((max - min) + 1)) + min;
    return this.hosts.get(readerIndex);
  }

  /**
   *
   *
   * @param connectedUsingCachedTopology True if connected using cached topology
   * @return True if it's an invalid
   */
  private boolean invalidCachedWriterConnection(boolean connectedUsingCachedTopology) {
    if(this.explicitlyReadOnly || !connectedUsingCachedTopology) {
      return false;
    }

    return this.currentHost == null || !this.currentHost.isWriter();
  }

  /**
   * Checks if the driver should conduct a writer failover
   *
   * @return True if the connection is not explicitly read only
   */
  private boolean shouldPerformWriterFailover() {
    return !this.explicitlyReadOnly;
  }

  /**
   * Invalidates the current connection.
   */
  private synchronized void invalidateCurrentConnection() {
    if(this.currentConnection == null) {
      return;
    }

    this.inTransaction = this.currentConnection.getQueryExecutor().getTransactionState() != TransactionState.IDLE;
    if (this.inTransaction) {
      try {
        this.currentConnection.rollback();
      } catch (SQLException e) {
        // eat
      }
    }
    invalidateConnection(this.currentConnection);
  }

  /**
   * Invalidates the specified connection by closing it.
   *
   * @param conn The connection instance to invalidate
   */
  protected synchronized void invalidateConnection(/* @Nullable */ BaseConnection conn) {
    try {
      if (conn != null && !conn.isClosed()) {
        conn.close();
      }
    } catch (SQLException e) {
      // swallow this exception, current connection should be useless anyway.
    }
  }

  /**
   * Deals with InvocationException from proxied objects.
   *
   * @param e The Exception instance to check
   *
   * @throws SQLException if an error occurs
   * @throws Throwable if an error occurs
   * @throws InvocationTargetException if an error occurs
   */
  protected synchronized void dealWithInvocationException(InvocationTargetException e)
      throws SQLException, Throwable, InvocationTargetException {
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
   * @throws Throwable
   */
  private void dealWithOriginalException(/* @Nullable */ Throwable originalException, Exception wrapperException) throws Throwable {
    if (originalException != null) {
      LOGGER.log(Level.WARNING, "[ClusterAwareConnectionProxy] Detected an exception while executing a command: {0}", originalException.getMessage());
      LOGGER.log(Level.FINER, Util.stackTraceToString(originalException, this.getClass()));
      if (this.lastExceptionDealtWith != originalException && shouldExceptionTriggerConnectionSwitch(originalException)) {

        if (this.gatherPerfMetricsSetting){
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

    // if (t instanceof EOFException || // Can not read response from server)
    //     t instanceof SSLException) { // Incomplete packets from server may cause SSL communication issues
    //   return true;
    // }

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
   * Checks if there is a underlying connection for this proxy.
   *
   * @return true if there is a connection
   */
  synchronized boolean isConnected() {
    try {
      return this.currentConnection != null && !this.currentConnection.isClosed();
    } catch (SQLException e) {
      return false;
    }
  }

  /**
   * Picks a new connection. Conducts failover if needed.
   *
   * @param inInitialization True if driver is initializing
   * @throws SQLException if failover fails
   */
  protected synchronized void pickNewConnection(boolean inInitialization) throws SQLException {
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
  private boolean shouldAttemptReaderConnection() {
    return this.explicitlyReadOnly && clusterContainsReader();
  }

  /**
   * Checks if the cluster contains a reader
   *
   * @return True if there are readers in the cluster
   */
  private boolean clusterContainsReader() {
    return this.hosts.size() > 1;
  }

  /**
   * Checks if the topology in the proxy class contains a host
   *
   * @param host The host to check
   * @return True if the host is part of the topology
   */
  private boolean topologyContainsHost(/* @Nullable */ HostInfo host) {
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
   * Connects this dynamic failover connection proxy to the given host
   *
   * @param host The {@link HostInfo} representing the host to connect to
   *
   * @throws SQLException if an error occurs
   */
  private synchronized void connectTo(HostInfo host) throws SQLException {
    try {
      BaseConnection connection = createConnectionForHost(host, this.originalProperties);
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
      if (this.currentConnection != null) {
        StringBuilder msg =
            new StringBuilder("Connection to ")
                .append(host.isWriter() ? "writer" : "reader")
                .append(" host '")
                .append(host.getHostPortPair())
                .append("' failed");
        LOGGER.log(Level.WARNING, msg.toString() + ": " + e.getMessage());
        LOGGER.log(Level.FINER, Util.stackTraceToString(e, this.getClass()));
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
  protected synchronized BaseConnection createConnectionForHost(HostInfo hostInfo, Properties props)
      throws SQLException {
    if (this.currentConnection != null && props.get("PGDBNAME") == null) {
      String db = this.currentConnection.getCatalog();
      if (!StringUtils.isNullOrEmpty(db)) {
        props.put("PGDBNAME", db);
      }
    }
    return this.connectionProvider.connect(hostInfo.toHostSpec(), props, hostInfo.getUrl() == null ? originalUrl : hostInfo.getUrl());
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
  protected void syncSessionState(/* @Nullable */ BaseConnection source, /* @Nullable */ BaseConnection target, boolean readOnly) throws SQLException {
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
   * @throws SQLException if an error occurs during
   */
  protected synchronized void failover() throws SQLException {

    if(this.currentConnection != null) {
      this.inTransaction = this.currentConnection.getQueryExecutor().getTransactionState() != TransactionState.IDLE;
    }

    if (shouldPerformWriterFailover()) {
      failoverWriter();
    } else {
      failoverReader();
    }

    if (this.inTransaction) {
      this.inTransaction = false;
      String transactionResolutionUnknownError = "Transaction resolution unknown. Please re-configure session state if required and try restarting the transaction.";
      LOGGER.log(Level.SEVERE, transactionResolutionUnknownError);
      throw new SQLException(transactionResolutionUnknownError, PSQLState.CONNECTION_FAILURE_DURING_TRANSACTION.getState());
    } else {
      String connectionChangedError = "The active SQL connection has changed due to a connection failure. Please re-configure session state if required.";
      LOGGER.log(Level.SEVERE, connectionChangedError);
      throw new SQLException(connectionChangedError, PSQLState.COMMUNICATION_LINK_CHANGED.getState());
    }
  }

  /**
   * Initiates writer failover procedure
   *
   * @throws SQLException If failover failed
   */
  protected void failoverWriter() throws SQLException {
    LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Starting writer failover procedure.");
    WriterFailoverResult failoverResult = this.writerFailoverHandler.failover(this.hosts);

    if (this.gatherPerfMetricsSetting) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerWriterFailoverProcedureTime(currentTimeMs - this.failoverStartTimeMs);
      this.failoverStartTimeMs = 0;
    }

    if (!failoverResult.isConnected()) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerFailoverConnects(false);
      }
      String unsuccessfulWriterFailoverError = "Unable to establish SQL connection to the writer instance";
      LOGGER.log(Level.SEVERE, unsuccessfulWriterFailoverError);
      throw new SQLException(unsuccessfulWriterFailoverError, PSQLState.CONNECTION_UNABLE_TO_CONNECT.getState());
    } else if (failoverResult.isNewHost()) {
      // connected to a new writer host; refresh the topology
      this.hosts = failoverResult.getTopology();
      this.currentHost = this.hosts.get(WRITER_CONNECTION_INDEX);
      this.currentConnection = failoverResult.getNewConnection();

      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerFailoverConnects(true);
      }
    } else {
      // successfully re-connected to the same writer node
      this.currentHost = this.hosts.get(WRITER_CONNECTION_INDEX);
      this.currentConnection = failoverResult.getNewConnection();
      LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Connected to: {0}", this.currentHost);

      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerFailoverConnects(true);
      }
    }
  }

  /**
   * Initiates reader failover procedure
   *
   * @throws SQLException If failover failed
   */
  protected void failoverReader() throws SQLException {
    LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Starting reader failover procedure.");
    ReaderFailoverResult result = readerFailoverHandler.failover(this.hosts, this.currentHost);

    if (this.gatherPerfMetricsSetting) {
      long currentTimeMs = System.currentTimeMillis();
      this.metrics.registerReaderFailoverProcedureTime(currentTimeMs - this.failoverStartTimeMs);
      this.failoverStartTimeMs = 0;
    }

    if (!result.isConnected()) {
      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerFailoverConnects(false);
      }
      String unsuccessfulReaderFailoverError = "Unable to establish a read-only connection";
      LOGGER.log(Level.SEVERE, unsuccessfulReaderFailoverError);
      throw new SQLException(unsuccessfulReaderFailoverError, PSQLState.CONNECTION_UNABLE_TO_CONNECT.getState());
    } else {
      this.currentConnection = result.getConnection();
      this.currentHost = result.getHost();
      updateTopologyAndConnectIfNeeded(true);
      topologyService.setLastUsedReaderHost(this.currentHost);
      LOGGER.log(Level.FINE, "[ClusterAwareConnectionProxy] Connected to: {0}");

      if (this.gatherPerfMetricsSetting) {
        this.metrics.registerFailoverConnects(true);
      }
    }
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
   * Method that handles whether or not a topology needs to be updated and if a new connection is needed
   *
   * @param forceUpdate If an update must occur
   * @throws SQLException if an error occurs fetching the topology or while conducting failover
   */
  protected void updateTopologyAndConnectIfNeeded(boolean forceUpdate) throws SQLException {
    if (!isFailoverEnabled()) {
      return;
    }

    if (!isConnected()) {
      pickNewConnection(false);
      return;
    }

    List<HostInfo> latestTopology = this.topologyService.getTopology(this.currentConnection, forceUpdate);
    if (latestTopology == null) {
      return;
    }

    this.hosts = latestTopology;
    if(this.currentHost == null) {
      // Most likely the currentHost is not established because the original connection URL specified an IP address,
      // reader cluster endpoint, custom domain, or custom cluster; skip scanning the new topology for the currentHost
      return;
    }

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
   * Proxies method invocation on connection object. Called when a method is called on the connection object.
   * Changes member variables depending on the Method parameter and initiates failover if needed.
   *
   * @param proxy The proxy object
   * @param method The method being called
   * @param args The arguments to the method
   * @return The results of the method call
   * @throws Throwable if an error occurs
   */
  public synchronized /* @Nullable */ Object invoke(Object proxy, Method method, /* @Nullable */ Object[] args) throws Throwable {
    this.invokeStartTimeMs = this.gatherPerfMetricsSetting ? System.currentTimeMillis() : 0;
    String methodName = method.getName();

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

    try {
      updateTopologyAndConnectIfNeeded(false);

      if (this.isClosed && !allowedOnClosedConnection(method)) {
        String reason = "No operations allowed after connection closed.";
        if (!StringUtils.isNullOrEmpty(this.closedReason)) {
          reason += ("  " + this.closedReason);
        }
        throw new SQLException(reason, PSQLState.CONNECTION_DOES_NOT_EXIST.getState());
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

      if (METHOD_SET_AUTO_COMMIT.equals(methodName)) {
        if(args[0] != null) {
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

      return result;
    } catch (InvocationTargetException e) {
      throw e.getCause() != null ? e.getCause() : e;
    } catch (Exception e) {
      // Check if the captured exception must be wrapped by an unchecked exception.
      Class<?>[] declaredException = method.getExceptionTypes();
      for (Class<?> declEx : declaredException) {
        if (declEx.isAssignableFrom(e.getClass())) {
          throw e;
        }
      }
      throw new IllegalStateException(e.getMessage(), e);
    }
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
   * Checks if it's required to connect to a writer
   *
   * @param originalReadOnlyValue Original value of read only
   * @param newReadOnlyValue The new value of read only
   *
   * @throws SQLException if failover failed
   */
  private void connectToWriterIfRequired(boolean originalReadOnlyValue, boolean newReadOnlyValue) throws SQLException {
    if(!originalReadOnlyValue || newReadOnlyValue) {
      return;
    }

    if (this.currentHost == null || !this.currentHost.isWriter()) {
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
  }

  /**
   * Accessor method for currentConnection
   *
   * @return the current connection object. Returns null if it doesn't exist.
   */
  public /* @Nullable */ Connection getConnection() {
    return this.currentConnection;
  }
}
