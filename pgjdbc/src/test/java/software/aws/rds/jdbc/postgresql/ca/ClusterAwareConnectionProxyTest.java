/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import software.aws.rds.jdbc.postgresql.Driver;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.postgresql.PGProperty;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.QueryExecutor;
import org.postgresql.core.TransactionState;
import org.postgresql.util.HostSpec;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * ClusterAwareConnectionProxyTest class.
 **/
public class ClusterAwareConnectionProxyTest {
  private static final String TEST_DB = "test";
  private static final int DEFAULT_PORT = 5432;

  /**
   * Tests {@link ClusterAwareConnectionProxy} return original connection if failover is not
   * enabled.
   */
  @Test
  public void testFailoverDisabled() throws SQLException {
    final String url =
        "jdbc:postgresql:aws://somehost:1234/test?"
            + PGProperty.ENABLE_CLUSTER_AWARE_FAILOVER.getName()
            + "=false";
    final HostSpec hostSpec = urlToHostSpec(url);
    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final Properties props = Driver.parseURL(url, null);
    when(mockConnectionProvider.connect(hostSpec,props, url)).thenReturn(mockConn);
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec, props ,url, mockConnectionProvider, mockTopologyService, Mockito.mock(WriterFailoverHandler.class), Mockito.mock(ReaderFailoverHandler.class), new RdsDnsAnalyzer());

    assertFalse(proxy.enableFailoverSetting);
    assertEquals(mockConn, proxy.getConnection());
  }

  @Test
  public void testIfClusterTopologyAvailable() throws SQLException {
    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final String url =
        "jdbc:postgresql:aws://somehost:1234/test?"
            + PGProperty.CLUSTER_INSTANCE_HOST_PATTERN.getName()
            + "=?.somehost";
    final HostSpec hostSpec = urlToHostSpec(url);
    Properties props = Driver.parseURL(url, null);
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url )).thenReturn(mockConn);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(Mockito.mock(HostInfo.class));
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec, props ,url, mockConnectionProvider, mockTopologyService, Mockito.mock(WriterFailoverHandler.class), Mockito.mock(ReaderFailoverHandler.class), new RdsDnsAnalyzer());

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testClusterTopologyNotAvailable() throws SQLException {
    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final String url = "jdbc:postgresql:aws://somehost:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    final List<HostInfo> emptyTopology = new ArrayList<>();
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(emptyTopology);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec, props, url, mockConnectionProvider, mockTopologyService, Mockito.mock(WriterFailoverHandler.class), Mockito.mock(ReaderFailoverHandler.class), new RdsDnsAnalyzer());

    assertTrue(proxy.isConnected());
    assertNull(proxy.currentHost);
    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertFalse(proxy.isClusterTopologyAvailable());
    assertFalse(proxy.isFailoverEnabled());
  }

  @Test
  public void testIfClusterTopologyAvailableAndDnsPatternRequired() throws SQLException {
    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);

    final String url = "jdbc:postgresql:aws://somehost:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(Mockito.mock(HostInfo.class));
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    assertThrows(SQLException.class, () -> new ClusterAwareConnectionProxy(hostSpec, props, url));
  }

  @Test
  public void testRdsCluster() throws SQLException {
    final String url =
        "jdbc:postgresql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final List<HostInfo> mockTopology = new ArrayList<>();
    HostInfo writerHost = new HostInfo("writer-host.XYZ.us-east-2.rds.amazonaws.com", "writer-host", 1234,true);
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler,
            new RdsDnsAnalyzer());

    assertTrue(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    assertTrue(proxy.isConnected());
    assertEquals(writerHost, proxy.currentHost);
    assertFalse(proxy.explicitlyReadOnly);

    verify(mockTopologyService, atLeastOnce())
        .setClusterId("my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234");
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsReaderCluster() throws SQLException {
    final String url =
        "jdbc:postgresql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final List<HostInfo> mockTopology = new ArrayList<>();
    HostInfo writerHost = new HostInfo("writer-host.XYZ.us-east-2.rds.amazonaws.com","writer-host", 1234, true);
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler,
            new RdsDnsAnalyzer());

    assertTrue(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    assertTrue(proxy.isConnected());
    assertNull(proxy.currentHost);
    assertTrue(proxy.explicitlyReadOnly);
    verify(mockTopologyService, atLeastOnce())
        .setClusterId("my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234");
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsCustomCluster() throws SQLException {
    final String url =
        "jdbc:postgresql:aws://my-custom-cluster-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final List<HostInfo> mockTopology = new ArrayList<>();
    HostInfo writerHost = new HostInfo("writer-host.XYZ.us-east-2.rds.amazonaws.com", "writer-host", 1234, true);
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler,
            new RdsDnsAnalyzer());

    assertTrue(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    assertTrue(proxy.isConnected());
    assertNull(proxy.currentHost);
    assertFalse(proxy.explicitlyReadOnly);
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsInstance() throws SQLException {
    final String url = "jdbc:postgresql:aws://my-instance-name.XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final List<HostInfo> mockTopology = new ArrayList<>();
    HostInfo writerHost = new HostInfo(hostSpec.getHost(), "my-instance-name", hostSpec.getPort(), true);
    mockTopology.add(writerHost);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler,
            new RdsDnsAnalyzer());

    assertTrue(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    assertTrue(proxy.isConnected());
    assertEquals(writerHost, proxy.currentHost);
    assertFalse(proxy.explicitlyReadOnly);
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testRdsProxy() throws SQLException {
    final String url = "jdbc:postgresql:aws://test-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(Mockito.mock(HostInfo.class));
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler,
            new RdsDnsAnalyzer());

    assertTrue(proxy.isRds());
    assertTrue(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertFalse(proxy.isFailoverEnabled());
    assertTrue(proxy.isConnected());
    assertNull(proxy.currentHost);
    assertFalse(proxy.explicitlyReadOnly);
    verify(mockTopologyService, atLeastOnce())
        .setClusterId("test-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com:1234");
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testCustomDomainCluster() throws SQLException {
    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final String url =
        "jdbc:postgresql:aws://my-custom-domain.com:1234/test?"
            + PGProperty.CLUSTER_INSTANCE_HOST_PATTERN.getName()
            + "=?.my-custom-domain.com:9999";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = Driver.parseURL(url, null);
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(Mockito.mock(HostInfo.class));
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler,
            new RdsDnsAnalyzer());

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testIpAddressCluster() throws SQLException {
    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final String url =
        "jdbc:postgresql:aws://10.10.10.10:1234/test?"
            + PGProperty.CLUSTER_INSTANCE_HOST_PATTERN.getName()
            + "=?.my-custom-domain.com:9999";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = Driver.parseURL(url,null);
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(Mockito.mock(HostInfo.class));
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler,
            new RdsDnsAnalyzer());

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, never()).setClusterId(any());
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testIpAddressClusterWithClusterId() throws SQLException {
    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final String url =
        "jdbc:postgresql:aws://10.10.10.10:1234/test?"
            + PGProperty.CLUSTER_INSTANCE_HOST_PATTERN.getName()
            + "=?.my-custom-domain.com:9999&"
            + PGProperty.CLUSTER_ID.getName()
            + "=test-cluster-id";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = Driver.parseURL(url, null);
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(Mockito.mock(HostInfo.class));
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler,
            new RdsDnsAnalyzer());

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertTrue(proxy.isClusterTopologyAvailable());
    assertTrue(proxy.isFailoverEnabled());
    verify(mockTopologyService, atLeastOnce()).setClusterId("test-cluster-id");
    verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  }

  @Test
  public void testIpAddressAndTopologyAvailableAndDnsPatternRequired() throws SQLException {
    final String url = "jdbc:postgresql:aws://10.10.10.10:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    final List<HostInfo> mockTopology = new ArrayList<>();
    mockTopology.add(Mockito.mock(HostInfo.class));
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(mockTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    assertThrows(SQLException.class, () ->
        new ClusterAwareConnectionProxy(
            hostSpec, props, url,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler,
            new RdsDnsAnalyzer()));
  }

  @Test
  public void testIpAddressAndTopologyNotAvailable() throws SQLException {
    final String url = "jdbc:postgresql:aws://10.10.10.10:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockConn);

    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    final List<HostInfo> emptyTopology = new ArrayList<>();
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class)))
        .thenReturn(emptyTopology);

    final WriterFailoverHandler writerFailoverHandler = Mockito.mock(WriterFailoverHandler.class);
    final ReaderFailoverHandler readerFailoverHandler = Mockito.mock(ReaderFailoverHandler.class);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec, props, url,
            mockConnectionProvider,
            mockTopologyService,
            writerFailoverHandler,
            readerFailoverHandler,
            new RdsDnsAnalyzer());

    assertFalse(proxy.isRds());
    assertFalse(proxy.isRdsProxy());
    assertFalse(proxy.isClusterTopologyAvailable());
    assertFalse(proxy.isFailoverEnabled());
    assertTrue(proxy.isConnected());
    assertNull(proxy.currentHost);
    assertFalse(proxy.explicitlyReadOnly);
  }

  @Test
  public void testReadOnlyFalseWhenWriterCluster() throws SQLException {
    final String url =
        "jdbc:postgresql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final HostInfo writerHost = new HostInfo("writer-host.XYZ.us-east-2.rds.amazonaws.com", "writer-host", 1234, true);
    final HostInfo readerA_Host = new HostInfo("reader-a-host.XYZ.us-east-2.rds.amazonaws.com","reader-a-host", 1234, false);
    final HostInfo readerB_Host = new HostInfo("reader-b-host.XYZ.us-east-2.rds.amazonaws.com", "reader-b-host", 1234, false);
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerA_Host);
    topology.add(readerB_Host);

    when(mockTopologyService.getCachedTopology()).thenReturn(topology);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class))).thenReturn(topology);
    when(mockConnectionProvider.connect(eq(writerHost.toHostSpec()), any(Properties.class), any(String.class))).thenReturn(mockConn);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class),
            new RdsDnsAnalyzer());

    assertTrue(proxy.isConnected());
    assertEquals(writerHost, proxy.currentHost);
    assertFalse(proxy.explicitlyReadOnly);
  }

  @Test
  public void testReadOnlyTrueWhenReaderCluster() throws SQLException {
    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);
    final String url =
        "jdbc:postgresql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final HostInfo writerHost = new HostInfo("writer-host.XYZ.us-east-2.rds.amazonaws.com", "writer-host", 1234, true);
    final HostInfo readerAHost = new HostInfo("reader-a-host.XYZ.us-east-2.rds.amazonaws.com","reader-a-host", 1234, false);
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerAHost);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(eq(hostSpec), eq(props), eq(url))).thenReturn(mockConn);

    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class))).thenReturn(topology);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class),
            new RdsDnsAnalyzer());

    assertTrue(proxy.isConnected());
    assertNull(proxy.currentHost);
    assertTrue(proxy.explicitlyReadOnly);
  }

  @Test
  public void testLastUsedReaderAvailable() throws SQLException {
    final String url =
        "jdbc:postgresql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final BaseConnection mockConn = Mockito.mock(BaseConnection.class);

    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockConn.getQueryExecutor()).thenReturn(mockQueryExecutor);

    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final HostInfo writerHost = new HostInfo("writer-host.XYZ.us-east-2.rds.amazonaws.com", "writer-host", 1234, true);
    final HostInfo readerA_Host = new HostInfo("reader-a-host.XYZ.us-east-2.rds.amazonaws.com","reader-a-host", 1234, false);
    final HostInfo readerB_Host = new HostInfo("reader-b-host.XYZ.us-east-2.rds.amazonaws.com", "reader-b-host", 1234, false);
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerA_Host);
    topology.add(readerB_Host);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(eq(readerA_Host.toHostSpec()), any(Properties.class), any(String.class))).thenReturn(mockConn);

    when(mockTopologyService.getCachedTopology()).thenReturn(topology);
    when(mockTopologyService.getLastUsedReaderHost()).thenReturn(readerA_Host);
    when(mockTopologyService.getTopology(eq(mockConn), any(Boolean.class))).thenReturn(topology);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class),
            new RdsDnsAnalyzer());

    assertTrue(proxy.isConnected());
    assertEquals(readerA_Host, proxy.currentHost);
    assertTrue(proxy.explicitlyReadOnly);
  }

  @Test
  public void testForWriterReconnectWhenInvalidCachedWriterConnection() throws SQLException {
    final String url =
        "jdbc:postgresql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();

    final BaseConnection mockActualWriterConn = Mockito.mock(BaseConnection.class);
    final BaseConnection mockCachedWriterConn = Mockito.mock(BaseConnection.class);
    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockCachedWriterConn.getQueryExecutor()).thenReturn(mockQueryExecutor);
    when(mockActualWriterConn.getQueryExecutor()).thenReturn(mockQueryExecutor);
    when(mockQueryExecutor.getTransactionState()).thenReturn(TransactionState.IDLE);

    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final HostInfo cachedWriterHost = new HostInfo("cached-writer-host.XYZ.us-east-2.rds.amazonaws.com", "cached-writer-host", 1234, true);
    final HostInfo readerA_Host = new HostInfo("reader-a-host.XYZ.us-east-2.rds.amazonaws.com", "reader-a-host", 1234, false);
    final HostInfo readerB_Host = new HostInfo("reader-b-host.XYZ.us-east-2.rds.amazonaws.com", "reader-b-host", 1234, false);
    final List<HostInfo> cachedTopology = new ArrayList<>();
    cachedTopology.add(cachedWriterHost);
    cachedTopology.add(readerA_Host);
    cachedTopology.add(readerB_Host);

    final HostInfo actualWriterHost = new HostInfo("actual-writer-host.XYZ.us-east-2.rds.amazonaws.com","actual-writer-host", 1234, true);
    // cached writer is now actually a reader
    final HostInfo updatedCachedWriterHost = new HostInfo("cached-writer-host.XYZ.us-east-2.rds.amazonaws.com","cached-writer-host", 1234, false);
    final List<HostInfo> actualTopology = new ArrayList<>();
    actualTopology.add(actualWriterHost);
    actualTopology.add(readerA_Host);
    actualTopology.add(updatedCachedWriterHost);

    when(mockTopologyService.getCachedTopology()).thenReturn(cachedTopology);
    when(mockTopologyService.getTopology(eq(mockCachedWriterConn), any(Boolean.class)))
        .thenReturn(actualTopology);

    ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(eq(cachedWriterHost.toHostSpec()), any(Properties.class), any(String.class))).thenReturn(mockCachedWriterConn);
    when(mockConnectionProvider.connect(eq(actualWriterHost.toHostSpec()), any(Properties.class), any(String.class))).thenReturn(mockActualWriterConn);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class),
            new RdsDnsAnalyzer());

    assertTrue(proxy.isConnected());
    assertEquals(actualWriterHost, proxy.currentHost);
    assertFalse(proxy.explicitlyReadOnly);
  }

  @Test
  public void testConnectToWriterFromReaderOnSetReadOnlyFalse() throws SQLException {
    final BaseConnection mockWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderConnection = Mockito.mock(BaseConnection.class);
    final QueryExecutor mockQueryExecutor = Mockito.mock(QueryExecutor.class);
    when(mockReaderConnection.getQueryExecutor()).thenReturn(mockQueryExecutor);
    when(mockQueryExecutor.getTransactionState()).thenReturn(TransactionState.IDLE);

    final String url =
        "jdbc:postgresql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
    final HostSpec hostSpec = urlToHostSpec(url);
    final Properties props = getPropertiesWithDb();
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);

    final HostInfo writerHost = new HostInfo("writer-host.XYZ.us-east-2.rds.amazonaws.com", "writer-host", 1234, true);
    final HostInfo readerA_Host = new HostInfo("reader-a-host.XYZ.us-east-2.rds.amazonaws.com","reader-a-host", 1234, false);
    final HostInfo readerB_Host = new HostInfo("reader-b-host.XYZ.us-east-2.rds.amazonaws.com", "reader-b-host", 1234, false);
    final List<HostInfo> topology = new ArrayList<>();
    topology.add(writerHost);
    topology.add(readerA_Host);
    topology.add(readerB_Host);

    when(mockTopologyService.getTopology(eq(mockReaderConnection), any(Boolean.class)))
        .thenReturn(topology);

    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnectionProvider.connect(hostSpec, props, url)).thenReturn(mockReaderConnection);
    when(mockConnectionProvider.connect(eq(writerHost.toHostSpec()), any(Properties.class), any(String.class))).thenReturn(mockWriterConnection);

    final ClusterAwareConnectionProxy proxy =
        new ClusterAwareConnectionProxy(
            hostSpec,
            props,
            url,
            mockConnectionProvider,
            mockTopologyService,
            Mockito.mock(WriterFailoverHandler.class),
            Mockito.mock(ReaderFailoverHandler.class),
            new RdsDnsAnalyzer());
    assertTrue(proxy.isConnected());
    // Current host can't be determined when using reader cluster endpoint
    assertNull(proxy.currentHost);
    assertTrue(proxy.explicitlyReadOnly);

    final BaseConnection connectionProxy =
        (BaseConnection)
            java.lang.reflect.Proxy.newProxyInstance(
                BaseConnection.class.getClassLoader(),
                new Class<?>[] {BaseConnection.class},
                proxy);
    connectionProxy.setReadOnly(false);

    assertTrue(proxy.isConnected());
    assertEquals(writerHost, proxy.currentHost);
    assertFalse(proxy.explicitlyReadOnly);
  }

  private HostSpec urlToHostSpec(String url) {
    int hostStart = url.indexOf("//") + 2;
    String urlHostStart = url.substring(hostStart);
    int portSeparatorIndex = urlHostStart.indexOf(":") + hostStart;
    portSeparatorIndex = portSeparatorIndex < hostStart ? -1 : portSeparatorIndex;
    int socketAddressEnd = url.lastIndexOf("/");
    int port = portSeparatorIndex == -1 ? DEFAULT_PORT : Integer.parseInt(url.substring(portSeparatorIndex + 1, socketAddressEnd));
    int hostEndIndex = portSeparatorIndex == -1 ? socketAddressEnd : portSeparatorIndex;
    String host = url.substring(hostStart, hostEndIndex);
    return new HostSpec(host, port);
  }

  private Properties getPropertiesWithDb() {
    Properties props = new Properties();
    props.put("PGDBNAME", TEST_DB);
    return props;
  }

}
