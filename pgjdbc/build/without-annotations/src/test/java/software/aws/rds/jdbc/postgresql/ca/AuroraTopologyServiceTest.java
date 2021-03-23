package software.aws.rds.jdbc.postgresql.ca;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/** AuroraTopologyServiceTest class. */
public class AuroraTopologyServiceTest {

  private final AuroraTopologyService spyProvider = Mockito.spy(new AuroraTopologyService());
  private final int defaultPort = 5432;
  private final String clusterInstanceHostPattern = "?.XYZ.us-east-2.rds.amazonaws.com";

  @BeforeEach
  void resetProvider() {
    spyProvider.setClusterId(UUID.randomUUID().toString());
    spyProvider.setRefreshRate(AuroraTopologyService.DEFAULT_REFRESH_RATE_IN_MILLISECONDS);
    spyProvider.clearAll();
    AuroraTopologyService.setExpireTime(AuroraTopologyService.DEFAULT_CACHE_EXPIRE_MS);
  }

  @Test
  public void testTopologyQuery() throws SQLException {
    final Connection mockConn = Mockito.mock(Connection.class);
    stubTopologyQuery(mockConn);

    final HostInfo clusterHostInfo = new HostInfo(null, null, false, HostInfo.initProps(clusterInstanceHostPattern, defaultPort, null));
    spyProvider.setClusterInstanceTemplate(clusterHostInfo);

    final List<HostInfo> topology = spyProvider.getTopology(mockConn, false);
    assertNotNull(topology);

    final HostInfo master = topology.get(AuroraTopologyService.WRITER_CONNECTION_INDEX);
    final List<HostInfo> slaves =
        topology.subList(AuroraTopologyService.WRITER_CONNECTION_INDEX + 1, topology.size());

    assertEquals("writer-instance.XYZ.us-east-2.rds.amazonaws.com", master.getHost());
    assertEquals(defaultPort, master.getPort());
    assertEquals("writer-instance", master.getInstanceIdentifier());
    assertTrue(master.isWriter());

    assertEquals(3, topology.size());
    assertEquals(2, slaves.size());
  }

  @Test
  public void testTopologyQuery_MultiWriter() throws SQLException {
    final Connection mockConn = Mockito.mock(Connection.class);
    stubTopologyQueryMultiWriter(mockConn);

    final HostInfo clusterHostInfo = new HostInfo(null, null, false, HostInfo.initProps(clusterInstanceHostPattern, defaultPort, null));
    spyProvider.setClusterInstanceTemplate(clusterHostInfo);

    final List<HostInfo> topology = spyProvider.getTopology(mockConn, false);
    assertNotNull(topology);
    final List<HostInfo> readers =
        topology.subList(AuroraTopologyService.WRITER_CONNECTION_INDEX + 1, topology.size());

    assertEquals(3, topology.size());
    assertEquals(2, readers.size());

    final HostInfo master1 = topology.get(AuroraTopologyService.WRITER_CONNECTION_INDEX);
    final HostInfo master2 = topology.get(2);
    final HostInfo reader = topology.get(1);

    assertEquals("writer-instance-1.XYZ.us-east-2.rds.amazonaws.com", master1.getHost());
    assertEquals(defaultPort, master1.getPort());
    assertEquals("writer-instance-1", master1.getInstanceIdentifier());
    assertTrue(master1.isWriter());

    assertEquals("writer-instance-2.XYZ.us-east-2.rds.amazonaws.com", master2.getHost());
    assertEquals(defaultPort, master2.getPort());
    assertEquals("writer-instance-2", master2.getInstanceIdentifier());
    // A second writer indicates the topology is in a failover state, and the second writer is
    // the obsolete one. It will be come a reader shortly, so we mark it as such
    assertFalse(master2.isWriter());

    assertEquals("reader-instance.XYZ.us-east-2.rds.amazonaws.com", reader.getHost());
    assertEquals(defaultPort, reader.getPort());
    assertEquals("reader-instance", reader.getInstanceIdentifier());
    assertFalse(reader.isWriter());
  }

  private void stubTopologyQuery(Connection conn)
      throws SQLException {
    ResultSet mockResults = Mockito.mock(ResultSet.class);
    stubTopologyQueryExecution(conn, mockResults);
    stubTopologyResponseData(mockResults);
  }

  private void stubTopologyQueryMultiWriter(Connection conn)
      throws SQLException {
    ResultSet mockResults = Mockito.mock(ResultSet.class);
    stubTopologyQueryExecution(conn, mockResults);
    stubTopologyResponseDataMultiWriter(mockResults);
  }

  private void stubTopologyQueryExecution(Connection conn, ResultSet results)
      throws SQLException {
    Statement mockStatement = Mockito.mock(Statement.class);
    when(conn.createStatement()).thenReturn(mockStatement);
    when(mockStatement.executeQuery(AuroraTopologyService.RETRIEVE_TOPOLOGY_SQL)).thenReturn(results);
  }

  private void stubTopologyResponseData(ResultSet results) throws SQLException {
    when(results.next()).thenReturn(true, true, true, false);
    // results.getString(AuroraTopologyService.SESSION_ID_COL) is called twice for each instance and should return the same result both times
    when(results.getString(AuroraTopologyService.SESSION_ID_COL))
        .thenReturn(
            "Replica",
            "Replica",
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            "Replica",
            "Replica");
    when(results.getString(AuroraTopologyService.SERVER_ID_COL))
        .thenReturn(
            "replica-instance-1",
            "writer-instance",
            "replica-instance-2");
  }

  private void stubTopologyResponseDataMultiWriter(ResultSet results) throws SQLException {
    when(results.next()).thenReturn(true, true, true, false);
    // results.getString(AuroraTopologyService.SESSION_ID_COL) is called twice for each instance and should return the same result both times
    when(results.getString(AuroraTopologyService.SESSION_ID_COL))
        .thenReturn(
            "Replica",
            "Replica",
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID,
            AuroraTopologyService.WRITER_SESSION_ID);
    when(results.getString(AuroraTopologyService.SERVER_ID_COL))
        .thenReturn(
            "reader-instance",
            "writer-instance-1",
            "writer-instance-2");
  }

  @Test
  public void testCachedEntryRetrieved() throws SQLException {
    final Connection mockConn = Mockito.mock(Connection.class);
    stubTopologyQueryMultiWriter(mockConn);
    final HostInfo clusterHostInfo = new HostInfo(null, null, false, HostInfo.initProps(clusterInstanceHostPattern, defaultPort, null));
    spyProvider.setClusterInstanceTemplate(clusterHostInfo);

    spyProvider.getTopology(mockConn, false);
    spyProvider.getTopology(mockConn, false);

    verify(spyProvider, times(1)).queryForTopology(mockConn);
  }

  @Test
  public void testForceUpdateQueryFailure() throws SQLException {
    final Connection mockConn = Mockito.mock(Connection.class);
    final HostInfo clusterHostInfo = new HostInfo(null, null, false, HostInfo.initProps(clusterInstanceHostPattern, defaultPort, null));
    spyProvider.setClusterInstanceTemplate(clusterHostInfo);
    when(mockConn.createStatement()).thenThrow(SQLException.class);

    final List<HostInfo> hosts = spyProvider.getTopology(mockConn, true);
    assertNull(hosts);
  }

  @Test
  public void testQueryFailureReturnsStaleTopology() throws SQLException, InterruptedException {
    final Connection mockConn = Mockito.mock(Connection.class);
    stubTopologyQuery(mockConn);
    final HostInfo clusterHostInfo = new HostInfo(null, null, false, HostInfo.initProps(clusterInstanceHostPattern, defaultPort, null));
    spyProvider.setClusterInstanceTemplate(clusterHostInfo);
    spyProvider.setRefreshRate(1);

    final List<HostInfo> hosts = spyProvider.getTopology(mockConn, false);
    when(mockConn.createStatement()).thenThrow(SQLException.class);
    Thread.sleep(5);
    final List<HostInfo> staleHosts = spyProvider.getTopology(mockConn, false);
    assertNotNull(staleHosts);

    verify(spyProvider, times(2)).queryForTopology(mockConn);
    assertEquals(3, staleHosts.size());
    assertEquals(hosts, staleHosts);
  }

  @Test
  public void testProviderRefreshesTopology() throws SQLException, InterruptedException {
    final Connection mockConn = Mockito.mock(Connection.class);
    stubTopologyQueryMultiWriter(mockConn);

    final HostInfo clusterHostInfo = new HostInfo(null, null, false, HostInfo.initProps(clusterInstanceHostPattern, defaultPort, null));
    spyProvider.setClusterInstanceTemplate(clusterHostInfo);

    spyProvider.setRefreshRate(1);
    spyProvider.getTopology(mockConn, false);
    Thread.sleep(2);
    spyProvider.getTopology(mockConn, false);

    verify(spyProvider, times(2)).queryForTopology(mockConn);
  }

  @Test
  public void testProviderTopologyExpires() throws SQLException, InterruptedException {
    final Connection mockConn = Mockito.mock(Connection.class);
    stubTopologyQueryMultiWriter(mockConn);

    final HostInfo clusterHostInfo = new HostInfo(null, null, false, HostInfo.initProps(clusterInstanceHostPattern, defaultPort, null));
    spyProvider.setClusterInstanceTemplate(clusterHostInfo);

    AuroraTopologyService.setExpireTime(1000); // 1 sec
    spyProvider.setRefreshRate(
        10000); // 10 sec; and cache expiration time is also (indirectly) changed to 10 sec

    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(1)).queryForTopology(mockConn);

    Thread.sleep(3000);

    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(1)).queryForTopology(mockConn);

    Thread.sleep(3000);
    // internal cache has NOT expired yet
    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(1)).queryForTopology(mockConn);

    Thread.sleep(5000);
    // internal cache has expired by now
    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(2)).queryForTopology(mockConn);
  }

  @Test
  public void testProviderTopologyNotExpired() throws SQLException, InterruptedException {
    final Connection mockConn = Mockito.mock(Connection.class);
    stubTopologyQueryMultiWriter(mockConn);

    final HostInfo clusterHostInfo = new HostInfo(null, null, false, HostInfo.initProps(clusterInstanceHostPattern, defaultPort, null));
    spyProvider.setClusterInstanceTemplate(clusterHostInfo);

    AuroraTopologyService.setExpireTime(10000); // 10 sec
    spyProvider.setRefreshRate(1000); // 1 sec

    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(1)).queryForTopology(mockConn);

    Thread.sleep(2000);

    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(2)).queryForTopology(mockConn);

    Thread.sleep(2000);

    spyProvider.getTopology(mockConn, false);
    verify(spyProvider, times(3)).queryForTopology(mockConn);
  }

  @Test
  public void testClearProviderCache() throws SQLException {
    final Connection mockConn = Mockito.mock(Connection.class);
    stubTopologyQueryMultiWriter(mockConn);

    final HostInfo clusterHostInfo = new HostInfo(null, null, false, HostInfo.initProps(clusterInstanceHostPattern, defaultPort, null));
    spyProvider.setClusterInstanceTemplate(clusterHostInfo);

    spyProvider.getTopology(mockConn, false);
    spyProvider.addToDownHostList(clusterHostInfo);
    assertEquals(1, AuroraTopologyService.topologyCache.size());

    spyProvider.clearAll();
    assertEquals(0, AuroraTopologyService.topologyCache.size());
  }
}
