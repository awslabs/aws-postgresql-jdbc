/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.HostSpec;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * ClusterAwareReaderFailoverHandlerTest class.
 * */

public class ClusterAwareReaderFailoverHandlerTest {
  static final int numTestUrls = 6;
  static final String testUrl1 = "jdbc:postgresql:aws://writer-1:1234/";
  static final String testUrl2 = "jdbc:postgresql:aws://reader-1:2345/";
  static final String testUrl3 = "jdbc:postgresql:aws://reader-2:3456/";
  static final String testUrl4 = "jdbc:postgresql:aws://reader-3:4567/";
  static final String testUrl5 = "jdbc:postgresql:aws://reader-4:5678/";
  static final String testUrl6 = "jdbc:postgresql:aws://reader-5:6789/";
  static final int WRITER_CONNECTION_INDEX = 0;

  @Test
  public void testFailover() throws SQLException {
    // original host list: [active writer, active reader, current connection (reader), active
    // reader, down reader, active reader]
    // priority order by index (the subsets will be shuffled): [[1, 3, 5], 0, [2, 4]]
    // connection attempts are made in pairs using the above list
    // expected test result: successful connection for host at index 4
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    final ConnectionProvider mockConnProvider = Mockito.mock(ConnectionProvider.class);
    final BaseConnection mockConnection = Mockito.mock(BaseConnection.class);
    final List<HostInfo> hosts = getHostsFromTestUrls(6);
    final int currentHostIndex = 2;
    final int successHostIndex = 4;
    for (int i = 0; i < hosts.size(); i++) {
      HostInfo host = hosts.get(i);
      assertNotNull(host.getUrl());
      if (i != successHostIndex) {
        when(mockConnProvider.connect(eq(host.toHostSpec()), any(Properties.class), eq(host.getUrl()))).thenThrow(new SQLException());
      } else {
        when(mockConnProvider.connect(eq(host.toHostSpec()), any(Properties.class), eq(host.getUrl()))).thenReturn(mockConnection);
      }
    }

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(hosts.get(hostIndex).getHostPortPair());
    }
    when(mockTopologyService.getDownHosts()).thenReturn(downHosts);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnProvider, new Properties());
    final ReaderFailoverResult result = target.failover(hosts, hosts.get(currentHostIndex));

    assertTrue(result.isConnected());
    assertSame(mockConnection, result.getConnection());
    assertEquals(hosts.get(successHostIndex), result.getHost());

    final HostInfo successHost = hosts.get(successHostIndex);
    verify(mockTopologyService, atLeast(4)).addToDownHostList(any());
    verify(mockTopologyService, never()).addToDownHostList(eq(successHost));
    verify(mockTopologyService, times(1)).removeFromDownHostList(eq(successHost));
  }

  private List<HostInfo> getHostsFromTestUrls(int numHosts) {
    final List<String> urlList =
        Arrays.asList(testUrl1, testUrl2, testUrl3, testUrl4, testUrl5, testUrl6);
    final List<HostInfo> hosts = new ArrayList<>();
    if (numHosts < 0 || numHosts > numTestUrls) {
      numHosts = numTestUrls;
    }

    for (int i = 0; i < numHosts; i++) {
      String url = urlList.get(i);
      int port = Integer.parseInt(url.substring(url.lastIndexOf(":") + 1,url.lastIndexOf("/")));
      String host = url.substring(url.indexOf("//") + 2, url.lastIndexOf("/"));
      hosts.add(new HostInfo(url, host, port, i == 0));
    }
    return hosts;
  }

  @Test
  public void testFailover_timeout() throws SQLException {
    // original host list: [active writer, active reader, current connection (reader), active
    // reader, down reader, active reader]
    // priority order by index (the subsets will be shuffled): [[1, 3, 5], 0, [2, 4]]
    // connection attempts are made in pairs using the above list
    // expected test result: failure to get reader since process is limited to 5s and each attempt to connect takes 20s
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    final ConnectionProvider mockConnProvider = Mockito.mock(ConnectionProvider.class);
    final BaseConnection mockConnection = Mockito.mock(BaseConnection.class);
    final List<HostInfo> hosts = getHostsFromTestUrls(6);
    final int currentHostIndex = 2;
    for (HostInfo host : hosts) {
      assertNotNull(host.getUrl());
      when(mockConnProvider.connect(eq(host.toHostSpec()), any(Properties.class), eq(host.getUrl())))
          .thenAnswer((Answer<BaseConnection>) invocation -> {
            Thread.sleep(20000);
            return mockConnection;
          });
    }

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(hosts.get(hostIndex).getHostPortPair());
    }
    when(mockTopologyService.getDownHosts()).thenReturn(downHosts);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnProvider, new Properties(), 5000, 30000);
    final ReaderFailoverResult result = target.failover(hosts, hosts.get(currentHostIndex));

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());
  }

  @Test
  public void testFailover_emptyHostList() throws SQLException {
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            mockTopologyService, Mockito.mock(ConnectionProvider.class), new Properties());
    final HostInfo currentHost = new HostInfo("writer",  "writer", 1234, true);

    final List<HostInfo> hosts = new ArrayList<>();
    ReaderFailoverResult result = target.failover(hosts, currentHost);
    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());
  }

  @Test
  public void testGetReader_connectionSuccess() throws SQLException {
    // even number of connection attempts
    // first connection attempt to return succeeds, second attempt cancelled
    // expected test result: successful connection for host at index 2
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    when(mockTopologyService.getDownHosts()).thenReturn(new HashSet<>());
    final BaseConnection mockConnection = Mockito.mock(BaseConnection.class);
    final List<HostInfo> hosts = getHostsFromTestUrls(3); // 2 connection attempts (writer not attempted)
    final HostInfo slowHost = hosts.get(1);
    assertNotNull(slowHost.getUrl());
    final HostInfo fastHost = hosts.get(2);
    assertNotNull(fastHost.getUrl());
    final ConnectionProvider mockConnProvider = Mockito.mock(ConnectionProvider.class);
    when(mockConnProvider.connect(eq(slowHost.toHostSpec()), any(Properties.class), eq(slowHost.getUrl())))
        .thenAnswer(
          (Answer<BaseConnection>)
            invocation -> {
              Thread.sleep(20000);
              return mockConnection;
            });
    when(mockConnProvider.connect(eq(fastHost.toHostSpec()), any(Properties.class), eq(fastHost.getUrl()))).thenReturn(mockConnection);

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnProvider, new Properties());
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertTrue(result.isConnected());
    assertSame(mockConnection, result.getConnection());
    assertEquals(fastHost, result.getHost());

    verify(mockTopologyService, never()).addToDownHostList(any());
    verify(mockTopologyService, times(1)).removeFromDownHostList(eq(fastHost));
  }

  @Test
  public void testGetReader_connectionFailure() throws SQLException {
    // odd number of connection attempts
    // first connection attempt to return fails
    // expected test result: failure to get reader
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    when(mockTopologyService.getDownHosts()).thenReturn(new HashSet<>());
    final ConnectionProvider mockConnProvider = Mockito.mock(ConnectionProvider.class);
    final List<HostInfo> hosts = getHostsFromTestUrls(4); // 3 connection attempts (writer not attempted)
    when(mockConnProvider.connect(any(HostSpec.class), any(Properties.class), any(String.class))).thenThrow(new SQLException());

    final int currentHostIndex = 2;

    final ReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockConnProvider, new Properties());
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());

    final HostInfo currentHost = hosts.get(currentHostIndex);
    verify(mockTopologyService, atLeastOnce()).addToDownHostList(eq(currentHost));
    verify(mockTopologyService, never())
        .addToDownHostList(
            eq(hosts.get(WRITER_CONNECTION_INDEX)));
  }

  @Test
  public void testGetReader_connectionAttemptsTimeout() throws SQLException {
    // connection attempts time out before they can succeed
    // first connection attempt to return times out
    // expected test result: failure to get reader
    final TopologyService mockTopologyService = Mockito.mock(TopologyService.class);
    when(mockTopologyService.getDownHosts()).thenReturn(new HashSet<>());
    final ConnectionProvider mockProvider = Mockito.mock(ConnectionProvider.class);
    final BaseConnection mockConnection = Mockito.mock(BaseConnection.class);
    final List<HostInfo> hosts = getHostsFromTestUrls(3); // 2 connection attempts (writer not attempted)
    when(mockProvider.connect(any(HostSpec.class), any(Properties.class), any(String.class)))
        .thenAnswer(
            (Answer<BaseConnection>)
            invocation -> {
                try {
                  Thread.sleep(5000);
                } catch (InterruptedException exception) {
                  // ignore
                }
                return mockConnection;
            });

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(mockTopologyService, mockProvider, new Properties(), 60000, 1000);
    final ReaderFailoverResult result = target.getReaderConnection(hosts);

    assertFalse(result.isConnected());
    assertNull(result.getConnection());
    assertNull(result.getHost());

    verify(mockTopologyService, never()).addToDownHostList(any());
  }

  @Test
  public void testGetHostsByPriority() {
    final List<HostInfo> originalHosts = getHostsFromTestUrls(6);

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4, 5);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(originalHosts.get(hostIndex).getHostPortPair());
    }

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            Mockito.mock(TopologyService.class), Mockito.mock(ConnectionProvider.class), new Properties());
    final List<HostInfo> hostsByPriority =
        target.getHostsByPriority(originalHosts, downHosts);

    assertPriorityOrdering(hostsByPriority, downHosts,true);
    assertEquals(6, hostsByPriority.size());
  }

  @Test
  public void testGetReaderHostsByPriority() {
    final List<HostInfo> originalHosts = getHostsFromTestUrls(6);

    final Set<String> downHosts = new HashSet<>();
    final List<Integer> downHostIndexes = Arrays.asList(2, 4, 5);
    for (int hostIndex : downHostIndexes) {
      downHosts.add(originalHosts.get(hostIndex).getHostPortPair());
    }

    final ClusterAwareReaderFailoverHandler target =
        new ClusterAwareReaderFailoverHandler(
            Mockito.mock(TopologyService.class), Mockito.mock(ConnectionProvider.class), new Properties());
    final List<HostInfo> readerHosts =
        target.getReaderHostsByPriority(originalHosts, downHosts);

    // assert the following priority ordering: active readers, down readers
    assertPriorityOrdering(readerHosts, downHosts,false);
    assertEquals(5, readerHosts.size());
  }

  private void assertPriorityOrdering(List<HostInfo> hostsByPriority, Set<String> downHosts, boolean shouldContainWriter) {
    // assert the following priority ordering: active readers, writer, down readers
    int i;
    int numActiveReaders = 2;
    for (i = 0; i < numActiveReaders; i++) {
      HostInfo activeReader = hostsByPriority.get(i);
      assertFalse(activeReader.isWriter());
      assertFalse(downHosts.contains(activeReader.getHostPortPair()));
    }
    if (shouldContainWriter) {
      HostInfo writerHost = hostsByPriority.get(i);
      assertTrue(writerHost.isWriter());
      i++;
    }
    for (; i < hostsByPriority.size(); i++) {
      HostInfo downReader = hostsByPriority.get(i);
      assertFalse(downReader.isWriter());
      assertTrue(downHosts.contains(downReader.getHostPortPair()));
    }
  }
}
