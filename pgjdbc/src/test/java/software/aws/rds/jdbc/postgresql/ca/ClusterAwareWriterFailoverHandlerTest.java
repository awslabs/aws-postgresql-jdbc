/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.postgresql.core.BaseConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * ClusterAwareWriterFailoverHandlerTest class.
 */
public class ClusterAwareWriterFailoverHandlerTest {

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes taskA: successfully re-connect to writer; return new connection taskB:
   * fail to connect to any reader due to exception expected test result: new connection by taskA
   */
  @Test
  public void testReconnectToWriter_taskBReaderException() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final BaseConnection mockConnection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", true);
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", false);
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", false);
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(eq(writerHost.toHostSpec()), any(Properties.class), eq(writerHost.getUrl()))).thenReturn(mockConnection);
    when(mockConnectionProvider.connect(eq(readerA_Host.toHostSpec()), any(Properties.class), eq(readerA_Host.getUrl()))).thenThrow(SQLException.class);
    when(mockConnectionProvider.connect(eq(readerB_Host.toHostSpec()), any(Properties.class), eq(readerB_Host.getUrl()))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(any(BaseConnection.class), eq(true)))
        .thenReturn(currentTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenThrow(SQLException.class);

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailover,
            5000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockConnection);

    final InOrder inOrder = Mockito.inOrder(mockTopologyService);
    inOrder.verify(mockTopologyService).addToDownHostList(eq(writerHost));
    inOrder.verify(mockTopologyService).removeFromDownHostList(eq(writerHost));
  }

  private HostInfo createBasicHostInfo(String host, boolean isWriter) {
    return new HostInfo(host, host,1234, isWriter);
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes seen by task A, changes to [new-writer, reader-A, reader-B] for taskB
   * taskA: successfully re-connect to initial writer; return new connection taskB: successfully
   * connect to readerA and then new writer but it takes more time than taskA expected test result:
   * new connection by taskA
   */
  @Test
  public void testReconnectToWriter_SlowReaderA() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final BaseConnection mockWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderA_Connection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", true);
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", false);
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", false);
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = createBasicHostInfo("new-writer-host", true);
    final List<HostInfo> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(eq(writerHost.toHostSpec()), any(Properties.class), eq(writerHost.getUrl()))).thenReturn(mockWriterConnection);
    when(mockConnectionProvider.connect(eq(readerB_Host.toHostSpec()), any(Properties.class), eq(readerB_Host.getUrl()))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(eq(mockWriterConnection), eq(true)))
        .thenReturn(currentTopology);
    when(mockTopologyService.getTopology(eq(mockReaderA_Connection), eq(true)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenAnswer(
            (Answer<ReaderFailoverResult>)
            invocation -> {
              Thread.sleep(5000);
                return new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true);
            });

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailover,
            60000,
            5000,
            5000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockWriterConnection);

    final InOrder inOrder = Mockito.inOrder(mockTopologyService);
    inOrder.verify(mockTopologyService).addToDownHostList(eq(writerHost));
    inOrder.verify(mockTopologyService).removeFromDownHostList(eq(writerHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a current writer node.
   *
   * <p>topology: no changes taskA: successfully re-connect to writer; return new connection taskB:
   * successfully connect to readerA and retrieve topology, but latest writer is not new (defer to
   * taskA) expected test result: new connection by taskA
   */
  @Test
  public void testReconnectToWriter_taskBDefers() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final BaseConnection mockWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderA_Connection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", true);
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", false);
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", false);
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(eq(writerHost.toHostSpec()), any(Properties.class), eq(writerHost.getUrl())))
        .thenAnswer(
          (Answer<Connection>)
            invocation -> {
              Thread.sleep(5000);
              return mockWriterConnection;
            });
    when(mockConnectionProvider.connect(eq(readerB_Host.toHostSpec()), any(Properties.class), eq(readerB_Host.getUrl()))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(any(BaseConnection.class), eq(true)))
        .thenReturn(currentTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailover,
            60000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertFalse(result.isNewHost());
    assertSame(result.getNewConnection(), mockWriterConnection);

    final InOrder inOrder = Mockito.inOrder(mockTopologyService);
    inOrder.verify(mockTopologyService).addToDownHostList(eq(writerHost));
    inOrder.verify(mockTopologyService).removeFromDownHostList(eq(writerHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a new writer node.
   *
   * <p>topology: changes to [new-writer, reader-A, reader-B] for taskB, taskA sees no changes
   * taskA: successfully re-connect to writer; return connection to initial writer but it takes more
   * time than taskB taskB: successfully connect to readerA and then to new-writer expected test
   * result: new connection to writer by taskB
   */
  @Test
  public void testConnectToReaderA_SlowWriter() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final BaseConnection mockWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockNewWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderA_Connection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderB_Connection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", true);
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", false);
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", false);
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = createBasicHostInfo("new-writer-host", true);
    final List<HostInfo> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(writerHost.toHostSpec(), new Properties(), writerHost.getUrl()))
        .thenAnswer(
          (Answer<Connection>)
            invocation -> {
              Thread.sleep(5000);
              return mockWriterConnection;
            });
    when(mockConnectionProvider.connect(eq(readerA_Host.toHostSpec()), any(Properties.class),
        eq(readerB_Host.getUrl()))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(eq(readerB_Host.toHostSpec()), any(Properties.class),
        eq(readerB_Host.getUrl()))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(eq(newWriterHost.toHostSpec()), any(Properties.class),
        eq(newWriterHost.getUrl()))).thenReturn(mockNewWriterConnection);

    when(mockTopologyService.getTopology(eq(mockWriterConnection), eq(true)))
        .thenReturn(currentTopology);
    when(mockTopologyService.getTopology(eq(mockReaderA_Connection), eq(true)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailover,
            60000,
            5000,
            5000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertTrue(result.isNewHost());
    assertSame(result.getNewConnection(), mockNewWriterConnection);
    assertEquals(3, result.getTopology().size());
    assertEquals("new-writer-host", result.getTopology().get(0).getHost());

    verify(mockTopologyService, times(1)).addToDownHostList(eq(writerHost));
    verify(mockTopologyService, times(1)).removeFromDownHostList(eq(newWriterHost));
  }

  /**
   * Verify that writer failover handler can re-connect to a new writer node.
   *
   * <p>topology: changes to [new-writer, initial-writer, reader-A, reader-B] taskA: successfully
   * reconnect, but initial-writer is now a reader (defer to taskB) taskB: successfully connect to
   * readerA and then to new-writer expected test result: new connection to writer by taskB
   */
  @Test
  public void testConnectToReaderA_taskADefers() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final BaseConnection mockNewWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderA_Connection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderB_Connection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo initialWriterHost = createBasicHostInfo("initial-writer-host", true);
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", false);
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", false);
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(initialWriterHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = createBasicHostInfo("new-writer-host", true);
    final List<HostInfo> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(initialWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(eq(initialWriterHost.toHostSpec()), any(Properties.class), eq(initialWriterHost.getUrl())))
        .thenReturn(Mockito.mock(BaseConnection.class));
    when(mockConnectionProvider.connect(eq(readerA_Host.toHostSpec()), any(Properties.class), eq(readerA_Host.getUrl()))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(eq(readerB_Host.toHostSpec()), any(Properties.class), eq(readerB_Host.getUrl()))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(eq(newWriterHost.toHostSpec()), any(Properties.class),
        eq(newWriterHost.getUrl())))
        .thenAnswer(
            (Answer<Connection>)
            invocation -> {
                Thread.sleep(5000);
                return mockNewWriterConnection;
            });

    when(mockTopologyService.getTopology(any(BaseConnection.class), eq(true)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailover,
            60000,
            5000,
            5000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertTrue(result.isConnected());
    assertTrue(result.isNewHost());
    assertSame(result.getNewConnection(), mockNewWriterConnection);
    assertEquals(4, result.getTopology().size());
    assertEquals("new-writer-host", result.getTopology().get(0).getHost());

    verify(mockTopologyService, times(1)).addToDownHostList(eq(initialWriterHost));
    verify(mockTopologyService, times(1)).removeFromDownHostList(eq(newWriterHost));
  }

  /**
   * Verify that writer failover handler fails to re-connect to any writer node.
   *
   * <p>topology: no changes seen by task A, changes to [new-writer, reader-A, reader-B] for taskB
   * taskA: fail to re-connect to writer due to failover timeout taskB: successfully connect to
   * readerA and then fail to connect to writer due to failover timeout expected test result: no
   * connection
   */
  @Test
  public void testFailedToConnect_failoverTimeout() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final BaseConnection mockWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockNewWriterConnection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderA_Connection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderB_Connection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", true);
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", false);
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", false);
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = createBasicHostInfo("new-writer-host", true);
    final List<HostInfo> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(writerHost.toHostSpec(), new Properties(), writerHost.getUrl()))
        .thenAnswer(
            (Answer<Connection>)
            invocation -> {
                Thread.sleep(30000);
                return mockWriterConnection;
          });
    when(mockConnectionProvider.connect(readerA_Host.toHostSpec(), new Properties(),
        readerA_Host.getUrl())).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(readerB_Host.toHostSpec(), new Properties(),
        readerB_Host.getUrl())).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(newWriterHost.toHostSpec(), new Properties(),
        newWriterHost.getUrl()))
        .thenAnswer(
            (Answer<Connection>)
            invocation -> {
                Thread.sleep(30000);
                return mockNewWriterConnection;
            });

    when(mockTopologyService.getTopology(eq(mockWriterConnection), any(Boolean.class)))
        .thenReturn(currentTopology);
    when(mockTopologyService.getTopology(eq(mockNewWriterConnection), any(Boolean.class)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailover,
            5000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockTopologyService, times(1)).addToDownHostList(eq(writerHost));
  }

  /**
   * Verify that writer failover handler fails to re-connect to any writer node.
   *
   * <p>topology: changes to [new-writer, reader-A, reader-B] for taskB taskA: fail to re-connect
   * to
   * writer due to exception taskB: successfully connect to readerA and then fail to connect to
   * writer due to exception expected test result: no connection
   */
  @Test
  public void testFailedToConnect_taskAException_taskBWriterException() throws SQLException {
    final AuroraTopologyService mockTopologyService = Mockito.mock(AuroraTopologyService.class);
    final ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
    final BaseConnection mockReaderA_Connection = Mockito.mock(BaseConnection.class);
    final BaseConnection mockReaderB_Connection = Mockito.mock(BaseConnection.class);
    final ReaderFailoverHandler mockReaderFailover = Mockito.mock(ReaderFailoverHandler.class);

    final HostInfo writerHost = createBasicHostInfo("writer-host", true);
    final HostInfo readerA_Host = createBasicHostInfo("reader-a-host", false);
    final HostInfo readerB_Host = createBasicHostInfo("reader-b-host", false);
    final List<HostInfo> currentTopology = new ArrayList<>();
    currentTopology.add(writerHost);
    currentTopology.add(readerA_Host);
    currentTopology.add(readerB_Host);

    final HostInfo newWriterHost = createBasicHostInfo("new-writer-host", true);
    final List<HostInfo> newTopology = new ArrayList<>();
    newTopology.add(newWriterHost);
    newTopology.add(readerA_Host);
    newTopology.add(readerB_Host);

    when(mockConnectionProvider.connect(eq(writerHost.toHostSpec()), any(Properties.class), eq(writerHost.getUrl()))).thenThrow(SQLException.class);
    when(mockConnectionProvider.connect(eq(readerA_Host.toHostSpec()), any(Properties.class),
        eq(readerA_Host.getUrl()))).thenReturn(mockReaderA_Connection);
    when(mockConnectionProvider.connect(eq(readerB_Host.toHostSpec()), any(Properties.class),
        eq(readerB_Host.getUrl()))).thenReturn(mockReaderB_Connection);
    when(mockConnectionProvider.connect(eq(newWriterHost.toHostSpec()), any(Properties.class),
        eq(newWriterHost.getUrl()))).thenThrow(SQLException.class);

    when(mockTopologyService.getTopology(any(BaseConnection.class), any(Boolean.class)))
        .thenReturn(newTopology);

    when(mockReaderFailover.getReaderConnection(ArgumentMatchers.anyList()))
        .thenReturn(new ReaderFailoverResult(mockReaderA_Connection, readerA_Host, true));

    final ClusterAwareWriterFailoverHandler target =
        new ClusterAwareWriterFailoverHandler(
            mockTopologyService,
            mockConnectionProvider,
            new Properties(),
            mockReaderFailover,
            5000,
            2000,
            2000);
    final WriterFailoverResult result = target.failover(currentTopology);

    assertFalse(result.isConnected());
    assertFalse(result.isNewHost());

    verify(mockTopologyService, times(1)).addToDownHostList(eq(writerHost));
    verify(mockTopologyService, atLeastOnce()).addToDownHostList(eq(newWriterHost));
  }
}
