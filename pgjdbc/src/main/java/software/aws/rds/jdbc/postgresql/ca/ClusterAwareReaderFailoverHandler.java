/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of ReaderFailoverHandler.
 *
 * <p>Reader Failover Process goal is to connect to any available reader. In order to connect
 * faster, this implementation tries to connect to two readers at the same time. The first
 * successfully connected reader is returned as the process result. If both readers are unavailable
 * (i.e. could not be connected to), the process picks up another pair of readers and repeat. If no
 * reader has been connected to, the process may consider a writer host, and other hosts marked
 * down, to connect to.
 */

public class ClusterAwareReaderFailoverHandler implements ReaderFailoverHandler {
  protected static final int DEFAULT_FAILOVER_TIMEOUT = 60000; // 60 sec
  protected static final int DEFAULT_READER_CONNECT_TIMEOUT = 30000; // 30 sec
  protected static final int WRITER_CONNECTION_INDEX = 0;
  private static final transient Logger LOGGER = Logger.getLogger(ClusterAwareReaderFailoverHandler.class.getName());

  protected int maxFailoverTimeoutMs;
  protected int timeoutMs;
  protected final ConnectionProvider connProvider;
  protected final TopologyService topologyService;
  protected Properties connectionProps;

  public ClusterAwareReaderFailoverHandler(
      TopologyService topologyService, ConnectionProvider connProvider, Properties connectionProps) {
    this(topologyService, connProvider, connectionProps, DEFAULT_FAILOVER_TIMEOUT, DEFAULT_READER_CONNECT_TIMEOUT);
  }

  /**
   * ClusterAwareReaderFailoverHandler constructor.
   * @param topologyService a topology service
   * @param connProvider a connection provider
   * @param connectionProps a set of properties for a new connections
   * @param failoverTimeoutMs timeout for the entire reader failover process
   * @param timeoutMs timeout for each individual connection attempt
   *
   * */

  public ClusterAwareReaderFailoverHandler(
      TopologyService topologyService, ConnectionProvider connProvider, Properties connectionProps, int failoverTimeoutMs, int timeoutMs) {
    this.topologyService = topologyService;
    this.connProvider = connProvider;
    this.connectionProps = connectionProps;
    this.maxFailoverTimeoutMs = failoverTimeoutMs;
    this.timeoutMs = timeoutMs;
  }

  /**
   * Set process timeout in millis. Entire process of connecting to a reader will be limited by this
   * time duration.
   *
   * @param timeoutMs Process timeout in millis
   */
  protected void setTimeoutMs(int timeoutMs) {
    this.timeoutMs = timeoutMs;
  }

  /**
   * Called to start Reader Failover Process. This process tries to connect to any reader. If no
   * reader is available then driver may also try to connect to a writer host, down hosts, and the
   * current reader host.
   *
   * @param hosts       Cluster current topology.
   * @param currentHost The currently connected host that has failed.
   *
   * @return {@link ReaderFailoverResult} The results of this process. May return null, which is
   *     considered an unsuccessful result.
   * @throws SQLException when a thread gets interrupted and failover fails
   */
  @Override
  public ReaderFailoverResult failover(List<HostInfo> hosts, @Nullable HostInfo currentHost)
      throws SQLException {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<ReaderFailoverResult> future = executor.submit(() -> {
      ReaderFailoverResult result = null;
      while (result == null || !result.isConnected()) {
        result = failoverInternal(hosts, currentHost);
        TimeUnit.SECONDS.sleep(1);
      }
      return result;
    });
    executor.shutdown();

    ReaderFailoverResult defaultResult = new ReaderFailoverResult(null, null, false);

    try {
      return future.get(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException("Thread was interrupted.", "70100", e);

    } catch (ExecutionException e) {
      return defaultResult;

    } catch (TimeoutException e) {
      future.cancel(true);
      return defaultResult;

    } finally {
      if (!executor.isTerminated()) {
        executor.shutdownNow(); // terminate all remaining tasks
      }
    }
  }

  /**
   * Helper method for {@link #failover(List, HostInfo)}.
   *
   * @param hosts List of hosts in the current topology
   * @param currentHost Currently connected host
   * @return {@link ReaderFailoverResult}. The result
   * @throws SQLException when a thread gets interrupted and failover fails
   */
  protected ReaderFailoverResult failoverInternal(List<HostInfo> hosts, @Nullable HostInfo currentHost)
      throws SQLException {
    this.topologyService.addToDownHostList(currentHost);
    if (hosts.isEmpty()) {
      return new ReaderFailoverResult(null, null, false);
    }
    Set<String> downHosts = topologyService.getDownHosts();
    List<HostInfo> hostsByPriority = getHostsByPriority(hosts, downHosts);
    return getConnectionFromHostList(hostsByPriority);
  }

  /**
   * Returns a list of hosts in order of priority. First by the active reader, then the writer, then
   * those that were original marked as down
   *
   * @param hosts List of hosts in topology
   * @param downHosts List of hosts that are currently marked as "down"
   * @return List of {@link HostInfo} in order of priority for connection attempts
   */
  List<HostInfo> getHostsByPriority(List<HostInfo> hosts, Set<String> downHosts) {
    List<HostInfo> hostsByPriority = new ArrayList<>();
    addActiveReaders(hostsByPriority, hosts, downHosts);
    HostInfo writerHost = hosts.get(WRITER_CONNECTION_INDEX);
    if (writerHost != null) {
      hostsByPriority.add(writerHost);
    }
    addDownHosts(hostsByPriority, hosts, downHosts);
    return hostsByPriority;
  }

  /**
   * Method to add the set of active reader hosts in a list
   *
   * @param list The list of hosts to add the active hosts to
   * @param hosts Full list of all the hosts in the topology
   * @param downHosts List of hosts that are marked as down
   */
  private void addActiveReaders(List<HostInfo> list, List<HostInfo> hosts, Set<String> downHosts) {
    List<HostInfo> activeReaders = new ArrayList<>();
    if (hosts != null) {
      for (int i = WRITER_CONNECTION_INDEX + 1; i < hosts.size(); i++) {
        HostInfo host = hosts.get(i);
        if (!downHosts.contains(host.getHostPortPair())) {
          activeReaders.add(host);
        }
      }
      Collections.shuffle(activeReaders);
      list.addAll(activeReaders);
    }
  }

  /**
   * Method to add the set of downed hosts in a list
   *
   * @param list The list of hosts to add the downed hosts to
   * @param hosts Full list of all the hosts in the topology
   * @param downHosts List of hosts that are marked as down
   */
  private void addDownHosts(List<HostInfo> list, List<HostInfo> hosts, Set<String> downHosts) {
    List<HostInfo> downHostList = new ArrayList<>();
    for (HostInfo host : hosts) {
      if (host != null && downHosts.contains(host.getHostPortPair())) {
        downHostList.add(host);
      }
    }
    Collections.shuffle(downHostList);
    list.addAll(downHostList);
  }

  /**
   * Called to get any available reader connection. If no reader is available then result of process
   * is unsuccessful. This process will not attempt to connect to the writer host.
   *
   * @param hostList Cluster current topology.
   *
   * @return {@link ReaderFailoverResult} The results of this process. May return null, which is
   *     considered an unsuccessful result.
   * @throws SQLException when a thread gets interrupted and failover fails
   */
  @Override
  public ReaderFailoverResult getReaderConnection(List<HostInfo> hostList) throws SQLException {
    Set<String> downHosts = topologyService.getDownHosts();
    List<HostInfo> readerHosts = getReaderHostsByPriority(hostList, downHosts);
    return getConnectionFromHostList(readerHosts);
  }

  /**
   * Returns a list of reader hostshosts in order of priority. First by the active reader, then the writer, then
   * those that were original marked as down
   *
   * @param hostList List of hosts in topology
   * @param downHosts List of hosts that are currently marked as "down"
   * @return List of {@link HostInfo} in order of priority for connection attempts
   */
  List<HostInfo> getReaderHostsByPriority(List<HostInfo> hostList, Set<String> downHosts) {
    List<HostInfo> readerHosts = new ArrayList<>();
    addActiveReaders(readerHosts, hostList, downHosts);
    addDownReaders(readerHosts, hostList, downHosts);
    return readerHosts;
  }

  /**
   * Adds reader hosts that are marked as "down" to a list of HostInfo
   *
   * @param list The list to add the downed reader hosts to
   * @param hosts The full list of all the hosts in the topology
   * @param downHosts Set of downed hosts
   */
  private void addDownReaders(List<HostInfo> list, List<HostInfo> hosts, Set<String> downHosts) {
    List<HostInfo> downReaders = new ArrayList<>();
    if (hosts != null) {
      for (int i = WRITER_CONNECTION_INDEX + 1; i < hosts.size(); i++) {
        HostInfo host = hosts.get(i);
        if (downHosts.contains(host.getHostPortPair())) {
          downReaders.add(host);
        }
      }
      Collections.shuffle(downReaders);
      list.addAll(downReaders);
    }
  }

  /**
   * Attempts to reconnect to a host (two at a time, concurrently) and attempts to return the results
   * of the first successful connection.
   *
   * @param hosts The topology of hosts
   * @return {@link ReaderFailoverResult} instance which contains the results from failover
   * @throws SQLException when a thread gets interrupted and failover fails
   */
  private ReaderFailoverResult getConnectionFromHostList(List<HostInfo> hosts)
      throws SQLException {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CompletionService<ReaderFailoverResult> completionService =
        new ExecutorCompletionService<>(executor);

    ReaderFailoverResult result;

    try {
      for (int i = 0; i < hosts.size(); i += 2) {
        boolean secondAttemptPresent = i + 1 < hosts.size();
        Future<ReaderFailoverResult> attempt1 =
            completionService.submit(
                new ConnectionAttemptTask(hosts.get(i)));
        if (secondAttemptPresent) {
          Future<ReaderFailoverResult> attempt2 =
              completionService.submit(
                  new ConnectionAttemptTask(hosts.get(i + 1)));
          result = getResultFromAttemptPair(attempt1, attempt2, completionService);
        } else {
          result = getNextResult(completionService);
        }

        if (result != null && result.isConnected()) {

          String portPair = "<null>";
          HostInfo resultHostInfo = result.getHost();
          if ( resultHostInfo != null) {
            portPair = resultHostInfo.getHostPortPair();
          }
          LOGGER.log(Level.FINE, "[ClusterAwareReaderFailoverHandler] Established read-only connection to ''{0}''",
              portPair);
          return result;
        }

        try {
          TimeUnit.MILLISECONDS.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLException(
              "Thread was interrupted.", "70100", e);
        }
      }

      return new ReaderFailoverResult(null, null, false);

    } finally {
      executor.shutdownNow();
    }
  }

  /**
   * Retrieves the result from both connection attempts
   *
   * @param attempt1 The first attempt to check
   * @param attempt2 The second attempt to check
   * @param service The completion service used to check the results from the future variables
   * @return {@link ReaderFailoverResult} instance which contains the connection results. If it's filled
   *     with null values and is not connected then the failover has failed
   * @throws SQLException when a thread gets interrupted and failover fails
   */
  private ReaderFailoverResult getResultFromAttemptPair(
      Future<ReaderFailoverResult> attempt1,
      Future<ReaderFailoverResult> attempt2,
      CompletionService<ReaderFailoverResult> service)
      throws SQLException {
    try {
      Future<ReaderFailoverResult> firstCompleted =
          service.poll(this.timeoutMs, TimeUnit.MILLISECONDS);
      if (firstCompleted != null) {
        ReaderFailoverResult result = firstCompleted.get();
        if (result.isConnected()) {
          if (firstCompleted.equals(attempt1)) {
            attempt2.cancel(true);
          } else {
            attempt1.cancel(true);
          }
          return result;
        }
      }
    } catch (ExecutionException e) {
      return getNextResult(service);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // "Thread was interrupted"
      throw new SQLException("Thread was interrupted.", "70100", e);
    }
    return getNextResult(service);
  }

  /**
   * Returns the result from a connection attempt
   *
   * @param service The CompletionService used
   * @return {@link ReaderFailoverResult}
   * @throws SQLException when a thread gets interrupted and failover fails
   */
  private ReaderFailoverResult getNextResult(CompletionService<ReaderFailoverResult> service)
      throws SQLException {
    try {
      Future<ReaderFailoverResult> result = service.poll(this.timeoutMs, TimeUnit.MILLISECONDS);
      if (result == null) {
        return new ReaderFailoverResult(null, null, false);
      }
      return result.get();
    } catch (ExecutionException e) {
      return new ReaderFailoverResult(null, null, false);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // "Thread was interrupted"
      throw new SQLException("Thread was interrupted.", "70100", e);
    }
  }

  /**
   * Class to handle connection attempts
   */
  private class ConnectionAttemptTask implements Callable<ReaderFailoverResult> {
    private final HostInfo newHost;

    /**
     * Constructor for ConnectionAttemptTask
     *
     * @param newHost The new host to attempt connection
     */
    private ConnectionAttemptTask(
        HostInfo newHost) {
      this.newHost = newHost;
    }

    /**
     * Call ReaderFailoverResult.
     * */
    @Override
    public ReaderFailoverResult call() {
      if (StringUtils.isNullOrEmpty(this.newHost.getHost())) {
        return new ReaderFailoverResult(null, null, false);
      }
      LOGGER.log(Level.FINE, "[ClusterAwareReaderFailoverHandler] Attempting to establish read-only connection to ''{0}''", this.newHost.getHostPortPair());

      try {
        BaseConnection conn = connProvider.connect(this.newHost.toHostSpec(), connectionProps, this.newHost.getUrl(connectionProps));
        topologyService.removeFromDownHostList(this.newHost);
        return new ReaderFailoverResult(conn, this.newHost, true);
      } catch (SQLException e) {
        topologyService.addToDownHostList(this.newHost);
        LOGGER.log(Level.FINE, "[ClusterAwareReaderFailoverHandler] Failed to establish read-only connection to ''{0}''", this.newHost);
        return new ReaderFailoverResult(null, null, false);
      }
    }
  }
}
