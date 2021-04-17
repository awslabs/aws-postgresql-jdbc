/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.core.BaseConnection;
import org.postgresql.util.Util;

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
 * <p>The goal of the reader failover process is to connect to any available reader. In order to connect
 * faster, this implementation tries to connect to two readers at the same time. The first
 * successfully connected reader is returned as the process result. If both readers are unavailable
 * (i.e. could not be connected to), the process picks up another pair of readers and repeats. If no
 * reader connection has successfully been established, the process may consider connecting to a writer host, and other
 * hosts marked as down.
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
   */
  public ClusterAwareReaderFailoverHandler(
          TopologyService topologyService, ConnectionProvider connProvider, Properties connectionProps,
          int failoverTimeoutMs, int timeoutMs) {
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
   * Called to start the reader failover process. This process tries to connect to any reader. If no
   * reader is available then driver may also try to connect to a writer host, down hosts, and the
   * current reader host.
   *
   * @param hosts       The latest cluster topology information that we had before connection failure.
   * @param currentHost The currently connected host that has failed.
   *
   * @return {@link ReaderFailoverResult} The results of this process.
   * @throws SQLException when a thread gets interrupted and failover fails
   */
  @Override
  public ReaderFailoverResult failover(List<HostInfo> hosts, @Nullable HostInfo currentHost)
          throws SQLException {
    if (hosts.isEmpty()) {
      LOGGER.log(Level.FINE, "[ClusterAwareReaderFailoverHandler] failover was called with an invalid (empty) topology");
      return new ReaderFailoverResult(null, null, false);
    }

    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<ReaderFailoverResult> future = submitInternalFailoverTask(hosts, currentHost, executor);
    return getInternalFailoverResult(executor, future);
  }

  /**
   * Submit the internal failover task for execution
   *
   * @param hosts The latest cluster topology information that we had before connection failure
   * @param currentHost The currently connected host that has failed
   * @param currentHost The currently connected host that has failed
   * @param executor The {@link ExecutorService} to submit the internal failover task to
   * @return the {@link Future} associated with the submitted internal failover task
   */
  private Future<ReaderFailoverResult> submitInternalFailoverTask(List<HostInfo> hosts, @Nullable HostInfo currentHost,
                                                                  ExecutorService executor) {
    Future<ReaderFailoverResult> future = executor.submit(() -> {
      ReaderFailoverResult result = null;
      while (result == null || !result.isConnected()) {
        result = failoverInternal(hosts, currentHost);
        TimeUnit.SECONDS.sleep(1);
      }
      return result;
    });
    executor.shutdown();
    return future;
  }

  /**
   * Get the results of the internal reader failover process
   *
   * @param executor The executor that the internal failover task was submitted to
   * @param future The future associated with the internal failover task
   * @return the results of the internal failover process
   * @throws SQLException If this thread is interrupted while trying to get the results
   */
  private ReaderFailoverResult getInternalFailoverResult(ExecutorService executor,
                                                         Future<ReaderFailoverResult> future) throws SQLException {
    ReaderFailoverResult defaultResult = new ReaderFailoverResult(null, null, false);

    try {
      ReaderFailoverResult result = future.get(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
      return result == null ? defaultResult : result;

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
   * @param currentHost The currently connected host
   * @return The {@link ReaderFailoverResult} of the internal failover process
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
   * @param hostList Current topology for the cluster.
   *
   * @return {@link ReaderFailoverResult} The results of this process.
   * @throws SQLException when a thread gets interrupted and failover fails
   */
  @Override
  public ReaderFailoverResult getReaderConnection(List<HostInfo> hostList) throws SQLException {
    if (hostList.isEmpty()) {
      LOGGER.log(Level.FINE, "[ClusterAwareReaderFailoverHandler] getReaderConnection was called with an invalid (empty) topology");
      return new ReaderFailoverResult(null, null, false);
    }

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

    try {
      for (int i = 0; i < hosts.size(); i += 2) {
        // submit connection attempt tasks in batches of 2
        ReaderFailoverResult result = getResultFromNextTaskBatch(hosts, executor, completionService, i);
        if (result.isConnected()) {
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
   * Submit connection attempt tasks in batches of two and retrieve the results. If the first task to complete is
   * successful, cancel the other task, otherwise wait on its completion and return the result. If the size of the host
   * list is odd, the last call to this method will submit a batch of one instead.
   *
   * @param hostGroup The list of potential reader connections to attempt
   * @param executor The {@link ExecutorService} that the tasks will be submitted to through the {@link CompletionService}
   * @param completionService The {@link CompletionService} to submit the tasks to
   * @param i The index of the next host that we should attempt to connect to
   * @return The {@link ReaderFailoverResult} representing the results of this task batch
   * @throws SQLException If this thread is interrupted while trying to get the results
   */
  private ReaderFailoverResult getResultFromNextTaskBatch(List<HostInfo> hostGroup, ExecutorService executor,
                                                          CompletionService<ReaderFailoverResult> completionService,
                                                          int i) throws SQLException {
    ReaderFailoverResult result;
    int numTasks = i + 1 < hostGroup.size() ? 2 : 1;
    completionService.submit(new ConnectionAttemptTask(hostGroup.get(i)));

    if (numTasks == 2) {
      completionService.submit(new ConnectionAttemptTask(hostGroup.get(i + 1)));
    }

    for (int taskNum = 0; taskNum < numTasks; taskNum++) {
      result = getNextResult(completionService);

      if (result.isConnected()) {
        logConnectionSuccess(result);
        executor.shutdownNow();
        return result;
      }
    }
    return new ReaderFailoverResult(null, null, false);
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
    ReaderFailoverResult defaultResult = new ReaderFailoverResult(null, null, false);
    try {
      Future<ReaderFailoverResult> future = service.poll(this.timeoutMs, TimeUnit.MILLISECONDS);
      if (future == null) {
        return new ReaderFailoverResult(null, null, false);
      }

      ReaderFailoverResult result = future.get();
      return result == null ? defaultResult : result;
    } catch (ExecutionException e) {
      return defaultResult;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // "Thread was interrupted"
      throw new SQLException("Thread was interrupted.", "70100", e);
    }
  }

  /**
   * Log a connection success, indicating the successful host port pair that we connected to
   *
   * @param result The successful {@link ReaderFailoverResult}
   */
  private void logConnectionSuccess(ReaderFailoverResult result) {
    String hostPortPair = "<null>";
    HostInfo host = result.getHost();
    if (host != null) {
      hostPortPair = host.getHostPortPair();
    }
    LOGGER.log(Level.FINE, "[ClusterAwareReaderFailoverHandler] Established read-only connection to ''{0}''", hostPortPair);
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
      if (Util.isNullOrEmpty(this.newHost.getHost())) {
        return new ReaderFailoverResult(null, null, false);
      }
      LOGGER.log(Level.FINE,
              "[ClusterAwareReaderFailoverHandler] Attempting to establish read-only connection to ''{0}''",
              this.newHost.getHostPortPair());

      try {
        BaseConnection conn = connProvider.connect(this.newHost.toHostSpec(), connectionProps,
                this.newHost.getUrl(connectionProps));
        topologyService.removeFromDownHostList(this.newHost);
        return new ReaderFailoverResult(conn, this.newHost, true);
      } catch (SQLException e) {
        topologyService.addToDownHostList(this.newHost);
        LOGGER.log(Level.FINE,
                "[ClusterAwareReaderFailoverHandler] Failed to establish read-only connection to ''{0}''", this.newHost);
        return new ReaderFailoverResult(null, null, false);
      }
    }
  }
}
