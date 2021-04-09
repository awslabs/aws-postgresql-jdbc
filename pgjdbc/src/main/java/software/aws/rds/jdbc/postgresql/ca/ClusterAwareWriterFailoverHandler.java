/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.core.BaseConnection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of WriterFailoverHandler.
 *
 * <p>Writer Failover Process goal is to re-establish connection to a writer. Connection to a
 * writer may be disrupted either by temporary network issue, or due to writer host unavailability during
 * cluster failover. This handler tries both approaches in parallel: 1) try to re-connect to the
 * same writer host, 2) try to update cluster topology and connect to a newly elected writer.
 */
public class ClusterAwareWriterFailoverHandler implements WriterFailoverHandler {
  static final int WRITER_CONNECTION_INDEX = 0;

  private static final transient Logger LOGGER = Logger.getLogger(
      ClusterAwareWriterFailoverHandler.class.getName());

  protected int maxFailoverTimeoutMs = 60000; // 60 sec
  protected int readTopologyIntervalMs = 5000; // 5 sec
  protected int reconnectWriterIntervalMs = 5000; // 5 sec
  protected TopologyService topologyService;
  protected ConnectionProvider connectionProvider;
  protected ReaderFailoverHandler readerFailoverHandler;
  protected Properties connectionProps;

  /**
   * ClusterAwareWriterFailoverHandler constructor
   *
   * @param topologyService The topology service used to update and query the cluster topology
   * @param connectionProvider The connection provider used to establish the database connection
   * @param connectionProps a set of properties for a new connections
   * @param readerFailoverHandler The reader failover handler used with this connection
   */
  public ClusterAwareWriterFailoverHandler(
          TopologyService topologyService,
          ConnectionProvider connectionProvider,
          Properties connectionProps,
          ReaderFailoverHandler readerFailoverHandler) {
    this.topologyService = topologyService;
    this.connectionProvider = connectionProvider;
    this.connectionProps = connectionProps;
    this.readerFailoverHandler = readerFailoverHandler;
  }

  /**
   * ClusterAwareWriterFailoverHandler constructor.
   *
   * @param topologyService The topology service used to update and query the cluster topology
   * @param connectionProvider The connection provider used to establish the database connection
   * @param connectionProps a set of properties for a new connections
   * @param readerFailoverHandler The reader failover handler used with this connection
   * @param failoverTimeoutMs The maximum allowable time for failover in ms
   * @param readTopologyIntervalMs The refresh rate of the cluster topology in ms
   * @param reconnectWriterIntervalMs The time in ms to wait before re-attempting to reconnect to the writer
   */
  public ClusterAwareWriterFailoverHandler(
          TopologyService topologyService,
          ConnectionProvider connectionProvider,
          Properties connectionProps,
          ReaderFailoverHandler readerFailoverHandler,
          int failoverTimeoutMs,
          int readTopologyIntervalMs,
          int reconnectWriterIntervalMs) {
    this(topologyService, connectionProvider, connectionProps, readerFailoverHandler);
    this.maxFailoverTimeoutMs = failoverTimeoutMs;
    this.readTopologyIntervalMs = readTopologyIntervalMs;
    this.reconnectWriterIntervalMs = reconnectWriterIntervalMs;
  }

  /**
   * Called to start Writer Failover Process.
   *
   * @param currentTopology Current topology for the cluster.
   * @return {@link WriterFailoverResult} The results of this process.
   */
  @Override
  public WriterFailoverResult failover(List<HostInfo> currentTopology) throws SQLException {
    if (currentTopology.isEmpty()) {
      LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] failover was called with an invalid (empty) topology");
      return new WriterFailoverResult(false, false, new ArrayList<>(), null);
    }

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    CompletionService<WriterFailoverResult> completionService =
            new ExecutorCompletionService<>(executorService);
    submitTasks(currentTopology, executorService, completionService);
    try {
      for (int numTasks = 2; numTasks > 0; numTasks--) {
        WriterFailoverResult result = getNextResult(executorService, completionService);
        if (result.isConnected()) {
          return result;
        }
      }

      LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] Failed to connect to the writer instance.");
      return new WriterFailoverResult(false, false, new ArrayList<>(), null);
    } finally {
      if (!executorService.isTerminated()) {
        executorService.shutdownNow(); // terminate all remaining tasks
      }
    }
  }

  /**
   * Mark the failed writer as down and submit one task to attempt reconnection to the same writer
   * host and another to check for a new writer host.
   *
   * @param currentTopology The latest topology information from before the connection failed
   * @param executorService The {@link ExecutorService} that manages the task execution, which will
   *                        be closed at the end of this method to prevent more task submissions
   * @param completionService The {@link CompletionService} to submit the tasks to
   */
  private void submitTasks(List<HostInfo> currentTopology, ExecutorService executorService,
                          CompletionService<WriterFailoverResult> completionService) {
    HostInfo writerHost = currentTopology.get(WRITER_CONNECTION_INDEX);
    this.topologyService.addToDownHostList(writerHost);
    completionService.submit(new ReconnectToWriterHandler(writerHost));
    completionService.submit(new WaitForNewWriterHandler(currentTopology, writerHost));
    executorService.shutdown();
  }

  /**
   * Poll the {@link CompletionService} passed in for the next completed task and process the results
   *
   * @param executorService The {@link ExecutorService} that manages the task execution
   * @param completionService The @link CompletionService} that the task was submitted to
   * @return {@link WriterFailoverResult} The results of the next task that completes.
   */
  private WriterFailoverResult getNextResult(ExecutorService executorService,
                                             CompletionService<WriterFailoverResult> completionService) throws SQLException {
    try {
      Future<WriterFailoverResult> firstCompleted = completionService.poll(
              this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
      if (firstCompleted == null) {
        // The task was unsuccessful and we have timed out
        return new WriterFailoverResult(false, false, new ArrayList<>(), null);
      }
      WriterFailoverResult result = firstCompleted.get();
      if (result.isConnected()) {
        executorService.shutdownNow();
        logTaskSuccess(result);
        return result;
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw createInterruptedException(e);
    } catch (ExecutionException e) {
      // return failure below
    }
    return new WriterFailoverResult(false, false, new ArrayList<>(), null);
  }

  /**
   * Log the relevant information for the writer host that we successfully connected to, unless
   * the result was marked as a success but contains invalid information. The latter scenario should
   * only happen very rarely, if at all.
   *
   * @param result The {@link WriterFailoverResult} to log. This method should only be called with a
   *        {@link WriterFailoverResult} for which {@link WriterFailoverResult#isConnected()} returns
   *        true.
   */
  private void logTaskSuccess(WriterFailoverResult result) {
    @Nullable List<HostInfo> topology = result.getTopology();
    if (topology == null || topology.isEmpty()) {
      String taskId = result.isNewHost() ? "TaskB" : "TaskA";
      LOGGER.log(Level.SEVERE, "[ClusterAwareWriterFailoverHandler] " + taskId
              + " successfully established a connection but doesn't contain a valid topology");
      return;
    }
    HostInfo newWriterHost = topology.get(WRITER_CONNECTION_INDEX);
    String newWriterHostPair =
            newWriterHost == null ? "<null>" : newWriterHost.getHostPortPair();
    String message;
    if (result.isNewHost()) {
      message = "[ClusterAwareWriterFailoverHandler] Successfully connected to the new writer instance: ''{0}''";
    } else  {
      message = "[ClusterAwareWriterFailoverHandler] Successfully re-connected to the current writer instance: ''{0}''";
    }
    LOGGER.log(Level.FINE, message, newWriterHostPair);
  }

  /**
   * Method used to transform an interrupted exception to a SQL exception during writer failover
   *
   * @param e The original exception caught
   * @return A {@link SQLException} indicating that the thread was interrupted
   */
  private SQLException createInterruptedException(InterruptedException e) {
    return new SQLException("Thread was interrupted.", "70100", e);
  }

  /**
   * Internal class responsible for re-connecting to the original failed writer (aka TaskA).
   */
  private class ReconnectToWriterHandler implements Callable<WriterFailoverResult> {
    private final HostInfo originalWriterHost;

    ReconnectToWriterHandler(HostInfo originalWriterHost) {
      this.originalWriterHost = originalWriterHost;
    }

    /**
     * This task will repeatedly attempt to reconnect to the original writer.
     */
    @Override
    public WriterFailoverResult call() {
      LOGGER.log(Level.FINE,
              "[ClusterAwareWriterFailoverHandler] [TaskA] Attempting to re-connect to the "
                      + "current writer instance: ''{0}''",
              this.originalWriterHost.getHostPortPair());
      try {
        while (true) {
          try {
            BaseConnection conn = connectionProvider.connect(this.originalWriterHost.toHostSpec(),
                    connectionProps, this.originalWriterHost.getUrl(connectionProps));

            List<HostInfo> latestTopology = topologyService.getTopology(conn, true);
            if (!latestTopology.isEmpty() && isCurrentHostWriter(latestTopology)) {
              topologyService.removeFromDownHostList(this.originalWriterHost);
              return new WriterFailoverResult(true, false, latestTopology, conn);
            }
          } catch (SQLException exception) {
            // ignore
          }

          TimeUnit.MILLISECONDS.sleep(reconnectWriterIntervalMs);
        }
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return new WriterFailoverResult(false, false, new ArrayList<>(), null);
      } catch (Exception ex) {
        LOGGER.log(Level.WARNING, "[ClusterAwareWriterFailoverHandler] [TaskA] encountered an exception: {0}", ex);
        throw ex;
      } finally {
        LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] [TaskA] Finished");
      }
    }

    /**
     * Checks if the current host is a writer
     *
     * @param latestTopology The latest topology that was fetched by the newly established connection
     * @return True if the current host is a writer. False otherwise.
     */
    private boolean isCurrentHostWriter(List<HostInfo> latestTopology) {
      String currentWriterName = this.originalWriterHost.getInstanceIdentifier();
      HostInfo latestWriter = latestTopology.get(WRITER_CONNECTION_INDEX);
      if (currentWriterName == null || latestWriter == null) {
        return false;
      }

      String latestWriterName = latestWriter.getInstanceIdentifier();
      return latestWriterName != null && latestWriterName.equals(currentWriterName);
    }
  }

  /**
   * Internal class responsible for getting latest cluster topology and connecting to a newly
   * elected writer (aka TaskB).
   */
  private class WaitForNewWriterHandler implements Callable<WriterFailoverResult> {
    private @Nullable BaseConnection currentConnection = null;
    private final @Nullable HostInfo originalWriterHost;
    private List<HostInfo> currentTopology;
    private @Nullable HostInfo currentReaderHost;
    private @Nullable BaseConnection currentReaderConnection;

    WaitForNewWriterHandler(List<HostInfo> currentTopology, @Nullable HostInfo originalWriterHost) {
      this.currentTopology = currentTopology;
      this.originalWriterHost = originalWriterHost;
    }

    /**
     * This task will repeatedly attempt to connect to a reader host in order to check the new topology,
     * and then connect to the newly promoted writer
     */
    public WriterFailoverResult call() {
      LOGGER.log(Level.FINE,
              "[ClusterAwareWriterFailoverHandler] [TaskB] Attempting to connect to a new writer instance");

      try {
        boolean success = false;
        while (!success) {
          connectToReader();
          success = refreshTopologyAndConnectToNewWriter();
          if (!success) {
            closeReaderConnection();
          }
        }
        return new WriterFailoverResult(true, true, this.currentTopology, this.currentConnection);
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        return new WriterFailoverResult(false, false, new ArrayList<>(), null);
      } catch (Exception ex) {
        LOGGER.log(Level.WARNING, "[ClusterAwareWriterFailoverHandler] [TaskB] encountered an exception: {0}", ex);
        throw ex;
      } finally {
        performFinalCleanup();
      }
    }

    /**
     * Repeatedly attempt to establish a connection to a reader host
     *
     * @throws InterruptedException Will throw if TaskA is able to connect to the original writer
     */
    private void connectToReader() throws InterruptedException {
      while (true) {
        try {
          ReaderFailoverResult connResult =
                  readerFailoverHandler.getReaderConnection(this.currentTopology);
          if (connResult.isConnected()
                  && connResult.getConnection() != null
                  && connResult.getHost() != null) {
            this.currentReaderConnection = connResult.getConnection();
            this.currentReaderHost = connResult.getHost();
            String hostPortPair = "<null>";
            if (this.currentReaderHost != null) {
              hostPortPair = this.currentReaderHost.getHostPortPair();
            }
            LOGGER.log(Level.FINE,
                    "[ClusterAwareWriterFailoverHandler] [TaskB] Connected to reader {0}",
                    hostPortPair);
            break;
          }
        } catch (SQLException e) {
          // ignore
        }
        LOGGER.log(Level.FINER, "[ClusterAwareWriterFailoverHandler] [TaskB] Failed to connect to any reader.");
        TimeUnit.MILLISECONDS.sleep(1);
      }
    }

    /**
     * Refresh the topology with the newly-established reader connection and attempt to connect to
     * the newly-promoted writer
     *
     * @return Returns true if a connection to the new writer is successfully established
     */
    private boolean refreshTopologyAndConnectToNewWriter() throws InterruptedException {
      while (this.currentReaderConnection != null) {
        List<HostInfo> topology = topologyService.getTopology(this.currentReaderConnection, true);
        if (!topology.isEmpty()) {
          this.currentTopology = topology;
          logTopology();
          HostInfo writerCandidate = this.currentTopology.get(WRITER_CONNECTION_INDEX);

          if (!isSame(writerCandidate, this.originalWriterHost)) {
            // the new writer is available and it's different from the previous writer
            if (connectToWriter(writerCandidate)) {
              return true;
            }
          }
        }

        TimeUnit.MILLISECONDS.sleep(readTopologyIntervalMs);
      }
      return false;
    }

    /**
     * Check that the writer in the topology is a new host and not the original failed host caused us
     * to initialize failover. If it is a new host, connect to it. The other scenario will be handled
     * by the {@link ReconnectToWriterHandler}
     *
     * @param writerCandidate The {@link HostInfo} for the writer to potentially connect to
     * @return Returns true if a connection to a new writer is successfully established
     */
    private boolean connectToWriter(HostInfo writerCandidate) {
      try {
        LOGGER.log(Level.FINER,
                "[ClusterAwareWriterFailoverHandler] [TaskB] Trying to connect to a new writer {0}",
                writerCandidate.getHostPortPair());

        if (isSame(writerCandidate, this.currentReaderHost)) {
          this.currentConnection = this.currentReaderConnection;
        } else {
          // connect to the new writer
          this.currentConnection = connectionProvider.connect(
                  writerCandidate.toHostSpec(), connectionProps, writerCandidate.getUrl(connectionProps));
        }

        topologyService.removeFromDownHostList(writerCandidate);
        return true;
      } catch (SQLException exception) {
        topologyService.addToDownHostList(writerCandidate);
        return false;
      }
    }

    /**
     * Checks if the writer is the same as before
     *
     * @param writerCandidate Writer to check
     * @param originalWriter Original writer
     * @return true if they are the same writer
     */
    private boolean isSame(HostInfo writerCandidate, @Nullable HostInfo originalWriter) {
      if (writerCandidate == null || originalWriter == null) {
        return false;
      }
      String writerCandidateName = writerCandidate.getInstanceIdentifier();
      return writerCandidateName != null && writerCandidateName.equals(originalWriter.getInstanceIdentifier());
    }

    /**
     * Close the reader connection if not done so already, and mark the relevant fields as null
     */
    private void closeReaderConnection() {
      try {
        if (this.currentReaderConnection != null && !this.currentReaderConnection.isClosed()) {
          this.currentReaderConnection.close();
        }
      } catch (SQLException e) {
        // ignore
      }
      this.currentReaderConnection = null;
      this.currentReaderHost = null;
    }

    /**
     * Method for logging the topology information during writer failover
     *
     */
    private void logTopology() {
      StringBuilder msg = new StringBuilder();
      for (int i = 0; i < this.currentTopology.size(); i++) {
        HostInfo hostInfo = this.currentTopology.get(i);
        msg.append("\n   [")
                .append(i)
                .append("]: ")
                .append(hostInfo == null ? "<null>" : hostInfo.getHost());
      }
      LOGGER.log(Level.FINER,
              "[ClusterAwareWriterFailoverHandler] [TaskB] Topology obtained: {0}",
              msg.toString());
    }

    /**
     * Close the reader connection established by this task unless it happened to be to the newly-
     * elected writer.
     */
    private void performFinalCleanup() {
      // Close the reader connection if it's not needed.
      if (this.currentReaderConnection != null && this.currentConnection != this.currentReaderConnection) {
        try {
          this.currentReaderConnection.close();
        } catch (SQLException e) {
          // ignore
        }
      }
      LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] [TaskB] Finished");
    }
  }
}
