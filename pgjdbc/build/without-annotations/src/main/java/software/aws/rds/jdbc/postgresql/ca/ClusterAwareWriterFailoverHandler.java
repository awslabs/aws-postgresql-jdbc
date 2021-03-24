/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import org.postgresql.core.BaseConnection;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
  private static transient final Logger LOGGER = Logger.getLogger(ClusterAwareWriterFailoverHandler.class.getName());

  protected int maxFailoverTimeoutMs = 60000; // 60 sec
  protected int readTopologyIntervalMs = 5000; // 5 sec
  protected int reconnectWriterIntervalMs = 5000; // 5 sec
  protected TopologyService topologyService;
  protected ConnectionProvider connectionProvider;
  protected ReaderFailoverHandler readerFailoverHandler;

  /**
   * ClusterAwareWriterFailoverHandler constructor
   *
   * @param topologyService The topology service used to update and query the cluster topology
   * @param connectionProvider The connection provider used to establish the database connection
   * @param readerFailoverHandler The reader failover handler used with this connection
   */
  public ClusterAwareWriterFailoverHandler(
      TopologyService topologyService,
      ConnectionProvider connectionProvider,
      ReaderFailoverHandler readerFailoverHandler) {
    this.topologyService = topologyService;
    this.connectionProvider = connectionProvider;
    this.readerFailoverHandler = readerFailoverHandler;
  }

  /**
   * ClusterAwareWriterFailoverHandler constructor.
   *
   * @param topologyService The topology service used to update and query the cluster topology
   * @param connectionProvider The connection provider used to establish the database connection
   * @param readerFailoverHandler The reader failover handler used with this connection
   * @param failoverTimeoutMs The maximum allowable time for failover in ms
   * @param readTopologyIntervalMs The refresh rate of the cluster topology in ms
   * @param reconnectWriterIntervalMs The time in ms to wait before re-attempting to reconnect to the writer
   */
  public ClusterAwareWriterFailoverHandler(
      TopologyService topologyService,
      ConnectionProvider connectionProvider,
      ReaderFailoverHandler readerFailoverHandler,
      int failoverTimeoutMs,
      int readTopologyIntervalMs,
      int reconnectWriterIntervalMs) {
    this(topologyService, connectionProvider, readerFailoverHandler);
    this.maxFailoverTimeoutMs = failoverTimeoutMs;
    this.readTopologyIntervalMs = readTopologyIntervalMs;
    this.reconnectWriterIntervalMs = reconnectWriterIntervalMs;
  }

  /**
   * Called to start Writer Failover Process.
   *
   * @param currentTopology Cluster current topology
   * @return {@link WriterFailoverResult} The results of this process. May return null, which is
   *     considered an unsuccessful result.
   */
  @Override
  public WriterFailoverResult failover(List<HostInfo> currentTopology) throws SQLException {

    ExecutorService executorService = null;
    try {
      executorService = Executors.newFixedThreadPool(2);

      CountDownLatch taskCompletedLatch = new CountDownLatch(1);

      ReconnectToWriterHandler taskA = null;
      Future<?> futureA = null;

      HostInfo writerHost = currentTopology.get(WRITER_CONNECTION_INDEX);
      this.topologyService.addToDownHostList(writerHost);
      if (writerHost != null) {
        taskA = new ReconnectToWriterHandler(
            taskCompletedLatch,
            writerHost,
            this.topologyService,
            this.connectionProvider,
            this.reconnectWriterIntervalMs);
        futureA = executorService.submit(taskA);
      }

      WaitForNewWriterHandler taskB =
          new WaitForNewWriterHandler(
              taskCompletedLatch,
              currentTopology,
              this.topologyService,
              writerHost,
              this.connectionProvider,
              this.readerFailoverHandler,
              this.readTopologyIntervalMs);
      Future<?> futureB = executorService.submit(taskB);

      executorService
          .shutdown(); // stop accepting new tasks but continue with the tasks already in the the pool

      boolean isLatchZero;
      try {
        // wait for any task to complete
        isLatchZero = taskCompletedLatch.await(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw createInterruptedException(e);
      }

      if (!isLatchZero) {
        // latch isn't 0, the tasks are not yet finished;
        // time is out; cancel them
        if (futureA != null) {
          futureA.cancel(true);
        }
        futureB.cancel(true);
        return new WriterFailoverResult(false, false, null, null);
      }

      // latch has passed and that means that either of the tasks are almost finished
      // wait till a task is "officially" done
      while ((futureA == null || !futureA.isDone()) && !futureB.isDone()) {
        try {
          TimeUnit.MILLISECONDS.sleep(1);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw createInterruptedException(e);
        }
      }

      if (futureA != null && futureA.isDone()) {
        if (taskA.isConnected()) {
          // taskA is completed and connected to the writer instance
          futureB.cancel(true);
          LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] Successfully re-connected to the current writer instance: ''{0}''", writerHost.getHostPortPair());

          return new WriterFailoverResult(true, false, taskA.getTopology(), taskA.getCurrentConnection());
        } else {
          // taskA is completed but wasn't successful
          // wait for taskB to complete

          try {
            futureB.get(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
          } catch (TimeoutException | ExecutionException e) {
            // time is out; neither tasks were successful
            LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] Failed to connect to the writer instance.");
            return new WriterFailoverResult(false, false, null, null);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw createInterruptedException(e);
          }

          // taskB is completed; check its results
          if (taskB.isConnected()) {
            HostInfo newWriterHost = taskB.getTopology().get(WRITER_CONNECTION_INDEX);
            String newWriterHostPair = newWriterHost == null ? "<null>" : newWriterHost.getHostPortPair();
            LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] Successfully connected to the new writer instance: ''{0}''", newWriterHostPair);

            return new WriterFailoverResult(
                true, true, taskB.getTopology(), taskB.getCurrentConnection());
          }
          LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] Failed to connect to the writer instance.");
          return new WriterFailoverResult(false, false, null, null);
        }
      } else if (futureB.isDone()) {
        if (taskB.isConnected()) {
          // taskB is done and it successfully connected to the writer instance
          if (futureA != null) {
            futureA.cancel(true);
          }

          HostInfo newWriterHost = taskB.getTopology().get(WRITER_CONNECTION_INDEX);
          String newWriterHostPair =
              newWriterHost == null ? "<null>" : newWriterHost.getHostPortPair();
          LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] Successfully connected to the new writer instance: ''{0}''", newWriterHostPair);

          return new WriterFailoverResult(
              true, true, taskB.getTopology(), taskB.getCurrentConnection());
        } else if (futureA != null) {
          // taskB is completed but it failed to connect to the writer node
          // wait for taskA completes

          try {
            futureA.get(this.maxFailoverTimeoutMs, TimeUnit.MILLISECONDS);
          } catch (TimeoutException | ExecutionException e) {
            // time is out; neither tasks were successful
            LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] Failed to connect to the writer instance.");
            return new WriterFailoverResult(false, false, null, null);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw createInterruptedException(e);
          }

          // taskA is completed; check its results
          if (taskA.isConnected()) {
            LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] Successfully re-connected to the current writer instance: ''{0}''", writerHost.getHostPortPair());
            return new WriterFailoverResult(
                true, false, taskA.getTopology(), taskA.getCurrentConnection());
          }

          LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] Failed to connect to the writer instance.");
          return new WriterFailoverResult(false, false, null, null);
        }
      }

      if (futureA != null) {
        futureA.cancel(true);
      }
      futureB.cancel(true);

      // both taskA and taskB were unsuccessful
      LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] Failed to connect to the writer instance.");
      return new WriterFailoverResult(false, false, null, null);
    } finally {
      if (executorService != null && !executorService.isTerminated()) {
        executorService.shutdownNow(); // terminate all remaining tasks
      }
    }
  }

  /**
   * Method used to change an interrupted exception to an SQL exception during wrtier failover
   *
   * @param e The original exception caught
   * @return An {@link SQLException{}
   */
  private SQLException createInterruptedException(InterruptedException e) {
    return new SQLException("Thread was interrupted.", "70100", e);
  }

  /**
   * Internal class responsible for re-connecting to the current writer (aka TaskA).
   */
  private static class ReconnectToWriterHandler implements Runnable {

    private List<HostInfo> latestTopology = null;
    private final HostInfo currentWriterHost;
    private final CountDownLatch taskCompletedLatch;
    private boolean isConnected = false;
    private BaseConnection currentConnection = null;
    private final ConnectionProvider connectionProvider;
    private final TopologyService topologyService;
    private final int reconnectWriterIntervalMs;

    ReconnectToWriterHandler(
        CountDownLatch taskCompletedLatch,
        HostInfo host,
        TopologyService topologyService,
        ConnectionProvider connectionProvider,
        int reconnectWriterIntervalMs) {
      this.taskCompletedLatch = taskCompletedLatch;
      this.currentWriterHost = host;
      this.topologyService = topologyService;
      this.connectionProvider = connectionProvider;
      this.reconnectWriterIntervalMs = reconnectWriterIntervalMs;
    }

    /**
     * Accessor method for isConnected
     *
     * @return True if the current instance has successfully connected. Else it will return false
     */
    public boolean isConnected() {
      return this.isConnected;
    }

    /**
     * Accessor method for currentConnection
     *
     * @return retrieves the current connection that the ReconnecetToWriterHandler object is connected with
     */
    public BaseConnection getCurrentConnection() {
      return this.currentConnection;
    }

    /**
     * Accessor method for lastTopology
     *
     * @return The most up-to-date topology
     */
    public List<HostInfo> getTopology() {
      return this.latestTopology;
    }

    /**
     * The action of what the thread will do. This thread will continuously attempt to reconnect to
     * the original writer.
     */
    @Override
    public void run() {
      try {
        LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] [TaskA] Attempting to re-connect to the current writer instance: {0}", this.currentWriterHost.getHostPortPair());
        while (true) {
          try {
            BaseConnection conn = this.connectionProvider.connect(this.currentWriterHost.toHostSpec(),
                this.currentWriterHost.getProps(), this.currentWriterHost.getUrl());

            this.latestTopology = this.topologyService.getTopology(conn, true);
            if (this.latestTopology != null
                && !this.latestTopology.isEmpty()
                && isCurrentHostWriter()) {
              this.isConnected = true;
              this.currentConnection = conn;
              this.topologyService.removeFromDownHostList(this.currentWriterHost);
              return;
            }
          } catch (SQLException exception) {
            // ignore
          }

          TimeUnit.MILLISECONDS.sleep(this.reconnectWriterIntervalMs);
        }
      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        this.isConnected = false;
        this.currentConnection = null;
        this.latestTopology = null;
      } catch (Exception ex) {
        LOGGER.log(Level.WARNING, "[ClusterAwareWriterFailoverHandler] [TaskA] encountered an exception: {0}", ex.getMessage());
        this.isConnected = false;
        this.currentConnection = null;
        this.latestTopology = null;
        throw ex;
      } finally {
        LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] [TaskA] Finished");
        // notify that this task is done
        this.taskCompletedLatch.countDown();
      }
    }

    /**
     * Checks if the current host is a writer
     *
     * @return True if the current host is a writer. False otherwise.
     */
    private boolean isCurrentHostWriter() {
      String currentWriterName =
          this.currentWriterHost.getInstanceIdentifier();
      if (currentWriterName == null) {
        return false;
      }

      HostInfo latestWriter = this.latestTopology.get(WRITER_CONNECTION_INDEX);
      if (latestWriter == null) {
        return false;
      }
      String latestWriterName =
          latestWriter.getInstanceIdentifier();
      return latestWriterName != null && latestWriterName.equals(currentWriterName);
    }
  }

  /**
   * Internal class responsible for getting latest cluster topology and connecting to a newly
   * elected writer (aka TaskB).
   */
  private static class WaitForNewWriterHandler implements Runnable {

    private final CountDownLatch taskCompletedLatch;
    private List<HostInfo> latestTopology;
    private BaseConnection currentConnection = null;
    private boolean isConnected;
    private final int readTopologyIntervalMs;
    private final TopologyService topologyService;
    private final HostInfo originalWriterHost;
    private final ConnectionProvider connectionProvider;
    private final List<HostInfo> currentTopology;
    private final ReaderFailoverHandler readerFailoverHandler;
    private HostInfo currentReaderHost;
    private BaseConnection currentReaderConnection;

    WaitForNewWriterHandler(
        CountDownLatch taskCompletedLatch,
        List<HostInfo> currentTopology,
        TopologyService topologyService,
        HostInfo currentHost,
        ConnectionProvider connectionProvider,
        ReaderFailoverHandler readerFailoverHandler,
        int readTopologyIntervalMs) {
      this.taskCompletedLatch = taskCompletedLatch;
      this.currentTopology = currentTopology;
      this.topologyService = topologyService;
      this.originalWriterHost = currentHost;
      this.connectionProvider = connectionProvider;
      this.readerFailoverHandler = readerFailoverHandler;
      this.readTopologyIntervalMs = readTopologyIntervalMs;
    }

    /**
     * Accessor method for isConnected
     *
     * @return True if the current instance has successfully connected. Else it will return false
     */
    public boolean isConnected() {
      return this.isConnected;
    }

    /**
     * Accessor method for lastTopology
     *
     * @return The most up-to-date topology
     */
    public List<HostInfo> getTopology() {
      return this.latestTopology;
    }

    /**
     * Accessor method for currentConnection
     *
     * @return retrieves the current connection that the ReconnecetToWriterHandler object is connected with
     */
    public BaseConnection getCurrentConnection() {
      return this.currentConnection;
    }

    /**
     * The action of what the thread will do. This thread will continuously attempt to connect to
     * the other readers to see if one have been promoted to the new writer.
     */
    public void run() {
      LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] [TaskB] Attempting to connect to a new writer instance");

      try {
        if (this.currentTopology == null) {
          // topology isn't available
          this.isConnected = false;
          return;
        }

        boolean success = false;
        while (!success) {
          connectToReader();
          success = connectToNewWriter();
        }

      } catch (InterruptedException exception) {
        Thread.currentThread().interrupt();
        this.isConnected = false;
        this.currentConnection = null;
        this.latestTopology = null;
      } catch (Exception ex) {
        LOGGER.log(Level.WARNING, "[ClusterAwareWriterFailoverHandler] [TaskB] encountered an exception: {0}", ex.getMessage());
        this.isConnected = false;
        this.currentConnection = null;
        this.latestTopology = null;
        throw ex;
      } finally {
        // Close the reader connection if it's not needed.
        if (this.currentReaderConnection != null
            && this.currentConnection != this.currentReaderConnection) {
          try {
            this.currentReaderConnection.close();
          } catch (SQLException e) {
            // ignore
          }
        }
        LOGGER.log(Level.FINE, "[ClusterAwareWriterFailoverHandler] [TaskB] Finished");
        this.taskCompletedLatch.countDown();
      }
    }

    /**
     * Method to attempt to connect to check if the other readers will be promoted
     *
     * @throws InterruptedException Will throw if TaskA is able to connect to the original writer
     */
    private void connectToReader() throws InterruptedException {
      // Close reader connection if it's not needed.
      try {
        if (this.currentReaderConnection != null && !this.currentReaderConnection.isClosed()) {
          this.currentReaderConnection.close();
        }
      } catch (SQLException e) {
        // ignore
      }

      this.currentReaderConnection = null;
      HostInfo connHost = null;
      this.currentReaderHost = null;

      while (true) {
        try {
          ReaderFailoverResult connResult =
              this.readerFailoverHandler.getReaderConnection(this.currentTopology);
          this.currentReaderConnection =
              connResult != null && connResult.isConnected() ? connResult.getConnection() : null;
          connHost =
              connResult != null && connResult.isConnected() ? connResult.getHost() : null;
        } catch (SQLException e) {
          // ignore
        }

        if (this.currentReaderConnection == null) {
          LOGGER.log(Level.FINER, "[ClusterAwareWriterFailoverHandler] [TaskB] Failed to connect to any reader.");
        } else {
          this.currentReaderHost = connHost;
          if (currentReaderHost != null) {
            LOGGER.log(Level.FINE,
                "[ClusterAwareWriterFailoverHandler] [TaskB] Connected to reader {0}",
                this.currentReaderHost.getHostPortPair());
            break;
          }
        }

        TimeUnit.MILLISECONDS.sleep(1);
      }
    }

    /**
     * Re-read topology and wait for a new writer.
     *
     * @return Returns true if successful
     */
    private boolean connectToNewWriter() throws InterruptedException {
      while (true) {
        this.latestTopology = this.topologyService.getTopology(this.currentReaderConnection, true);

        if (this.latestTopology == null) {
          // topology couldn't be obtained; there may be issues with the reader connection
          return false;
        }

        if (!this.latestTopology.isEmpty()) {
          logTopology();
          HostInfo writerCandidate = this.latestTopology.get(WRITER_CONNECTION_INDEX);

          if (writerCandidate != null && (this.originalWriterHost == null ||
              !isSame(writerCandidate, this.originalWriterHost))) {
            // the new writer is available and it's different from the previous writer
            try {
              LOGGER.log(Level.FINER,
                  "[ClusterAwareWriterFailoverHandler] [TaskB] Trying to connect to a new writer {0}", writerCandidate.getHostPortPair());

              if (isSame(writerCandidate, this.currentReaderHost)) {
                this.currentConnection = this.currentReaderConnection;
              } else {
                // connected to the new writer
                this.currentConnection = this.connectionProvider.connect(
                    writerCandidate.toHostSpec(), writerCandidate.getProps(), writerCandidate.getUrl());
              }
              this.isConnected = true;

              this.topologyService.removeFromDownHostList(writerCandidate);
              return true;
            } catch (SQLException exception) {
              this.topologyService.addToDownHostList(writerCandidate);
            }
          }
        }

        TimeUnit.MILLISECONDS.sleep(this.readTopologyIntervalMs);
      }
    }

    /**
     * Checks if the writer is the same as before
     *
     * @param writerCandidate Writer to check
     * @param originalWriter Original writer
     * @return true if they are the same writer
     */
    private boolean isSame(HostInfo writerCandidate, HostInfo originalWriter) {
      if (writerCandidate == null) {
        return false;
      }
      String writerCandidateName = writerCandidate.getInstanceIdentifier();
      return writerCandidateName != null && writerCandidateName.equals(originalWriter.getInstanceIdentifier());
    }

    /**
     * Method for logging the topology information during writer failover
     *
     */
    private void logTopology() {
      StringBuilder msg = new StringBuilder();
      for (int i = 0; i < this.latestTopology.size(); i++) {
        HostInfo hostInfo = this.latestTopology.get(i);
        msg.append("\n   [")
            .append(i)
            .append("]: ")
            .append(hostInfo == null ? "<null>" : hostInfo.getHost());
      }
      LOGGER.log(Level.FINER, "[ClusterAwareWriterFailoverHandler] [TaskB] Topology obtained: {0}", msg.toString());
    }
  }
}
