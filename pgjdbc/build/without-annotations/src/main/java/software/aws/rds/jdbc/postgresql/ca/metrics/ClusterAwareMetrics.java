/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
package software.aws.rds.jdbc.postgresql.ca.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Logger;

/**
 * Collect certain performance metrics for ClusterAwareConnectionProxy.
 *
 * <p>Performance (duration in time) metrics: - Failover Detection Duration of time between a start
 * of executing sql statement to the moment when driver identifies a communication error and starts
 * a failover process (a process to re-connect to another cluster instance). - Writer Failover
 * Procedure records the duration of time running writer failover procedure. - Reader Failover Procedure
 * records the duration of time running reader failover procedure. - Topology Query records the duration
 * of time it takes to fetch the new topology
 *
 * <p>Performance (hit-miss) metrics: - Successful Failover Reconnects A total number of failover
 * events vs a number of successful ones. "Hit" events record whenever a failover procedure successfully
 * reconnects to an appropriate instance and "Miss" events record whenever a failover procedure fails to
 * reconenct to an appropriate instance.
 */
public class ClusterAwareMetrics{

  private ClusterAwareMetricsReporter failureDetection =
      new ClusterAwareTimeMetricsHolder("Failover Detection");
  private ClusterAwareMetricsReporter writerFailoverProcedure =
      new ClusterAwareTimeMetricsHolder("Writer Failover Procedure");
  private ClusterAwareMetricsReporter readerFailoverProcedure =
      new ClusterAwareTimeMetricsHolder("Reader Failover Procedure");

  private ClusterAwareMetricsReporter queryTopology =
      new ClusterAwareTimeMetricsHolder("Topology Query");

  private ClusterAwareMetricsReporter failoverConnects =
      new ClusterAwareHitMissMetricsHolder("Successful Failover Reconnects");
  private ArrayList<ClusterAwareMetricsReporter> reporters;

  /**
   * Constructor for ClusterAwareMetrics. Creates a list of all metrics that will be recorded.
   */
  public ClusterAwareMetrics() {
    reporters = new ArrayList<ClusterAwareMetricsReporter>(
        Arrays.asList(failureDetection, writerFailoverProcedure, readerFailoverProcedure, queryTopology,
            failoverConnects)
    );
  }

  /**
   * Registers the time it takes to failover
   *
   * @param timeMs Time it took to detect that failover is needed
   */
  public void registerFailureDetectionTime(long timeMs) {
    this.failureDetection.register(timeMs);
  }

  /**
   * Registers the time it takes to perform a writer failover once initiated
   *
   * @param timeMs Time it took for writer to failover
   */
  public void registerWriterFailoverProcedureTime(long timeMs) {
    this.writerFailoverProcedure.register(timeMs);
  }

  /**
   * Registers the time it takes to perform a reader failover once initiated
   *
   * @param timeMs Time it took for reader to failover
   */
  public void registerReaderFailoverProcedureTime(long timeMs) {
    this.readerFailoverProcedure.register(timeMs);
  }

  /**
   * Registers the time it takes to fetch the topology from the database
   *
   * @param timeMs Time it to took to complete the topology query
   */
  public void registerTopologyQueryTime(long timeMs) {
    this.queryTopology.register(timeMs);
  }

  /**
   * Registers whether or not a failover procedure successfully reconnected
   *
   * @param isHit True if failover was successful
   */
  public void registerFailoverConnects(boolean isHit) {
    this.failoverConnects.register(isHit);
  }

  /**
   * Create a report from the current available data and attach it to a Logger instance
   *
   * @param log the log used to display
   */
  public void reportMetrics(Logger log) {

    reporters.forEach(report -> report.reportMetrics(log));
  }

}
