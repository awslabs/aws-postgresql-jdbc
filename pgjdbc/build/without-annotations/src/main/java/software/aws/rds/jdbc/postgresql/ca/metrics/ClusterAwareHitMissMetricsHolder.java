/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */
package software.aws.rds.jdbc.postgresql.ca.metrics;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple implementation of hit-miss metric. It collects a total number of events been reported as
 * well as number of "hit" events.
 *
 * <p>Example of hit-miss metric: loaded or not loaded data, data found in cache or not found.
 */

public class ClusterAwareHitMissMetricsHolder implements ClusterAwareMetricsReporter<Boolean> {

  protected String metricName;
  protected int numberOfReports;
  protected int numberOfHits;

  private Object lockObject = new Object();

  /**
   * Initialize a metric holder with a metric name.
   *
   * @param metricName Metric name
   */
  public ClusterAwareHitMissMetricsHolder(String metricName) {
    this.metricName = metricName;
  }

  /**
   * Register (notify) a metric holder about event.
   *
   * @param isHit True if event is a "hit" event.
   */
  public void register(Boolean isHit) {
    synchronized (this.lockObject) {
      this.numberOfReports++;
      if (isHit) {
        this.numberOfHits++;
      }
    }
  }

  /**
   * Report collected metric to a provided logger.
   *
   * @param log A logger to report collected metric.
   */
  public void reportMetrics(Logger log) {
    StringBuilder logMessage = new StringBuilder(256);

    logMessage.append("** Performance Metrics Report for '");
    logMessage.append(this.metricName);
    logMessage.append("' **\n");
    logMessage.append("\nNumber of reports: " + this.numberOfReports);
    if (this.numberOfReports > 0) {
      logMessage.append("\nNumber of hits: " + this.numberOfHits);
      logMessage.append("\nRatio: " + (this.numberOfHits * 100.0 / this.numberOfReports) + " %");
    }

    log.log(Level.INFO, logMessage.toString());
  }
}
