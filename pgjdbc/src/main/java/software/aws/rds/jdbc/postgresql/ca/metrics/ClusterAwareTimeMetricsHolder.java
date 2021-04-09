/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca.metrics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple implementation of timing metric. It collects an execution time for particular
 * case/event.
 *
 * <p>Use registerQueryExecutionTime(long queryTimeMs) to report an execution time.
 */
public class ClusterAwareTimeMetricsHolder implements ClusterAwareMetricsReporter<Long> {

  private final String metricName;
  private final ArrayList<Long> times = new ArrayList<>();

  /**
   * Initialize a metric holder with a metric name.
   *
   * @param metricName Metric name
   */
  public ClusterAwareTimeMetricsHolder(String metricName) {
    this.metricName = metricName;
  }

  /**
   * Registers the time
   *
   * @param elapsedTime time to register
   */
  public void register(Long elapsedTime) {
    this.times.add(elapsedTime);
  }

  /**
   * Report collected metric to a provided logger.
   *
   * @param log A logger to report collected metric.
   */
  public void reportMetrics(Logger log) {

    int size = this.times.size();
    int index95 = (int) Math.ceil(0.95 * size);
    long[] sortedTimes = this.times.stream().mapToLong(x -> x).sorted().toArray();
    long shortestTime = sortedTimes.length == 0 ? 0 : sortedTimes[0];
    long longestTime = sortedTimes.length == 0 ? 0 : sortedTimes[size - 1];
    long average = (long) Arrays.stream(sortedTimes).mapToDouble(x -> x).average().orElse(0);

    StringBuilder logMessage = new StringBuilder(256);

    logMessage.append("** Performance Metrics Report for '").append(this.metricName).append("' **\n");
    logMessage.append("\nNumber of reports: ").append(size);
    if (size > 0) {
      logMessage.append("\nLongest reported time: ").append(longestTime).append(" ms");
      logMessage.append("\nShortest reported time: ").append(shortestTime).append(" ms");
      logMessage.append("\nAverage query execution time: ").append(average).append(" ms");
      logMessage.append("\np95 value: ").append(sortedTimes[index95 - 1]);
    }
    log.log(Level.INFO, logMessage.toString());
  }
}
