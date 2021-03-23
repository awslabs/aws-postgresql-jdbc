/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca.metrics;

import java.util.logging.Logger;

public interface ClusterAwareMetricsReporter<T> {

  /**
   * Registers a value to be collected
   *
   * @param value the value that you want to register
   */
  void register(T value);

  /**
   * Reports metrics based on the data collected.
   *
   * @param log the log passed into the report metrics
   */
  void reportMetrics(Logger log);
}
