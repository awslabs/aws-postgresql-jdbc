package software.aws.rds.jdbc.postgresql.ca.metrics;

import java.util.logging.Logger;

public interface ClusterAwareMetricsReporter<T> {

  /**
   * Registers a value to be collected
   *
   * @param value the value that you want to register
   */
  public void register(T value);

  /**
   * Reports metrics based on the data collected.
   *
   * @param log the log passed into the report metrics
   */
  public void reportMetrics(Logger log);
}
