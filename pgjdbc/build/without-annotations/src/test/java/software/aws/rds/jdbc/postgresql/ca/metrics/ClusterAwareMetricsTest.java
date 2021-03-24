package software.aws.rds.jdbc.postgresql.ca.metrics;

import org.junit.jupiter.api.Test;
import software.aws.rds.jdbc.postgresql.ca.metrics.ClusterAwareMetrics;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClusterAwareMetricsTest {

  @Test
  public void testTimeReportLog(){

    ClusterAwareMetrics metrics = new ClusterAwareMetrics();
    LoggerTest log = new LoggerTest();
    ArrayList<String> logs;

    metrics.reportMetrics(log);
    logs = log.getReportMessages();

    // Assert that all the messages should only contain the number of reports
    for (String message : logs) {
      assertTrue(message.contains("Number of reports: 0"));
      assertFalse(message.contains("Shortest reported time:"));
      assertFalse(message.contains("Longest reported time:"));
      assertFalse(message.contains("Average query execution time:"));
      assertFalse(message.contains("p95 value:"));
    }

    log.clearReportMessages();

    for (int i = 0; i <= 100; i++) {
      metrics.registerFailureDetectionTime(i);
      metrics.registerReaderFailoverProcedureTime(i);
      metrics.registerWriterFailoverProcedureTime(i);
      metrics.registerTopologyQueryTime(i);
    }

    metrics.reportMetrics(log);
    logs = log.getReportMessages();

    // Assert that all the messages are properly displayed
    for (String message : logs) {

      if (message.contains("Successful Failover Reconnects")){
        continue;
      }
      assertTrue(message.contains("Number of reports: 101"));
      assertTrue(message.contains("Shortest reported time: 0"));
      assertTrue(message.contains("Longest reported time: 100"));
      assertTrue(message.contains("Average query execution time: 50"));
      assertTrue(message.contains("p95 value: 95"));
      System.out.println(message);
    }

  }
  @Test
  public void testHitOrMissReportLog() {

    ClusterAwareMetrics metrics = new ClusterAwareMetrics();
    LoggerTest log = new LoggerTest();
    ArrayList<String> logs;

    for(int i = 0; i < 10; i++) {
      metrics.registerFailoverConnects(true);
    }

    for(int i = 0; i < 10; i++) {
      metrics.registerFailoverConnects(false);
    }

    metrics.reportMetrics(log);
    logs = log.getReportMessages();

    for (String message : logs) {

      if (message.contains("Successful Failover Reconnects")){

        assertTrue(message.contains("Number of reports: 20"));
        assertTrue(message.contains("Number of hits: 10"));
        assertTrue(message.contains("Ratio: 50.0 %"));

      }
    }

  }

  class LoggerTest extends Logger {

    private ArrayList<String> reportMessages = new ArrayList<String>();

    LoggerTest() {
      super(null,null);
    }

    @Override
    public void log(Level level, String msg) {
      reportMessages.add(msg);
    }

    public ArrayList<String> getReportMessages(){
      return reportMessages;
    }

    public void clearReportMessages(){
      this.reportMessages.clear();
    }

  }

}


