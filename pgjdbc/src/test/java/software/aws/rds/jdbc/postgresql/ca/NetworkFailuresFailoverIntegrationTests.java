/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.postgresql.PGProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

@Disabled
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class NetworkFailuresFailoverIntegrationTests extends FailoverIntegrationTest {

  private final Logger logger = Logger.getLogger(NetworkFailuresFailoverIntegrationTests.class.getName());

  private static final String SOCKET_TIMEOUT_VAL = "1"; //sec
  private static final String CONNECT_TIMEOUT_VAL = "3"; //sec

  // XXX.YYY.rds.amazonaws.com
  private final String pgAuroraHostBase = pgAuroraInstanceDnsSuffix.substring(1);
  private final String dbHostCluster = pgAuroraClusterIdentifier + ".cluster-"
      + pgAuroraHostBase;
  private final String dbHostClusterRo = pgAuroraClusterIdentifier + ".cluster-ro-"
      + pgAuroraHostBase;

  private final String dbHostInstancePattern = "%s." + pgAuroraHostBase;
  private final String dbConnInstancePattern = pgJDBCProtocol + dbHostInstancePattern
      + ":" + pgAuroraPort + "/" + pgAuroraTestDb ;

  public NetworkFailuresFailoverIntegrationTests() throws SQLException {
    super();
  }

  /**
   * Driver loses connection to a current writer host, tries to reconnect to it and fails.
   */
  @Test
  public void test_1_LostConnectionToWriter() throws SQLException {
    FailoverSocketFactory.flushAllStaticData();

    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory.class.getName());
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection = DriverManager.getConnection(pgAuroraWriterConnectionStr , props);

    String currentWriter = queryInstanceId(testConnection);

    String lastConnectedHost = FailoverSocketFactory.getHostFromLastConnection();
    assertEquals(FailoverSocketFactory.STATUS_CONNECTED + dbHostCluster, lastConnectedHost);

    // put down current writer host
    FailoverSocketFactory.downHost(dbHostCluster);
    FailoverSocketFactory.downHost(String.format(dbHostInstancePattern, currentWriter));

    SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(testConnection));

    assertEquals("08001", exception.getSQLState());

    testConnection.close();
  }

  /**
   * Driver loses connection to a reader host, tries to reconnect to another reader and achieve success.
   */
  @Test
  public void test_2_LostConnectionToReader() throws SQLException {
    FailoverSocketFactory.flushAllStaticData();

    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.GATHER_PERF_METRICS.getName(), "true");
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), "software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory");
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection =
        DriverManager.getConnection(pgAuroraReadonlyConnectionStr, props);

    String currentReader = queryInstanceId(testConnection);
    logger.log(Level.INFO, "Current reader: " + currentReader);

    // put down current reader host
    FailoverSocketFactory.downHost(dbHostClusterRo);
    FailoverSocketFactory.downHost(String.format(dbHostInstancePattern, currentReader));

    SQLException exception = assertThrows(SQLException.class, () -> querySelect1(testConnection));
    assertEquals("08S02", exception.getSQLState());

    String newReader = queryInstanceId(testConnection);
    logger.log(Level.INFO, "New reader: " + newReader);

    testConnection.close();
  }

  /**
   * Driver loses connection to a reader host, tries to reconnect to another reader, fails and connects to a writer.
   */
  @Test
  public void test_3_LostConnectionToReader() throws SQLException {
    FailoverSocketFactory.flushAllStaticData();

    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.ENABLE_CLUSTER_AWARE_FAILOVER.getName(), "false");
    final Connection checkWriterConnection =
        DriverManager.getConnection(pgAuroraWriterConnectionStr , props);
    String currentWriter = queryInstanceId(checkWriterConnection);
    logger.log(Level.INFO, "Current writer: " + currentWriter);
    checkWriterConnection.close();

    props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.GATHER_PERF_METRICS.getName(), "true");
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), "software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory");
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection =
        DriverManager.getConnection(pgAuroraReadonlyConnectionStr, props);

    String currentReader = queryInstanceId(testConnection);
    logger.log(Level.INFO, "Current reader: " + currentReader);

    final String[] allInstances = instanceIDs;
    // put down all hosts except a writer
    FailoverSocketFactory.downHost(dbHostClusterRo);
    for (String host : allInstances) {
      if (!host.equalsIgnoreCase(currentWriter)) {
        FailoverSocketFactory.downHost(String.format(dbHostInstancePattern, host));
        logger.log(Level.INFO, "Put down host: " + String.format(dbHostInstancePattern, host));
      }
    }

    SQLException exception = assertThrows(SQLException.class, () -> querySelect1(testConnection));
    assertEquals("08S02", exception.getSQLState());

    String newReader = queryInstanceId(testConnection);
    logger.log(Level.INFO, "New reader: " + newReader);
    assertEquals(currentWriter, newReader);

    testConnection.close();
  }

  /**
   * Driver is connected to a reader instance.
   * When driver loses connection to this reader host, it connects to a writer.
   */
  @Test
  public void test_4_LostConnectionToReaderAndConnectToWriter() throws SQLException {
    FailoverSocketFactory.flushAllStaticData();

    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.ENABLE_CLUSTER_AWARE_FAILOVER.getName(), "false");

    final Connection checkWriterConnection =
        DriverManager.getConnection(pgAuroraWriterConnectionStr , props);
    String currentWriter = queryInstanceId(checkWriterConnection);
    logger.log(Level.INFO, "Current writer: " + currentWriter);
    checkWriterConnection.close();

    final String[] allInstances = instanceIDs;
    ArrayList<String> readers = new ArrayList<>(Arrays.asList(allInstances));
    readers.remove(currentWriter);
    Collections.shuffle(readers);
    String anyReader = readers.get(0);
    logger.log(Level.INFO, "Test connection to reader: " + anyReader);

    props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.GATHER_PERF_METRICS.getName(), "true");
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), "software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory");
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection =
        DriverManager.getConnection(String.format(dbConnInstancePattern, anyReader), props);

    String currentReader = queryInstanceId(testConnection);
    logger.log(Level.INFO, "Current reader: " + currentReader);
    assertEquals(anyReader, currentReader);

    // put down current reader
    FailoverSocketFactory.downHost(String.format(dbHostInstancePattern, currentReader));

    SQLException exception = assertThrows(SQLException.class, () -> querySelect1(testConnection));
    assertEquals("08S02", exception.getSQLState());

    String newInstance = queryInstanceId(testConnection);
    logger.log(Level.INFO, "New connection to: " + newInstance);
    assertEquals(currentWriter, newInstance);

    testConnection.close();
  }

  /**
   * Driver is connected to a reader instance and connection is read-only.
   * When driver loses connection to this reader host, it connects to another reader.
   */
  @Test
  public void test_5_LostConnectionToReaderAndConnectToReader() throws SQLException {
    FailoverSocketFactory.flushAllStaticData();

    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.ENABLE_CLUSTER_AWARE_FAILOVER.getName(), "false");

    final Connection checkWriterConnection =
        DriverManager.getConnection(pgAuroraWriterConnectionStr , props);
    String currentWriter = queryInstanceId(checkWriterConnection);
    logger.log(Level.INFO, "Current writer: " + currentWriter);
    checkWriterConnection.close();

    final String[] allInstances = instanceIDs;
    ArrayList<String> readers = new ArrayList<>(Arrays.asList(allInstances));
    readers.remove(currentWriter);
    Collections.shuffle(readers);
    String anyReader = readers.get(0);
    logger.log(Level.INFO, "Test connection to reader: " + anyReader);

    props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.GATHER_PERF_METRICS.getName(), "true");
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), "software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory");
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection =
        DriverManager.getConnection(String.format(dbConnInstancePattern, anyReader), props);

    String currentReader = queryInstanceId(testConnection);
    logger.log(Level.INFO, "Current reader: " + currentReader);
    assertEquals(anyReader, currentReader);

    testConnection.setReadOnly(true);

    // put down current reader
    FailoverSocketFactory.downHost(String.format(dbHostInstancePattern, currentReader));

    SQLException exception = assertThrows(SQLException.class, () -> querySelect1(testConnection));
    assertEquals("08S02", exception.getSQLState());

    String newInstance = queryInstanceId(testConnection);
    logger.log(Level.INFO, "New connection to: " + newInstance);
    assertNotEquals(currentWriter, newInstance);

    testConnection.close();
  }

}
