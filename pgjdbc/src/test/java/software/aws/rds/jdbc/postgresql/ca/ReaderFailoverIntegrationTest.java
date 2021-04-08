/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.postgresql.PGProperty;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

@Disabled
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class ReaderFailoverIntegrationTest extends FailoverIntegrationTest {

  public ReaderFailoverIntegrationTest() throws SQLException {
    super();
  }

  /** Current reader dies, the driver failover to another reader. */
  @Test
  public void test2_1_failFromReaderToAnotherReader() throws SQLException {
    assertTrue(clusterSize >= 3, "Minimal cluster configuration: 1 writer + 2 readers");

    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory.class.getName());
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection = connectToReaderInstance(instanceIDs[1], props);

    FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, instanceIDs[1]));

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are now connected to a new reader instance.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(currentConnectionId));
    assertNotEquals(currentConnectionId, instanceIDs[1]);
  }

  /** Current reader dies, other known reader instances do not respond, failover to writer. */
  @Test
  public void test2_2_failFromReaderToWriterWhenAllReadersAreDown()
      throws SQLException, InterruptedException {
    assertTrue(clusterSize >= 2, "Minimal cluster configuration: 1 writer + 1 reader");

    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory.class.getName());
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection = connectToReaderInstance(instanceIDs[1], props);

    // Fist kill instances other than writer and connected reader
    for (int i = 2; i < instanceIDs.length; i++) {
      FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, instanceIDs[i]));
    }

    TimeUnit.SECONDS.sleep(3);

    // Then kill connected reader.
    FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, instanceIDs[1]));

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that the driver failed over to the writer instance (Instance1).
    final String currentConnectionId = queryInstanceId(testConnection);
    assertEquals(instanceIDs[0], currentConnectionId);
    assertTrue(isDBInstanceWriter(currentConnectionId));
  }

  /**
   * Current reader dies, after failing to connect to several reader instances, failover to another
   * reader.
   */
  @Test
  public void test2_3_failFromReaderToReaderWithSomeReadersAreDown()
      throws SQLException, InterruptedException {
    assertTrue(clusterSize >= 3, "Minimal cluster configuration: 1 writer + 2 readers");

    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory.class.getName());
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection = connectToReaderInstance(instanceIDs[1], props);

    // Fist kill all reader instances except one
    for (int i = 1; i < instanceIDs.length - 1; i++) {
      FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, instanceIDs[i]));
    }

    TimeUnit.SECONDS.sleep(2);
    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we failed over to the only remaining reader instance (Instance5) OR Writer
    // instance (Instance1).
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(
        currentConnectionId.equals(instanceIDs[instanceIDs.length - 1]) || currentConnectionId.equals(instanceIDs[0]));
  }

  /**
   * Current reader dies, failover to another reader repeat to loop through instances in the cluster
   * testing ability to revive previously down reader instance.
   */
  @Test
  public void test2_4_failoverBackToThePreviouslyDownReader()
      throws SQLException {
    assertTrue(clusterSize >= 5, "Minimal cluster configuration: 1 writer + 4 readers");

    final String firstReaderInstanceId = instanceIDs[1];

    // Connect to reader (Instance2).
    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory.class.getName());
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection = connectToReaderInstance(firstReaderInstanceId, props);

    // Start crashing reader (Instance2).
    FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, firstReaderInstanceId));

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to another reader instance.
    final String secondReaderInstanceId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(secondReaderInstanceId));
    assertNotEquals(firstReaderInstanceId, secondReaderInstanceId);

    // Crash the second reader instance.
    FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, secondReaderInstanceId));

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to the third reader instance.
    final String thirdReaderInstanceId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(thirdReaderInstanceId));
    assertNotEquals(firstReaderInstanceId, thirdReaderInstanceId);
    assertNotEquals(secondReaderInstanceId, thirdReaderInstanceId);

    // Grab the id of the fourth reader instance.
    final HashSet<String> readerInstanceIds = new HashSet<>(Arrays.asList(instanceIDs)); // getDBClusterReaderInstanceIds();
    readerInstanceIds.remove(instanceIDs[0]);
    readerInstanceIds.remove(firstReaderInstanceId);
    readerInstanceIds.remove(secondReaderInstanceId);
    readerInstanceIds.remove(thirdReaderInstanceId);

    Optional<String> optionalInstanceId = readerInstanceIds.stream().findFirst();
    assertTrue(optionalInstanceId.isPresent());
    final String fourthInstanceId = optionalInstanceId.get();

    // Crash the fourth reader instance.
    FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, fourthInstanceId));

    // Stop crashing the first and second.
    FailoverSocketFactory.upHost(String.format(pgHostInstancePattern, firstReaderInstanceId));
    FailoverSocketFactory.upHost(String.format(pgHostInstancePattern, secondReaderInstanceId));

    final String currentInstanceId = queryInstanceId(testConnection);
    assertEquals(thirdReaderInstanceId, currentInstanceId);

    // Start crashing the third instance.
    FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, thirdReaderInstanceId));

    assertFirstQueryThrows(testConnection, "08S02");

    final String lastInstanceId = queryInstanceId(testConnection);

    assertTrue(
        firstReaderInstanceId.equals(lastInstanceId)
            || secondReaderInstanceId.equals(lastInstanceId));
  }

  /**
   * Current reader dies, no other reader instance, failover to writer, then writer dies, failover
   * to another available reader instance.
   */
  @Test
  public void test2_5_failFromReaderToWriterToAnyAvailableInstance()
      throws SQLException, InterruptedException {

    assertTrue(clusterSize >= 3, "Minimal cluster configuration: 1 writer + 2 readers");

    // Crashing all readers except the first one
    for (int i = 2; i < instanceIDs.length; i++) {
      FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, instanceIDs[i]));
    }

    // Connect to Instance2 which is the only reader that is up.
    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory.class.getName());
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection = connectToReaderInstance(instanceIDs[1], props);

    // Crash Instance2
    FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, instanceIDs[1]));

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are currently connected to the writer Instance1.
    String currentConnectionId = queryInstanceId(testConnection);
    assertEquals(instanceIDs[0], currentConnectionId);
    assertTrue(isDBInstanceWriter(currentConnectionId));

    // Stop Crashing reader Instance2 and Instance3
    FailoverSocketFactory.upHost(String.format(pgHostInstancePattern, instanceIDs[1]));
    FailoverSocketFactory.upHost(String.format(pgHostInstancePattern, instanceIDs[2]));

    // Crash writer Instance1.
    failoverClusterToATargetAndWaitUntilWriterChanged(instanceIDs[0], instanceIDs[2]);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to one of the available instances.
    currentConnectionId = queryInstanceId(testConnection);
    assertTrue(
            instanceIDs[1].equals(currentConnectionId) || instanceIDs[2].equals(currentConnectionId));
  }

  /** Connect to a readonly cluster endpoint and ensure that we are doing a reader failover. */
  @Test
  public void test2_6_clusterEndpointReadOnlyFailover() throws SQLException {
    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory.class.getName());
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection = createConnectionToReadonlyClusterEndpoint(props);

    final String initialConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(initialConnectionId));

    FailoverSocketFactory.downHost(pgAuroraClusterIdentifier + ".cluster-ro-" + pgAuroraInstanceDnsSuffix.substring(1));
    FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, initialConnectionId));

    assertFirstQueryThrows(testConnection, "08S02");

    final String newConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(newConnectionId));
    assertNotEquals(newConnectionId, initialConnectionId);
  }
}
