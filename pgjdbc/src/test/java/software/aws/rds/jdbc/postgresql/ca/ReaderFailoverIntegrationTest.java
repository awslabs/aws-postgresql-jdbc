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

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Disabled
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class ReaderFailoverIntegrationTest extends FailoverIntegrationTest {

  public ReaderFailoverIntegrationTest() throws SQLException {
  }

  /** Current reader dies, the driver failover to another reader. */
  @Test
  public void test2_1_failFromReaderToAnotherReader() throws SQLException, InterruptedException {
    testConnection = connectToReaderInstance(instanceID2);

    startCrashingInstanceAndWaitUntilDown(instanceID2);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are now connected to a new reader instance.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(currentConnectionId));
    assertNotEquals(currentConnectionId, instanceID2);
  }

  /** Current reader dies, other known reader instances do not respond, failover to writer. */
  @Test
  public void test2_2_failFromReaderToWriterWhenAllReadersAreDown()
      throws SQLException, InterruptedException {
    testConnection = connectToReaderInstance(instanceID2);

    // Fist kill instances 3-5.
    startCrashingInstanceAndWaitUntilDown(instanceID3);
    startCrashingInstanceAndWaitUntilDown(instanceID4);
    startCrashingInstanceAndWaitUntilDown(instanceID5);

    // Then kill instance 2.
    startCrashingInstanceAndWaitUntilDown(instanceID2);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that the driver failed over to the writer instance (Instance1).
    final String currentConnectionId = queryInstanceId(testConnection);
    assertEquals(instanceID1, currentConnectionId);
    assertTrue(isDBInstanceWriter(currentConnectionId));
  }

  /**
   * Current reader dies, after failing to connect to several reader instances, failover to another
   * reader.
   */
  @Test
  public void test2_3_failFromReaderToReaderWithSomeReadersAreDown()
      throws SQLException, InterruptedException {
    testConnection = connectToReaderInstance(instanceID2);

    startCrashingInstanceAndWaitUntilDown(instanceID3);
    startCrashingInstanceAndWaitUntilDown(instanceID4);
    startCrashingInstanceAndWaitUntilDown(instanceID2);

    TimeUnit.SECONDS.sleep(2);
    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we failed over to the only remaining reader instance (Instance5) OR Writer
    // instance (Instance1).
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(
        currentConnectionId.equals(instanceID5) || currentConnectionId.equals(instanceID1));
  }

  /**
   * Current reader dies, failover to another reader repeat to loop through instances in the cluster
   * testing ability to revive previously down reader instance.
   */
  @Test
  public void test2_4_failoverBackToThePreviouslyDownReader()
      throws SQLException, InterruptedException {
    final String firstReaderInstanceId = instanceID2;

    // Connect to reader (Instance2).
    testConnection = connectToReaderInstance(firstReaderInstanceId);

    // Start crashing reader (Instance2).
    startCrashingInstanceAndWaitUntilDown(firstReaderInstanceId);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to another reader instance.
    final String secondReaderInstanceId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(secondReaderInstanceId));
    assertNotEquals(firstReaderInstanceId, secondReaderInstanceId);

    // Crash the second reader instance.
    startCrashingInstanceAndWaitUntilDown(secondReaderInstanceId);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to the third reader instance.
    final String thirdReaderInstanceId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(thirdReaderInstanceId));
    assertNotEquals(firstReaderInstanceId, thirdReaderInstanceId);
    assertNotEquals(secondReaderInstanceId, thirdReaderInstanceId);

    // Grab the id of the fourth reader instance.
    final List<String> readerInstanceIds = getDBClusterReaderInstanceIds();
    readerInstanceIds.remove(instanceID1);
    readerInstanceIds.remove(instanceID2);
    readerInstanceIds.remove(secondReaderInstanceId);
    readerInstanceIds.remove(thirdReaderInstanceId);

    final String fourthInstanceId = readerInstanceIds.get(0);

    // Crash the fourth reader instance.
    startCrashingInstanceAndWaitUntilDown(fourthInstanceId);

    // Stop crashing the first and second.
    stopCrashingInstanceAndWaitUntilUp(firstReaderInstanceId);
    stopCrashingInstanceAndWaitUntilUp(secondReaderInstanceId);

    // Sleep 30+ seconds to force cache update upon successful query.
    Thread.sleep(31000);

    final String currentInstanceId = queryInstanceId(testConnection);
    assertEquals(thirdReaderInstanceId, currentInstanceId);

    // Start crashing the third instance.
    startCrashingInstanceAndWaitUntilDown(thirdReaderInstanceId);

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
    // Crashing Instance3, Instance4 and Instance5
    startCrashingInstanceAndWaitUntilDown(instanceID3);
    startCrashingInstanceAndWaitUntilDown(instanceID4);
    startCrashingInstanceAndWaitUntilDown(instanceID5);

    // Connect to Instance2 which is the only reader that is up.
    testConnection = connectToReaderInstance(instanceID2);

    // Crash Instance2
    startCrashingInstanceAndWaitUntilDown(instanceID2);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are currently connected to the writer Instance1.
    String currentConnectionId = queryInstanceId(testConnection);
    assertEquals(instanceID1, currentConnectionId);
    assertTrue(isDBInstanceWriter(currentConnectionId));

    // Stop Crashing reader Instance2 and Instance3
    stopCrashingInstanceAndWaitUntilUp(instanceID2);
    stopCrashingInstanceAndWaitUntilUp(instanceID3);

    // Crash writer Instance1.
    failoverClusterToATargetAndWaitUntilWriterChanged(instanceID1, instanceID3);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to one of the available instances.
    currentConnectionId = queryInstanceId(testConnection);
    assertTrue(
        instanceID2.equals(currentConnectionId) || instanceID3.equals(currentConnectionId));
  }

  /** Connect to a readonly cluster endpoint and ensure that we are doing a reader failover. */
  @Test
  public void test2_6_clusterEndpointReadOnlyFailover() throws SQLException, InterruptedException {
    testConnection = createConnectionToReadonlyClusterEndpoint();

    final String initialConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(initialConnectionId));

    startCrashingInstanceAndWaitUntilDown(initialConnectionId);

    assertFirstQueryThrows(testConnection, "08S02");

    final String newConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(newConnectionId));
    assertNotEquals(newConnectionId, initialConnectionId);
  }
}
