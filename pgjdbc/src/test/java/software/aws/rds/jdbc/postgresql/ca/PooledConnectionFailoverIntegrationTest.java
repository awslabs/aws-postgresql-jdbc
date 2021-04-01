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

@Disabled
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class PooledConnectionFailoverIntegrationTest extends FailoverIntegrationTest {

  PooledConnectionFailoverIntegrationTest() throws SQLException {
    super();
  }

  /** Writer connection failover within the connection pool. */
  @Test
  public void test4_1_pooledWriterConnection_basicfailover()
      throws SQLException, InterruptedException {
    final String initalWriterId = getDBClusterWriterInstanceId();
    assertEquals(instanceIDs[0], initalWriterId);

    testConnection = createPooledConnectionWithInstanceId(initalWriterId);

    // Crash writer Instance1 and nominate Instance2 as the new writer
    failoverClusterToATargetAndWaitUntilWriterChanged(initalWriterId, instanceIDs[1]);

    assertFirstQueryThrows(testConnection, "08S02");

    // Execute Query again to get the current connection id;
    /*final */String currentConnectionId = queryInstanceId(testConnection);

    // Assert that we are connected to the new writer after failover happens.
    assertTrue(isDBInstanceWriter(currentConnectionId));
    final String nextWriterId = getDBClusterWriterInstanceId();
    assertEquals(nextWriterId, currentConnectionId);
    assertEquals(instanceIDs[1], currentConnectionId);

    // Assert that the pooled connection is valid.
    assertTrue(testConnection.isValid(IS_VALID_TIMEOUT));
  }

  /** Reader connection failover within the connection pool. */
  @Test
  public void test4_2_pooledReaderConnection_basicfailover()
      throws SQLException, InterruptedException {
    testConnection = createPooledConnectionWithInstanceId(instanceIDs[1]);
    testConnection.setReadOnly(true);

    startCrashingInstances(instanceIDs[1]);
    makeSureInstancesDown(instanceIDs[1]);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are now connected to a new reader instance.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceReader(currentConnectionId));
    assertNotEquals(currentConnectionId, instanceIDs[1]);

    // Assert that the pooled connection is valid.
    assertTrue(testConnection.isValid(IS_VALID_TIMEOUT));
  }
}
