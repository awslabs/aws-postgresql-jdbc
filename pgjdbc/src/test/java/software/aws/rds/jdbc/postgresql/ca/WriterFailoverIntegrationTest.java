/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.postgresql.PGProperty;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

@Disabled
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class WriterFailoverIntegrationTest extends FailoverIntegrationTest {

  WriterFailoverIntegrationTest() throws SQLException {
    super();
  }

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer. Driver failover occurs when executing a method against the connection
   */
  @Test
  public void test1_1_failFromWriterToNewWriter_failOnConnectionInvocation()
      throws SQLException, InterruptedException {
    final String initalWriterId = instanceIDs[0];

    testConnection = connectToWriterInstance(initalWriterId);

    // Crash Instance1 and nominate a new writer
    failoverClusterAndWaitUntilWriterChanged(initalWriterId);

    // Failure occurs on Connection invocation
    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to the new writer after failover happens.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceWriter(currentConnectionId));
    assertNotEquals(currentConnectionId, initalWriterId);
  }

  /**
   * Current writer dies, a reader instance is nominated to be a new writer, failover to the new
   * writer Driver failover occurs when executing a method against an object bound to the connection
   * (eg a Statement object created by the connection).
   */
  @Test
  public void test1_2_failFromWriterToNewWriter_failOnConnectionBoundObjectInvocation()
      throws SQLException, InterruptedException {
    final String initalWriterId = instanceIDs[0];

    testConnection = connectToWriterInstance(initalWriterId);
    Statement stmt = testConnection.createStatement();

    // Crash Instance1 and nominate a new writer
    failoverClusterAndWaitUntilWriterChanged(initalWriterId);

    // Failure occurs on Statement invocation
    assertFirstQueryThrows(stmt, "08S02");

    // Assert that the driver is connected to the new writer after failover happens.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceWriter(currentConnectionId));
    assertNotEquals(initalWriterId, currentConnectionId);
  }

  /** Current writer dies, no available reader instance, connection fails. */
  @Test
  public void test1_3_writerConnectionFailsDueToNoReader()
      throws SQLException {
    final String initalWriterId = instanceIDs[0];

    Properties props = new Properties();
    props.setProperty(PGProperty.USER.getName(), pgAuroraUsername);
    props.setProperty(PGProperty.PASSWORD.getName(), pgAuroraPassword);
    props.setProperty(PGProperty.SOCKET_FACTORY.getName(), software.aws.rds.jdbc.postgresql.ca.FailoverSocketFactory.class.getName());
    props.setProperty(PGProperty.SOCKET_TIMEOUT.getName(), SOCKET_TIMEOUT_VAL);
    props.setProperty(PGProperty.CONNECT_TIMEOUT.getName(), CONNECT_TIMEOUT_VAL);
    testConnection = connectToWriterInstance(initalWriterId, props);

    // Crash all reader instances (2 - 5).
    for (int i = 2; i < instanceIDs.length; i++) {
      FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, instanceIDs[i]));
    }

    // Crash the writer Instance1.
    FailoverSocketFactory.downHost(String.format(pgHostInstancePattern, initalWriterId));

    // All instances should be down, assert exception thrown with SQLState code 08001
    // (SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE)
    assertFirstQueryThrows(testConnection, "08001");
  }

  /** Writer fails within a transaction. Open transaction with setAutoCommit(false) */
  @Test
  public void test3_1_writerFailWithinTransaction_setAutoCommitFalse()
      throws SQLException, InterruptedException {
    final String initialClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(instanceIDs[0], initialClusterWriterId);

    testConnection = connectToWriterInstance(initialClusterWriterId);

    final Statement testStmt1 = testConnection.createStatement();
    testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_2");
    testStmt1.executeUpdate(
        "CREATE TABLE test3_2 (id int not null primary key, test3_2_field varchar(255) not null)");
    testConnection.setAutoCommit(false); // open a new transaction

    final Statement testStmt2 = testConnection.createStatement();
    testStmt2.executeUpdate("INSERT INTO test3_2 VALUES (1, 'test field string 1')");

    failoverClusterAndWaitUntilWriterChanged(initialClusterWriterId);

    // If there is an active transaction, roll it back and return an error with SQLState 08007.
    final SQLException exception =
        assertThrows(
            SQLException.class,
            () -> testStmt2.executeUpdate("INSERT INTO test3_2 VALUES (2, 'test field string 2')"));
    assertEquals("08007", exception.getSQLState());

    // Attempt to query the instance id.
    final String currentConnectionId = queryInstanceId(testConnection);
    // Assert that we are connected to the new writer after failover happens.
    assertTrue(isDBInstanceWriter(currentConnectionId));
    final String nextClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(currentConnectionId, nextClusterWriterId);
    assertNotEquals(initialClusterWriterId, nextClusterWriterId);

    // testStmt2 can NOT be used anymore since it's invalid

    final Statement testStmt3 = testConnection.createStatement();
    final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_2");
    rs.next();
    // Assert that NO row has been inserted to the table;
    assertEquals(0, rs.getInt(1));

    testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_2");
  }

  /** Writer fails within a transaction. Open transaction with "START TRANSACTION". */
  @Test
  public void test3_2_writerFailWithinTransaction_startTransaction()
      throws SQLException, InterruptedException {
    final String initialClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(instanceIDs[0], initialClusterWriterId);

    testConnection = connectToWriterInstance(initialClusterWriterId);

    final Statement testStmt1 = testConnection.createStatement();
    testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_3");
    testStmt1.executeUpdate(
        "CREATE TABLE test3_3 (id int not null primary key, test3_3_field varchar(255) not null)");
    testStmt1.executeUpdate("START TRANSACTION"); // open a new transaction

    final Statement testStmt2 = testConnection.createStatement();
    testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (1, 'test field string 1')");

    failoverClusterAndWaitUntilWriterChanged(initialClusterWriterId);

    // If there is an active transaction, roll it back and return an error with SQLState 08007.
    final SQLException exception =
        assertThrows(
            SQLException.class,
            () -> testStmt2.executeUpdate("INSERT INTO test3_3 VALUES (2, 'test field string 2')"));
    assertEquals("08007", exception.getSQLState());

    // Attempt to query the instance id.
    final String currentConnectionId = queryInstanceId(testConnection);
    // Assert that we are connected to the new writer after failover happens.
    assertTrue(isDBInstanceWriter(currentConnectionId));
    final String nextClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(currentConnectionId, nextClusterWriterId);
    assertNotEquals(initialClusterWriterId, nextClusterWriterId);

    // testStmt2 can NOT be used anymore since it's invalid

    final Statement testStmt3 = testConnection.createStatement();
    final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_3");
    rs.next();
    // Assert that NO row has been inserted to the table;
    assertEquals(0, rs.getInt(1));

    testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_3");
  }

  /** Writer fails within NO transaction. */
  @Test
  public void test3_3_writerFailWithNoTransaction() throws SQLException, InterruptedException {
    final String initialClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(instanceIDs[0], initialClusterWriterId);

    testConnection = connectToWriterInstance(initialClusterWriterId);

    final Statement testStmt1 = testConnection.createStatement();
    testStmt1.executeUpdate("DROP TABLE IF EXISTS test3_4");
    testStmt1.executeUpdate(
        "CREATE TABLE test3_4 (id int not null primary key, test3_4_field varchar(255) not null)");

    final Statement testStmt2 = testConnection.createStatement();
    testStmt2.executeUpdate("INSERT INTO test3_4 VALUES (1, 'test field string 1')");

    failoverClusterAndWaitUntilWriterChanged(initialClusterWriterId);

    final SQLException exception =
        assertThrows(
            SQLException.class,
            () -> testStmt2.executeUpdate("INSERT INTO test3_4 VALUES (2, 'test field string 2')"));
    assertEquals("08S02", exception.getSQLState());

    // Attempt to query the instance id.
    final String currentConnectionId = queryInstanceId(testConnection);
    // Assert that we are connected to the new writer after failover happens.
    assertTrue(isDBInstanceWriter(currentConnectionId));
    final String nextClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(currentConnectionId, nextClusterWriterId);
    assertNotEquals(initialClusterWriterId, nextClusterWriterId);

    // testStmt2 can NOT be used anymore since it's invalid
    final Statement testStmt3 = testConnection.createStatement();
    final ResultSet rs = testStmt3.executeQuery("SELECT count(*) from test3_4 WHERE id = 1");
    rs.next();
    // Assert that row with id=1 has been inserted to the table;
    assertEquals(1, rs.getInt(1));

    final ResultSet rs1 = testStmt3.executeQuery("SELECT count(*) from test3_4 WHERE id = 2");
    rs1.next();
    // Assert that row with id=2 has NOT been inserted to the table;
    assertEquals(0, rs1.getInt(1));

    testStmt3.executeUpdate("DROP TABLE IF EXISTS test3_4");
  }

  @Test
  public void test5_1_takeOverConnectionProperties() throws SQLException, InterruptedException {
    final String initialClusterWriterId = getDBClusterWriterInstanceId();
    assertEquals(instanceIDs[0], initialClusterWriterId);

    final int socketTimeout = 50;
    final int newRowFetchSize = 70;

    final Properties props = new Properties();
    props.setProperty("user", pgAuroraUsername);
    props.setProperty("password", pgAuroraPassword);
    props.setProperty("socketTimeout", String.valueOf(socketTimeout));
    props.setProperty("defaultRowFetchSize", String.valueOf(newRowFetchSize));

    testConnection = connectToWriterInstance(initialClusterWriterId, props);

    queryInstanceId(testConnection);

    assertEquals(socketTimeout * 1000, testConnection.getNetworkTimeout());

    final Statement testStmt1 = testConnection.createStatement();
    assertEquals(newRowFetchSize, testStmt1.getFetchSize());

    // Crash Instance1 and nominate a new writer.
    failoverClusterAndWaitUntilWriterChanged(initialClusterWriterId);

    assertFirstQueryThrows(testConnection, "08S02");

    // Assert that we are connected to the new writer after failover happens.
    final String currentConnectionId = queryInstanceId(testConnection);
    assertTrue(isDBInstanceWriter(currentConnectionId));
    assertNotEquals(currentConnectionId, initialClusterWriterId);

    // Assert that the connection property is maintained.
    assertEquals(socketTimeout * 1000, testConnection.getNetworkTimeout());

    final Statement testStmt2 = testConnection.createStatement();
    assertEquals(newRowFetchSize, testStmt2.getFetchSize());
  }
}
