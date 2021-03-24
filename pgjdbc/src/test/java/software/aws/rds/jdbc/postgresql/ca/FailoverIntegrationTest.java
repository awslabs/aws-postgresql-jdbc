/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.DBCluster;
import com.amazonaws.services.rds.model.DBClusterMember;
import com.amazonaws.services.rds.model.DescribeDBClustersRequest;
import com.amazonaws.services.rds.model.DescribeDBClustersResult;
import com.amazonaws.services.rds.model.FailoverDBClusterRequest;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public abstract class FailoverIntegrationTest {

  // Load environment variables
  protected final String pgAuroraClusterIdentifier = System.getenv("PG_AURORA_CLUSTER_IDENTIFIER");
  protected final String pgAuroraInstanceDnsSuffix = System.getenv("PG_AURORA_INSTANCE_DNS_SUFFIX");
  protected final String pgAuroraPort = System.getenv("PG_AURORA_PORT") != null
      ? System.getenv("PG_AURORA_PORT") : "5432";
  protected final String pgAuroraTestDb = System.getenv("PG_AURORA_TEST_DB");
  protected final String pgJDBCProtocol = "jdbc:postgresql:aws://";
  protected final String pgAuroraUsername = System.getenv("TEST_USERNAME");
  protected final String pgAuroraPassword = System.getenv("TEST_PASSWORD");

  // Connection pool config parameters
  private static final int connPoolMinIdle = 5;
  private static final int connPoolMaxIdle = 10;
  private static final int connPoolMaxOpenPreparedStatements = 100;

  // Connection Strings
  protected final String dbAuroraInstanceConnectionStrSuffix = pgAuroraInstanceDnsSuffix + ":"
      + pgAuroraPort + "/" + pgAuroraTestDb;

  protected final String dbAuroraReadonlyConnectionStrSuffix =  ".cluster-ro-"
      + dbAuroraInstanceConnectionStrSuffix.substring(1);

  protected final String pgAuroraReadonlyConnectionStr =
      pgJDBCProtocol + pgAuroraClusterIdentifier + dbAuroraReadonlyConnectionStrSuffix;

  protected final String pgAuroraWriterConnectionStr = pgJDBCProtocol + pgAuroraClusterIdentifier
      + ".cluster-" + dbAuroraInstanceConnectionStrSuffix.substring(1);

  protected static final int TEST_CLUSTER_SIZE = 5;
  protected static String instanceID1 = "";
  protected static String instanceID2 = "";
  protected static String instanceID3 = "";
  protected static String instanceID4 = "";
  protected static String instanceID5 = "";
  protected static final int IS_VALID_TIMEOUT = 5;

  private static final String NO_SUCH_CLUSTER_MEMBER =
      "Cannot find cluster member whose db instance identifier is ";
  private static final String NO_WRITER_AVAILABLE =
      "Cannot get the id of the writer Instance in the cluster.";

  private final AmazonRDS rdsClient = AmazonRDSClientBuilder.standard().build();
  protected Connection testConnection;
  private final Logger logger = Logger.getLogger(FailoverIntegrationTest.class.getName());

  private CrashInstanceRunnable instanceCrasher1;
  private CrashInstanceRunnable instanceCrasher2;
  private CrashInstanceRunnable instanceCrasher3;
  private CrashInstanceRunnable instanceCrasher4;
  private CrashInstanceRunnable instanceCrasher5;

  private Map<String, CrashInstanceRunnable> instanceCrasherMap = new HashMap<>();
  private Map<String, String> ipToInstanceMap = new HashMap<>();

  public FailoverIntegrationTest() throws SQLException {
    DriverManager.registerDriver(new software.aws.rds.jdbc.postgresql.Driver());

    configureDriverLogger();

    initiateInstanceNames();

    instanceCrasher1 = new CrashInstanceRunnable(instanceID1);
    instanceCrasher2 = new CrashInstanceRunnable(instanceID2);
    instanceCrasher3 = new CrashInstanceRunnable(instanceID3);
    instanceCrasher4 = new CrashInstanceRunnable(instanceID4);
    instanceCrasher5 = new CrashInstanceRunnable(instanceID5);

    instanceCrasherMap.put(instanceID1, instanceCrasher1);
    instanceCrasherMap.put(instanceID2, instanceCrasher2);
    instanceCrasherMap.put(instanceID3, instanceCrasher3);
    instanceCrasherMap.put(instanceID4, instanceCrasher4);
    instanceCrasherMap.put(instanceID5, instanceCrasher5);

    // initializing ipToStringMap by creating connections to each instances and fetching their IPs
    initiateIpMap(instanceID1);
    initiateIpMap(instanceID2);
    initiateIpMap(instanceID3);
    initiateIpMap(instanceID4);
    initiateIpMap(instanceID5);
    logger.log(Level.INFO, ipToInstanceMap.toString());
  }

  private void configureDriverLogger() {
    Logger rootLogger = Logger.getLogger("");
    Handler[] handlers = rootLogger.getHandlers(); //this call indirectly creates a console handler for a root logger
    boolean foundConsoleHandler = false;
    for (java.util.logging.Handler handler : handlers) {
      if (handler instanceof ConsoleHandler) {
        handler.setLevel(Level.ALL); // for a sake of test, we want a handler to log everything
        foundConsoleHandler = true;
      }
    }

    if (!foundConsoleHandler) {
      ConsoleHandler ch = new ConsoleHandler();
      ch.setLevel(Level.ALL);
      rootLogger.addHandler(ch);
    }
  }

  private void initiateIpMap(String instanceID) throws SQLException {

    Connection connection = createConnectionToInstanceWithId(instanceID);

    try {
      Statement myStmt = connection.createStatement();
      ResultSet resultSet = myStmt.executeQuery("select inet_server_addr()");
      if (resultSet.next()) {
        ipToInstanceMap.put(resultSet.getString(1), instanceID);
        myStmt.close();
      }
      resultSet.close();
    } catch (SQLException e) {
      throw e;
    } finally {
      connection.close();
    }

  }

  private DBCluster getDBCluster(String dbClusterIdentifier) {
    DescribeDBClustersRequest dbClustersRequest =
        new DescribeDBClustersRequest().withDBClusterIdentifier(dbClusterIdentifier);
    DescribeDBClustersResult dbClustersResult = rdsClient.describeDBClusters(dbClustersRequest);
    List<DBCluster> dbClusterList = dbClustersResult.getDBClusters();
    return dbClusterList.get(0);
  }

  private void initiateInstanceNames() {
    logger.log(Level.INFO, "Initiating db instance names.");
    List<DBClusterMember> dbClusterMembers = getDBClusterMemberList();

    assertEquals(TEST_CLUSTER_SIZE, dbClusterMembers.size());
    instanceID1 = dbClusterMembers.get(0).getDBInstanceIdentifier();
    instanceID2 = dbClusterMembers.get(1).getDBInstanceIdentifier();
    instanceID3 = dbClusterMembers.get(2).getDBInstanceIdentifier();
    instanceID4 = dbClusterMembers.get(3).getDBInstanceIdentifier();
    instanceID5 = dbClusterMembers.get(4).getDBInstanceIdentifier();
  }

  private List<DBClusterMember> getDBClusterMemberList() {
    DBCluster dbCluster = getDBCluster(pgAuroraClusterIdentifier);
    return dbCluster.getDBClusterMembers();
  }

  private DBClusterMember getMatchedDBClusterMember(String instanceId) {
    List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList().stream()
            .filter(dbClusterMember -> dbClusterMember.getDBInstanceIdentifier().equals(instanceId))
            .collect(Collectors.toList());
    if (matchedMemberList.isEmpty()) {
      throw new RuntimeException(NO_SUCH_CLUSTER_MEMBER + instanceId);
    }
    return matchedMemberList.get(0);
  }

  protected String getDBClusterWriterInstanceId() {
    List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList().stream()
            .filter(DBClusterMember::isClusterWriter)
            .collect(Collectors.toList());
    if (matchedMemberList.isEmpty()) {
      throw new RuntimeException(NO_WRITER_AVAILABLE);
    }
    // Should be only one writer at index 0.
    return matchedMemberList.get(0).getDBInstanceIdentifier();
  }

  protected List<String> getDBClusterReaderInstanceIds() {
    List<DBClusterMember> matchedMemberList =
        getDBClusterMemberList().stream()
            .filter(dbClusterMember -> !dbClusterMember.isClusterWriter())
            .collect(Collectors.toList());
    return matchedMemberList.stream()
        .map(DBClusterMember::getDBInstanceIdentifier)
        .collect(Collectors.toList());
  }

  protected Boolean isDBInstanceWriter(String instanceId) {
    return getMatchedDBClusterMember(instanceId).isClusterWriter();
  }

  protected Boolean isDBInstanceReader(String instanceId) {
    return !getMatchedDBClusterMember(instanceId).isClusterWriter();
  }

  protected void failoverClusterAndWaitUntilWriterChanged(String clusterWriterId)
      throws InterruptedException {
    failoverCluster();
    waitUntilWriterInstanceChanged(clusterWriterId);
  }

  private void failoverCluster() throws InterruptedException {
    waitUntilClusterHasRightState();
    FailoverDBClusterRequest request =
        new FailoverDBClusterRequest().withDBClusterIdentifier(pgAuroraClusterIdentifier);

    while (true) {
      try {
        rdsClient.failoverDBCluster(request);
        break;
      } catch (Exception e) {
        Thread.sleep(1000);
      }
    }
  }

  protected void failoverClusterToATargetAndWaitUntilWriterChanged(
      String clusterWriterId, String targetInstanceId) throws InterruptedException {
    failoverClusterWithATargetInstance(targetInstanceId);
    waitUntilWriterInstanceChanged(clusterWriterId);
  }

  private void failoverClusterWithATargetInstance(String targetInstanceId)
      throws InterruptedException {
    logger.log(Level.INFO, "Failover cluster to " + targetInstanceId);
    waitUntilClusterHasRightState();
    FailoverDBClusterRequest request =
        new FailoverDBClusterRequest()
            .withDBClusterIdentifier(pgAuroraClusterIdentifier)
            .withTargetDBInstanceIdentifier(targetInstanceId);

    while (true) {
      try {
        rdsClient.failoverDBCluster(request);
        break;
      } catch (Exception e) {
        Thread.sleep(1000);
      }
    }

    logger.log(Level.INFO, "Cluster failover request successful.");
  }

  private void waitUntilWriterInstanceChanged(String initialWriterInstanceId)
      throws InterruptedException {
    logger.log(Level.INFO,"Wait until the writer is not " + initialWriterInstanceId + " anymore.");
    String nextClusterWriterId = getDBClusterWriterInstanceId();
    while (initialWriterInstanceId.equals(nextClusterWriterId)) {
      Thread.sleep(1000);
      // Calling the RDS API to get writer Id.
      nextClusterWriterId = getDBClusterWriterInstanceId();
    }

    logger.log(Level.INFO,"Writer is now " + nextClusterWriterId);
  }

  private void waitUntilClusterHasRightState() throws InterruptedException {
    logger.log(Level.INFO,"Wait until cluster is in available state.");
    String status = getDBCluster(pgAuroraClusterIdentifier).getStatus();
    while (!"available".equalsIgnoreCase(status)) {
      Thread.sleep(1000);
      status = getDBCluster(pgAuroraClusterIdentifier).getStatus();
    }
    logger.log(Level.INFO,"Cluster is available.");
  }

  protected Connection createConnectionToReadonlyClusterEndpoint() throws SQLException {
    return DriverManager.getConnection(pgAuroraReadonlyConnectionStr,
        pgAuroraUsername,
        pgAuroraPassword);
  }

  private Connection createConnectionToInstanceWithId(String instanceID) throws SQLException {
    return DriverManager.getConnection(
        pgJDBCProtocol + instanceID + dbAuroraInstanceConnectionStrSuffix, pgAuroraUsername, pgAuroraPassword);
  }

  private Connection createConnectionToInstanceWithId(String instanceID, Properties props) throws SQLException {
    return DriverManager.getConnection(
        pgJDBCProtocol + instanceID + dbAuroraInstanceConnectionStrSuffix, props);
  }

  private Connection createConnectionWithProxyDisabled(String instanceID) throws SQLException {
    return DriverManager.getConnection(
        pgJDBCProtocol + instanceID + dbAuroraInstanceConnectionStrSuffix + "?enableClusterAwareFailover=false",
        pgAuroraUsername,
        pgAuroraPassword);
  }

  protected Connection createPooledConnectionWithInstanceId(String instanceID) throws SQLException {
    BasicDataSource ds = new BasicDataSource();
    ds.setUrl(pgJDBCProtocol + instanceID + dbAuroraInstanceConnectionStrSuffix);
    ds.setUsername(pgAuroraUsername);
    ds.setPassword(pgAuroraPassword);
    ds.setMinIdle(connPoolMinIdle);
    ds.setMaxIdle(connPoolMaxIdle);
    ds.setMaxOpenPreparedStatements(connPoolMaxOpenPreparedStatements);

    return ds.getConnection();
  }

  protected Connection connectToReaderInstance(String readerInstanceId) throws SQLException {
    final Connection testConnection = createConnectionToInstanceWithId(readerInstanceId);
    testConnection.setReadOnly(true);
    assertTrue(isDBInstanceReader(queryInstanceId(testConnection)));
    return testConnection;
  }

  protected Connection connectToWriterInstance(String writerInstanceId) throws SQLException {
    final Connection testConnection = createConnectionToInstanceWithId(writerInstanceId);
    assertTrue(isDBInstanceWriter(queryInstanceId(testConnection)));
    return testConnection;
  }

  protected Connection connectToWriterInstance(String writerInstanceId, Properties props) throws SQLException {
    final Connection testConnection = createConnectionToInstanceWithId(writerInstanceId, props);
    assertTrue(isDBInstanceWriter(queryInstanceId(testConnection)));
    return testConnection;
  }

  protected String queryInstanceId(Connection connection) throws SQLException {
    try (Statement myStmt = connection.createStatement()) {
      return executeInstanceIdQuery(myStmt);
    }
  }

  protected String executeInstanceIdQuery(Statement stmt) throws SQLException {
    try (ResultSet rs = stmt.executeQuery("select inet_server_addr()")) {
      if (rs.next()) {
        String instance = ipToInstanceMap.get(rs.getString(1));
        stmt.close();
        rs.close();
        return instance;
      }
    }
    throw new SQLException();
  }

  protected String querySelect1(Connection connection) throws SQLException {
    try (Statement myStmt = connection.createStatement();
         ResultSet resultSet = myStmt.executeQuery("SELECT 1")
    ) {
      if (resultSet.next()) {
        return resultSet.getString(1);
      }
    }
    throw new SQLException();
  }
  // Attempt to run a query after the instance is down.
  // This should initiate the driver failover, first query after a failover
  // should always throw with the expected error message.

  protected void assertFirstQueryThrows(Connection connection, String expectedSQLErrorCode) {
    logger.log(Level.INFO,
        "Assert that the first read query throws, "
            + "this should kick off the driver failover process..");
    SQLException exception = assertThrows(SQLException.class, () -> queryInstanceId(connection));
    assertEquals(expectedSQLErrorCode, exception.getSQLState());
  }

  protected void assertFirstQueryThrows(Statement stmt, String expectedSQLErrorCode) {
    logger.log(Level.INFO,
            "Assert that the first read query throws, "
                    + "this should kick off the driver failover process..");
    SQLException exception = assertThrows(SQLException.class, () -> executeInstanceIdQuery(stmt));
    assertEquals(expectedSQLErrorCode, exception.getSQLState());
  }

  private void waitUntilFirstInstanceIsWriter() throws InterruptedException {
    logger.log(Level.INFO,"Failover cluster to Instance 1.");
    failoverClusterWithATargetInstance(instanceID1);
    String clusterWriterId = getDBClusterWriterInstanceId();

    logger.log(Level.INFO,"Wait until Instance 1 becomes the writer.");
    while (!instanceID1.equals(clusterWriterId)) {
      clusterWriterId = getDBClusterWriterInstanceId();
      logger.log(Level.INFO, "Writer is still " + clusterWriterId);
      Thread.sleep(1000);
    }
  }

  /**
   * Block until the specified instance is inaccessible.
   * */
  public void waitUntilInstanceIsDown(String instanceId) throws InterruptedException {
    logger.log(Level.INFO,"Wait until " + instanceId + " is down.");
    while (true) {
      try (Connection conn = createConnectionWithProxyDisabled(instanceId)) {
        // Continue waiting until instance is down.
      } catch (SQLException e) {
        break;
      }
      Thread.sleep(1000);
    }
  }

  /**
   * Block until the specified instance is accessible again.
   * */
  public void waitUntilInstanceIsUp(String instanceId) throws InterruptedException {
    logger.log(Level.INFO,"Wait until " + instanceId + " is up.");
    while (true) {
      try (Connection conn = createConnectionWithProxyDisabled(instanceId)) {
        conn.close();
        break;
      } catch (SQLException ex) {
        // Continue waiting until instance is up.
      }
      Thread.sleep(1000);
    }
    logger.log(Level.INFO,instanceId + " is up.");
  }

  @BeforeEach
  private void resetCluster() throws InterruptedException {
    logger.log(Level.INFO,"Resetting cluster.");
    waitUntilFirstInstanceIsWriter();
    waitUntilInstanceIsUp(instanceID1);
    waitUntilInstanceIsUp(instanceID2);
    waitUntilInstanceIsUp(instanceID3);
    waitUntilInstanceIsUp(instanceID4);
    waitUntilInstanceIsUp(instanceID5);
  }

  @AfterEach
  private void reviveInstancesAndCloseTestConnection() throws SQLException, InterruptedException {
    reviveAllInstances();
    testConnection.close();
    TimeUnit.SECONDS.sleep(2); // Wait for 2 seconds to avoid any connection errors on subsequent tests
  }

  private void reviveAllInstances() {
    logger.log(Level.INFO,"Revive all crashed instances in the test and wait until they are up.");
    instanceCrasherMap.forEach(
        (instanceId, instanceCrasher) -> {
          try {
            stopCrashingInstanceAndWaitUntilUp(instanceId);
          } catch (InterruptedException ex) {
            logger.log(Level.INFO,"Exception occurred while trying to stop crashing an instance.");
          }
        });
  }

  protected void stopCrashingInstanceAndWaitUntilUp(String instanceId)
      throws InterruptedException {
    logger.log(Level.INFO,"Stop crashing " + instanceId + ".");
    CrashInstanceRunnable instanceCrasher = instanceCrasherMap.get(instanceId);
    instanceCrasher.stopCrashingInstance();
    waitUntilInstanceIsUp(instanceId);
  }

  /**
   * Start crashing the specified instance and wait until its inaccessible.
   * */
  protected void startCrashingInstanceAndWaitUntilDown(String instanceId)
      throws InterruptedException {
    logger.log(Level.INFO,"Start crashing " + instanceId + ".");
    CrashInstanceRunnable instanceCrasher = instanceCrasherMap.get(instanceId);
    instanceCrasher.startCrashingInstance();
    Thread instanceCrasherThread = new Thread(instanceCrasher);
    instanceCrasherThread.start();
    waitUntilInstanceIsDown(instanceId);
  }

  /**
   * Runnable class implementation that is used to crash an instance.
   * */
  public class CrashInstanceRunnable implements Runnable {
    static final String CRASH_QUERY = "SELECT aurora_inject_crash('instance')";
    private final String instanceId;
    private boolean keepCrashingInstance = false;

    CrashInstanceRunnable(String instanceId) {
      logger.log(Level.INFO,"create runnable for " + instanceId);
      this.instanceId = instanceId;
    }

    public String getInstanceId() {
      return this.instanceId;
    }

    public synchronized void stopCrashingInstance() {
      this.keepCrashingInstance = false;
    }

    public synchronized void startCrashingInstance() {
      this.keepCrashingInstance = true;
    }

    @Override
    public void run() {
      while (keepCrashingInstance) {
        try (Connection conn = createConnectionWithProxyDisabled(instanceId);
            Statement myStmt = conn.createStatement()
        ) {
          myStmt.execute(CRASH_QUERY);
        } catch (SQLException e) {
          // Do nothing and keep creating connection to crash instance.
        }
      }
      // Run the garbage collector in the Java Virtual Machine to abandon thread.
      System.gc();
    }
  }

}
