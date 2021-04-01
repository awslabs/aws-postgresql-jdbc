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
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

  protected final String pgHostInstancePattern = "%s" + pgAuroraInstanceDnsSuffix;

  protected int clusterSize = 0;
  protected String[] instanceIDs; // index 0 is always writer!
  protected final HashSet<String> instancesToCrash = new HashSet<>();
  protected ExecutorService crashInstancesExecutorService;

  protected static final int IS_VALID_TIMEOUT = 5;
  protected static final String SOCKET_TIMEOUT_VAL = "1"; //sec
  protected static final String CONNECT_TIMEOUT_VAL = "3"; //sec

  private static final String NO_SUCH_CLUSTER_MEMBER =
      "Cannot find cluster member whose db instance identifier is ";
  private static final String NO_WRITER_AVAILABLE =
      "Cannot get the id of the writer Instance in the cluster.";

  private final AmazonRDS rdsClient = AmazonRDSClientBuilder.standard().build();
  protected Connection testConnection;
  protected final Logger logger = Logger.getLogger(FailoverIntegrationTest.class.getName());
  private Map<String, String> ipToInstanceMap = new HashMap<>();

  public FailoverIntegrationTest() throws SQLException {
    DriverManager.registerDriver(new software.aws.rds.jdbc.postgresql.Driver());

    configureDriverLogger();

    initiateInstanceNames();

    // initializing ipToStringMap by creating connections to each instances and fetching their IPs
    for (String id : instanceIDs) {
      initiateIpMap(id);
    }

    StringBuilder sb = new StringBuilder();
    sb.append("Cluster Instance IP addresses: \n");
    for (String ip : ipToInstanceMap.keySet()) {
      sb.append(ip + " -> " + ipToInstanceMap.get(ip) + (instanceIDs[0].equals(ipToInstanceMap.get(ip)) ? " (WRITER)" : "") + "\n");
    }
    logger.log(Level.INFO, sb.toString());
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

    Connection connection = createConnectionWithProxyDisabled(instanceID);

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

    clusterSize = dbClusterMembers.size();
    assertTrue(clusterSize >= 2); // many tests assume that cluster contains at least a writer and a reader

    instanceIDs = dbClusterMembers.stream()
        .sorted(Comparator.comparing((DBClusterMember x) -> !x.isClusterWriter())
                .thenComparing((DBClusterMember x) -> x.getDBInstanceIdentifier()))
        .map((DBClusterMember m) -> m.getDBInstanceIdentifier())
        .toArray(String[]::new);
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

  protected Connection createConnectionToReadonlyClusterEndpoint(Properties props) throws SQLException {
    return DriverManager.getConnection(pgAuroraReadonlyConnectionStr, props);
  }

  private Connection createConnectionToInstanceWithId(String instanceID) throws SQLException {
    return DriverManager.getConnection(
        pgJDBCProtocol + instanceID + dbAuroraInstanceConnectionStrSuffix, pgAuroraUsername, pgAuroraPassword);
  }

  protected Connection createConnectionToInstanceWithId(String instanceID, Properties props) throws SQLException {
    return DriverManager.getConnection(
        pgJDBCProtocol + instanceID + dbAuroraInstanceConnectionStrSuffix, props);
  }

  private Connection createConnectionWithProxyDisabled(String instanceID) throws SQLException {
    return DriverManager.getConnection(
        pgJDBCProtocol + instanceID + dbAuroraInstanceConnectionStrSuffix + "?enableClusterAwareFailover=false",
        pgAuroraUsername,
        pgAuroraPassword);
  }

  private Connection createCrashConnection(String instanceID) throws SQLException {
    final Properties props = new Properties();
    props.setProperty("user", pgAuroraUsername);
    props.setProperty("password", pgAuroraPassword);
    props.setProperty("enableClusterAwareFailover", "false");
    props.setProperty("connectTimeout", "2");
    props.setProperty("socketTimeout", "2");
    props.setProperty("loginTimeout", "3");

    return DriverManager.getConnection(pgJDBCProtocol + instanceID + dbAuroraInstanceConnectionStrSuffix, props);
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

  protected Connection connectToReaderInstance(String readerInstanceId, Properties props) throws SQLException {
    final Connection testConnection = createConnectionToInstanceWithId(readerInstanceId, props);
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

    try (Statement myStmt = connection.createStatement();
         ResultSet resultSet = myStmt.executeQuery("select inet_server_addr()")
    ) {
      if (resultSet.next()) {
        String instance = ipToInstanceMap.get(resultSet.getString(1));
        myStmt.close();
        resultSet.close();
        return instance;
      }
    }
    return null;
  }

  protected String executeInstanceIdQuery(Statement stmt) throws SQLException {
    try (ResultSet rs = stmt.executeQuery("select inet_server_addr()")) {
      if (rs.next()) {
        String instance = ipToInstanceMap.get(rs.getString(1));
        return instance;
      }
    }
    throw null;
  }

  protected String querySelect1(Connection connection) throws SQLException {
    try (Statement myStmt = connection.createStatement();
         ResultSet resultSet = myStmt.executeQuery("SELECT 1")
    ) {
      if (resultSet.next()) {
        return resultSet.getString(1);
      }
    }
    return null;
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

  @BeforeEach
  private void validateCluster() throws InterruptedException {
    logger.log(Level.INFO,"Validating cluster.");

    crashInstancesExecutorService = Executors.newFixedThreadPool(instanceIDs.length);
    instancesToCrash.clear();
    for (String id : instanceIDs) {
      crashInstancesExecutorService.submit(() -> {
        while (true) {
          if (instancesToCrash.contains(id)) {
            try (Connection conn = createCrashConnection(id);
                 Statement myStmt = conn.createStatement()
            ) {
              myStmt.execute("SELECT aurora_inject_crash('instance')");
            } catch (SQLException e) {
              // Do nothing and keep creating connection to crash instance.
            }
          }
          Thread.sleep(100);
        }
      });
    }
    crashInstancesExecutorService.shutdown();

    makeSureInstancesUp(instanceIDs);
    FailoverSocketFactory.flushAllStaticData();

    logger.log(Level.INFO,"===================== Pre-Test init is done. Ready for test =====================");
  }

  @AfterEach
  private void reviveInstancesAndCloseTestConnection() throws SQLException, InterruptedException {
    logger.log(Level.INFO,"===================== Test is over. Post-Test clean-up is below. =====================");

    try {
      testConnection.close();
    } catch (Exception ex) {
      // ignore
    }
    testConnection = null;

    instancesToCrash.clear();
    crashInstancesExecutorService.shutdownNow();

    makeSureInstancesUp(instanceIDs, false);
  }

  protected void startCrashingInstances(String... instances) {
    instancesToCrash.addAll(Arrays.asList(instances));
  }

  protected void stopCrashingInstances(String... instances) {
    instancesToCrash.removeAll(Arrays.asList(instances));
  }

  protected void makeSureInstancesUp(String... instances) throws InterruptedException {
    makeSureInstancesUp(instances, true);
  }

  protected void makeSureInstancesUp(String[] instances, boolean finalCheck) throws InterruptedException {
    logger.log(Level.INFO,"Wait until the following instances are up: \n" + String.join("\n", instances));
    ExecutorService executorService = Executors.newFixedThreadPool(instances.length);
    final HashSet<String> remainingInstances = new HashSet<String>();
    remainingInstances.addAll(Arrays.asList(instances));

    for (String id : instances) {
      executorService.submit(() -> {
        while (true) {
          try (Connection conn = createCrashConnection(id)) {
            conn.close();
            remainingInstances.remove(id);
            break;
          } catch (SQLException ex) {
            // Continue waiting until instance is up.
          }
          Thread.sleep(500);
        }
        return null;
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(120, TimeUnit.SECONDS);

    if (finalCheck) {
      assertTrue(remainingInstances.isEmpty(), "The following instances are still down: \n"
              + String.join("\n", remainingInstances));
    }

    logger.log(Level.INFO,"The following instances are up: \n" + String.join("\n", instances));
  }

  protected void makeSureInstancesDown(String... instances) throws InterruptedException {
    makeSureInstancesDown(instances, true);
  }

  protected void makeSureInstancesDown(String[] instances, boolean finalCheck) throws InterruptedException {
    logger.log(Level.INFO,"Wait until the following instances are down: \n" + String.join("\n", instances));
    ExecutorService executorService = Executors.newFixedThreadPool(instances.length);
    final HashSet<String> remainingInstances = new HashSet<String>();
    remainingInstances.addAll(Arrays.asList(instances));

    for (String id : instances) {
      executorService.submit(() -> {
        while (true) {
          try (Connection conn = createCrashConnection(id)) {
            // Continue waiting until instance is down.
          } catch (SQLException e) {
            remainingInstances.remove(id);
            break;
          }
          Thread.sleep(500);
        }
        return null;
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(120, TimeUnit.SECONDS);

    if (finalCheck) {
      assertTrue(remainingInstances.isEmpty(), "The following instances are still up: \n"
              + String.join("\n", remainingInstances));
    }

    logger.log(Level.INFO,"The following instances are down: \n" + String.join("\n", instances));
  }
}
