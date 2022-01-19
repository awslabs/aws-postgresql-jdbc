# Amazon Web Services (AWS) JDBC Driver for PostgreSQL

[![Build Status](https://github.com/awslabs/aws-postgresql-jdbc/workflows/CI/badge.svg?kill_cache=1)](https://github.com/awslabs/aws-postgresql-jdbc/actions?query=workflow%3A%22CI%22)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.aws.rds/aws-postgresql-jdbc/badge.svg?kill_cache=1)](https://maven-badges.herokuapp.com/maven-central/software.aws.rds/aws-postgresql-jdbc)
[![Javadoc](https://javadoc.io/badge2/software.aws.rds/aws-postgresql-jdbc/javadoc.svg?kill_cache=1)](https://javadoc.io/doc/software.aws.rds/aws-postgresql-jdbc)
[![License](https://img.shields.io/badge/License-BSD--2--Clause-blue.svg)](https://opensource.org/licenses/BSD-2-Clause)

**The Amazon Web Services (AWS) JDBC Driver for PostgreSQL** allows an application to take advantage of the features of clustered PostgreSQL databases. It is based on and is drop-in compatible with the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc), and is compatible with all PostgreSQL deployments.

The AWS JDBC Driver for PostgreSQL supports fast failover for Amazon Aurora with PostgreSQL compatibility. Support for additional features of clustered databases, including features of Amazon RDS for PostgreSQL and on-premises PostgreSQL deployments, is planned.

> **IMPORTANT** Because this project is in preview, we encourage you to experiment with the PostgreSQL driver but DO NOT recommend adopting it for production use. Use of the PostgreSQL driver in preview is subject to the terms and conditions contained in the [AWS Service Terms](https://aws.amazon.com/service-terms), (particularly the Beta Service Participation Service Terms) This applies to any drivers not marked as 'Generally Available'.

## What is Failover?

In an Amazon Aurora DB cluster, failover is a mechanism by which Aurora automatically repairs the DB cluster status when a primary DB instance becomes unavailable. It achieves this goal by electing an Aurora Replica to become the new primary DB instance, so that the DB cluster can provide maximum availability to a primary read-write DB instance. The AWS JDBC Driver for PostgreSQL is designed to coordinate with this behavior in order to provide minimal downtime in the event of a DB instance failure.

## Benefits of the AWS JDBC Driver for PostgreSQL

Although Aurora is able to provide maximum availability through the use of failover, existing client drivers do not currently support this functionality. This is partially due to the time required for the DNS of the new primary DB instance to be fully resolved in order to properly direct the connection. The AWS JDBC Driver for PostgreSQL fully utilizes failover behavior by maintaining a cache of the Aurora cluster topology and each DB instance's role (Aurora Replica or primary DB instance). This topology is provided via a direct query to the Aurora database, essentially providing a shortcut to bypass the delays caused by DNS resolution. With this knowledge, the AWS JDBC Driver can more closely monitor the Aurora DB cluster status so that a connection to the new primary DB instance can be established as fast as possible. Additionally, as noted above, the AWS JDBC Driver is designed to be a drop-in compatible for other PostgreSQL JDBC drivers and can be used to interact with regular RDS and PostgreSQL databases as well as Aurora PostgreSQL.

## The AWS JDBC Driver Failover Process

<div style="text-align:center"><img src="./docs/files/images/failover_diagram.png" /></div>

The figure above provides a simplified overview of how the AWS JDBC Driver handles an Aurora failover encounter. Starting at the top of the diagram, an application with the AWS JDBC Driver on its class path uses the driver to get a logical connection to an Aurora database. In this example, the application requests a connection using the Aurora DB cluster endpoint and is returned a logical connection that is physically connected to the primary DB instance in the DB cluster, DB instance C. Due to how the application operates against the logical connection, the physical connection details about which specific DB instance it is connected to have been abstracted away. Over the course of the application's lifetime, it executes various statements against the logical connection. If DB instance C is stable and active, these statements succeed and the application continues as normal. If DB instance C later experiences a failure, Aurora will initiate failover to promote a new primary DB instance. At the same time, the AWS JDBC Driver will intercept the related communication exception and kick off its own internal failover process. In this case, in which the primary DB instance has failed, the driver will use its internal topology cache to temporarily connect to an active Aurora Replica. This Aurora Replica will be periodically queried for the DB cluster topology until the new primary DB instance is identified (DB instance A or B in this case). At this point, the driver will connect to the new primary DB instance and return control to the application by raising a SQLException with SQLState 08S02 so that they can reconfigure their session state as required. Although the DNS endpoint for the DB cluster might not yet resolve to the new primary DB instance, the driver has already discovered this new DB instance during its failover process and will be directly connected to it when the application continues executing statements. In this way the driver provides a faster way to reconnect to a newly promoted DB instance, thus increasing the availability of the DB cluster.

## Getting Started

### Minimum Requirements
You need to install Amazon Corretto 8+ or Java 8+ before using the AWS JDBC Driver for PostgreSQL.

### Obtaining the AWS JDBC Driver for PostgreSQL

#### Direct Download
The AWS JDBC Driver for PostgreSQL can be installed from pre-compiled packages that can be downloaded directly from [GitHub Releases](https://github.com/awslabs/aws-postgresql-jdbc/releases) or [Maven Central](https://search.maven.org/search?q=g:software.aws.rds). To install the driver, obtain the corresponding JAR file and include it in the application's CLASSPATH.

**Example - Direct Download via wget**
```bash
wget https://github.com/awslabs/aws-postgresql-jdbc/releases/download/0.1.0/aws-postgresql-jdbc-0.2.0.jar
```

**Example - Adding the Driver to the CLASSPATH**
```bash
export CLASSPATH=$CLASSPATH:/home/userx/libs/aws-postgresql-jdbc-0.2.0.jar
```

#### As a Maven Dependency
You can use [Maven's dependency management](https://search.maven.org/search?q=g:software.aws.rds) to obtain the driver by adding the following configuration in the application's Project Object Model (POM) file:

**Example - Maven**
```xml
<dependencies>
  <dependency>
    <groupId>software.aws.rds</groupId>
    <artifactId>aws-postgresql-jdbc</artifactId>
    <version>0.1.0</version>
  </dependency>
</dependencies>
```
#### As a Gradle Dependency
You can use [Gradle's dependency management](https://search.maven.org/search?q=g:software.aws.rds) to obtain the driver by adding the following configuration in the application's ```build.gradle``` file:

**Example - Gradle**
```gradle
dependencies {
    compile group: 'software.aws.rds', name: 'aws-postgresql-jdbc', version: '0.1.0'
}
```
### Using the AWS JDBC Driver for PostgreSQL
The AWS JDBC Driver for PostgreSQL is drop-in compatible, so usage is identical to the [PostgreSQL JDBC Driver](https://github.com/pgjdbc/pgjdbc). The sections below highlight usage specific to failover.

#### Driver Name
Use the driver name: ```software.aws.rds.jdbc.postgresql.Driver```. You will need this name when loading the driver explicitly to the driver manager.

#### Driver Protocol
Currently, the driver only supports the following protocol for the connection string:
`jdbc:postgresql:aws:`. The driver does not support any other protocols to avoid potential conflicts with other PostgreSQL JDBC drivers. 

#### Connection URL Descriptions

There are many different types of URLs that can connect to an Aurora DB cluster; this section outlines the various URL types. For some URL types, the AWS JDBC Driver requires the user to provide some information about the Aurora DB cluster to provide failover functionality.  For each URL type, information is provided below about how the driver will behave and what information the driver requires about the DB cluster, if applicable.

Note: The connection string follows standard URL parameters. To add parameters to the connection string, simply add `?` and then the `parameter_name=value` pair at the end of the connection string. You can add multiple parameters by separating the parameter name and value pair (`parameter_name=value`) with the `&` symbol. For example, to add 2 parameters simply add `?param_name=value&param_2=value2` at the end of the connection string.
 

| URL Type      | Example         | Required Parameters | Driver Behavior |
| :------------ | :-------------: | :-----------------: | :-------------- |
| Cluster Endpoint      | `jdbc:postgresql:aws://db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com:5432` | None | *Initial connection:* primary DB instance<br/>*Failover behavior:* connect to the new primary DB instance |
| Read-Only Cluster Endpoint      | `jdbc:postgresql:aws://db-identifier.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:5432`      |   None |  *Initial connection:* any Aurora Replica<br/>*Failover behavior:* prioritize connecting to any active Aurora Replica but might connect to the primary DB instance if it provides a faster connection|
| Instance Endpoint | `jdbc:postgresql:aws://instance-1.XYZ.us-east-2.rds.amazonaws.com:5432`      |    None | *Initial connection:* the instance specified (DB instance 1)<br/>*Failover behavior:* connect to the primary DB instance|
| RDS Custom Cluster | `jdbc:postgresql:aws://db-identifier.cluster-custom-XYZ.us-east-2l.rds.amazonaws.com:5432`      |    None | *Initial connection:* any DB instance in the custom DB cluster<br/>*Failover behavior:* connect to the primary DB instance (note that this might be outside of the custom DB cluster) |
| IP Address | `jdbc:postgresql:aws://10.10.10.10:5432`      |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |
| Custom Domain | `jdbc:postgresql:aws://my-custom-domain.com:5432`      |    `clusterInstanceHostPattern` | *Initial connection:* the DB instance specified<br/>*Failover behavior:* connect to the primary DB instance |
| Non-Aurora Endpoint | `jdbc:postgresql:aws://localhost:5432`     |    None | A regular JDBC connection will be returned - no failover functionality |

Information about the `clusterInstanceHostPattern` is provided in the section below.

For more information about parameters that can be configured with the AWS JDBC Driver, see the section below about failover parameters.

#### Failover Parameters

In addition to [the parameters that you can configure for the PostgreSQL JDBC Driver](https://jdbc.postgresql.org/documentation/head/connect.html), you can pass the following parameters to the AWS JDBC Driver through the connection URL to specify additional driver behavior.

| Parameter    | Value    | Required      | Description |
| :----------: | :------: |:-------------:| :---------- |
|`enableClusterAwareFailover` | Boolean | No | Set to true to enable the fast failover behavior offerred by the AWS JDBC Driver. Set to false for simple JDBC connections that do not require fast failover functionality.<br/><br/>**Default value:** `true` |
|`clusterInstanceHostPattern` | String | If connecting using an IP address or custom domain URL: Yes<br/>Otherwise: No | This parameter is not required unless connecting to an AWS RDS cluster via an IP address or custom domain URL. In those cases, this parameter specifies the cluster instance DNS pattern that will be used to build a complete instance endpoint. A "?" character in this pattern should be used as a placeholder for the DB instance identifiers of the instances in the cluster. <br/><br/>Example: `?.my-domain.com`, `any-subdomain.?.my-domain.com:9999`<br/><br/>Usecase Example: If your cluster instance endpoints followed this pattern:`instanceIdentifier1.customHost.com`, `instanceIdentifier2.customHost.com`, etc. and you wanted your initial connection to be `customHost.com:1234`, then your connection string should look something like this: `jdbc:postgresql:aws://customHost.com:1234/test?clusterInstanceHostPattern=?.customHost.com`<br/><br/>**Default value:** if unspecified, and the provided connection string is not an IP address or custom domain, the driver will automatically acquire the cluster instance host pattern from the customer-provided connection string. |
|`clusterId` | String | No | A unique identifier for the cluster. Connections with the same cluster id share a cluster topology cache. This connection parameter is not required and thus should only be set if desired. <br/><br/>**Default value:** If unspecified, the driver will automatically acquire a cluster id for AWS RDS clusters. |
|`clusterTopologyRefreshRateMs` | Integer | No | Cluster topology refresh rate in milliseconds. The cached topology for the cluster will be invalidated after the specified time, after which it will be updated during the next interaction with the connection.<br/><br/>**Default value:** `30000` |
|`failoverTimeoutMs` | Integer | No | Maximum allowed time in milliseconds to attempt reconnecting to a new writer or reader instance after a cluster failover is initiated.<br/><br/>**Default value:** `60000` |
|`failoverClusterTopologyRefreshRateMs` | Integer | No | Cluster topology refresh rate in milliseconds during a writer failover process. During the writer failover process, cluster topology may be refreshed at a faster pace than normal to speed up discovery of the newly promoted writer.<br/><br/>**Default value:** `5000` |
|`failoverWriterReconnectIntervalMs` | Integer | No | Interval of time in milliseconds to wait between attempts to reconnect to a failed writer during a writer failover process.<br/><br/>**Default value:** `5000` |
|`failoverReaderConnectTimeoutMs` | Integer | No | Maximum allowed time in milliseconds to attempt to connect to a reader instance during a reader failover process. <br/><br/>**Default value:** `5000`
|`gatherPerfMetrics` | Boolean | No | Set to true if you would like the driver to record failover-associated metrics, which will then be logged upon closing the connection. <br/><br/>**Default value:** `false` | 
#### Failover Exception Codes
##### 08001 - Unable to Establish SQL Connection
When the driver throws a SQLException with code ```08001```, the original connection has failed, and the driver tried to failover to a new instance, but was unable to. There are various reasons this may happen: no nodes were available, a network failure occurred, and so on. In this scenario, please wait until the server is up or other problems are solved. (Exception will be thrown.)

##### 08S02 - Communication Link 
When the driver throws a SQLException with code ```08S02```, the original connection has failed while autocommit was set to true, and the driver successfully failed over to another available instance in the cluster. However, any session state configuration of the initial connection is now lost. In this scenario, the user should:

- Reuse and reconfigure the original connection (e.g., reconfigure session state to be the same as the original connection).

- Repeat that query which was executed when the connection failed and continue work as desired.

###### Sample Code
```java
import java.sql.*;

/**
 * Scenario 1: Failover happens when autocommit is set to true - Catch SQLException with code 08S02.
 */
public class FailoverSampleApp1 {
  private static final String CONNECTION_STRING = "jdbc:postgresql:aws://database-postgresql.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/myDb";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final int MAX_RETRIES = 5;

  public static void main(String[] args) throws SQLException {
    // Create a connection.
    try(Connection conn = DriverManager.getConnection(CONNECTION_STRING, USERNAME, PASSWORD)) {
      // Configure the connection.
      setInitialSessionState(conn);
   
      // Do something with method "betterExecuteQuery" using the Cluster-Aware Driver.
      String select_sql = "SELECT * FROM employees";
      try(ResultSet rs = betterExecuteQuery(conn, select_sql)) {
        while (rs.next()) {
          System.out.println(rs.getString("name"));
        }
      }
    }
  }

  private static void setInitialSessionState(Connection conn) throws SQLException {
    // Your code here for the initial connection setup.
    try(Statement stmt1 = conn.createStatement()) {
      stmt1.executeUpdate("SET timezone TO \"+00:00\"");
    }
  }
  
  // A better executing query method when autocommit is set as the default value - True.
  private static ResultSet betterExecuteQuery(Connection conn, String query) throws SQLException {
    // Create a boolean flag.
    boolean isSuccess = false;
    // Record the times of re-try.
    int retries = 0;
    
    ResultSet rs = null;
    while (!isSuccess) {
      try {
        Statement stmt = conn.createStatement();
        rs = stmt.executeQuery(query);
        isSuccess = true;
    
      } catch (SQLException e) {
    
        // If the attempt to connect has failed MAX_RETRIES times,
        // throw the exception to inform users of the failed connection.
        if (retries > MAX_RETRIES) {
          throw e;
        }
    
        // Failover has occurred and the driver has failed over to another instance successfully.
        if (e.getSQLState().equalsIgnoreCase("08S02")) {
          // Reconfigure the connection.
          setInitialSessionState(conn);
          // Re-execute that query again.
          retries++;
  
        } else {
          // If some other exception occurs, throw the exception.
          throw e;
        }
      }
    }
    
    // return the ResultSet successfully.
    return rs;
  }
}
```

##### 08007 - Transaction Resolution Unknown
When the driver throws a SQLException with code ```08007```, the original connection has failed within a transaction (while autocommit was set to false). In this scenario, the driver first attempts to rollback the transaction and then fails over to another available instance in the cluster. Note that the rollback might be unsuccessful as the initial connection may be broken at the time that the driver recognizes the problem. Note also that any session state configuration of the initial connection is now lost. In this scenario, you should:

- Reuse and reconfigure the original connection (e.g: reconfigure session state to be the same as the original connection).

- Restart the transaction and repeat all queries which were executed during the transaction before the connection failed.

- Repeat that query which was executed when the connection failed and continue work as desired.

###### Sample Code
```java
import java.sql.*;

/**
 * Scenario 2: Failover happens when autocommit is set to false - Catch SQLException with code 08007.
 */
public class FailoverSampleApp2 {
  private static final String CONNECTION_STRING = "jdbc:postgresql:aws://database-postgresql.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/myDb";
  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";
  private static final int MAX_RETRIES = 5;

  public static void main(String[] args) throws SQLException {
    // Create a connection
    try(Connection conn = DriverManager.getConnection(CONNECTION_STRING, USERNAME, PASSWORD)) {
      // Configure the connection - set autocommit to false.
      setInitialSessionState(conn);
  
      // Do something with method "betterExecuteUpdate_setAutoCommitFalse" using the Cluster-Aware Driver.
      String[] update_sql = new String[3];
      // Add all queries that you want to execute inside a transaction.
      update_sql[0] = "INSERT INTO employees(name, position, salary) VALUES('john', 'developer', 2000)";
      update_sql[1] = "INSERT INTO employees(name, position, salary) VALUES('mary', 'manager', 2005)";
      update_sql[2] = "INSERT INTO employees(name, position, salary) VALUES('Tom', 'accountant', 2019)";
      betterExecuteUpdate_setAutoCommitFalse(conn, update_sql);
    }
  }

  private static void setInitialSessionState(Connection conn) throws SQLException {
    // Your code here for the initial connection setup.
    try(Statement stmt1 = conn.createStatement()) {
      stmt1.executeUpdate("SET timezone TO \"+00:00\"");
    }
    conn.setAutoCommit(false);
  }

  // A better executing query method when autocommit is set to False.
  private static void betterExecuteUpdate_setAutoCommitFalse(Connection conn, String[] queriesInTransaction) throws SQLException {
    // Create a boolean flag.
    boolean isSuccess = false;
    // Record the times of re-try.
    int retries = 0;

    while (!isSuccess) {
      try(Statement stmt = conn.createStatement()) {
        for(String sql: queriesInTransaction){
          stmt.executeUpdate(sql);
        }
        conn.commit();
        isSuccess = true;
      } catch (SQLException e) {

        // If the attempt to connect has failed MAX_RETRIES times,
        // rollback the transaction and throw the exception to inform users of the failed connection.
        if (retries > MAX_RETRIES) {
          conn.rollback();
          throw e;
        }

        // Failure happens within the transaction and the driver failed over to another instance successfully.
        if (e.getSQLState().equalsIgnoreCase("08007")) {
          // Reconfigure the connection, restart the transaction.
          setInitialSessionState(conn);
          // Re-execute every queries that were inside the transaction.
          retries++;

        } else {
          // If some other exception occurs, rollback the transaction and throw the exception.
          conn.rollback();
          throw e;
        }
      } 
    }
  }
}
```

>### :warning: Warnings About Proper Usage of the AWS JDBC Driver for PostgreSQL
>1. A common practice when using JDBC drivers is to wrap invocations against a Connection object in a try-catch block, and dispose of the Connection object if an Exception is hit. If this practice is left unaltered, the application will lose the fast-failover functionality offered by the Driver. When failover occurs, the Driver internally establishes a ready-to-use connection inside the original Connection object before throwing an exception to the user. If this Connection object is disposed of, the newly established connection will be thrown away. The correct practice is to check the SQL error code of the exception and reuse the Connection object if the error code indicates successful failover. [FailoverSampleApp1](#sample-code) and [FailoverSampleApp2](#sample-code-1) demonstrate this practice. See the section below on [Failover Exception Codes](#failover-exception-codes) for more details.
>2. It is highly recommended that you use the cluster and read-only cluster endpoints instead of the direct instance endpoints of your Aurora cluster, unless you are confident about your application's use of instance endpoints. Although the Driver will correctly failover to the new writer instance when using instance endpoints, use of these endpoints is discouraged because individual instances can spontaneously change reader/writer status when failover occurs. The driver will always connect directly to the instance specified if an instance endpoint is provided, so a write-safe connection cannot be assumed if the application uses instance endpoints.

### AWS IAM Database Authentication

The driver supports Amazon AWS Identity and Access Management (IAM) authentication. When using AWS IAM database authentication, host URL must be a valid Amazon endpoint, and not a custom domain or an IP address (for example, `database-PostgreSQL-name.cluster-XYZ.us-east-2.rds.amazonaws.com`).

AWS IAM database authentication is limited to certain database engines. 
For more information on limitations and recommendations, please refer to [IAM database authentication for PostgreSQL and PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html).

#### Setup for IAM Database Authentication for PostgreSQL
1. Turn on AWS IAM database authentication for the existing database or create a new database on AWS RDS Console.
   1.  For information about creating a new database, see [the documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_CreateDBInstance.html).
   2.  For information about modifying an existing database, see [the documentation](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Overview.DBInstance.Modifying.html). 
2. To allow an AWS IAM user or role to connect to the DB instance, they must have sufficient permissions.
   See [Creating and using an IAM policy for IAM database access](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.IAMPolicy.html).
3. To use AWS IAM database authentication with PostgreSQL, create a database user and grant them the `rds_iam` role as follows:
    ```
    CREATE USER db_userx;
    GRANT rds_iam TO db_userx;
    ```
For more information, please refer to [Creating a database account using IAM authentication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.DBAccounts.html#UsingWithRDS.IAMDBAuth.DBAccounts.PostgreSQL).

#### Using AWS IAM Database Authentication
| Parameter       | Value           | Default Value      | Description  |
| ------------- |:-------------:|:-------------:| ----- |
|`useAwsIam` | Boolean | `false` | Set to `true` to use AWS IAM database authentication. |

##### Sample Code
```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import software.aws.rds.jdbc.PostgreSQL.shading.com.PostgreSQL.cj.conf.PropertyKey;
import software.aws.rds.jdbc.PostgreSQL.Driver;

public class AwsIamAuthenticationSample {
  private static final String CONNECTION_STRING = "jdbc:postgresql:aws://databa©se-PostgreSQL-name.cluster-XYZ.us-east-2.rds.amazonaws.com:5432/postgres";
  private static final String USER = "example_user_name";

  public static void main(String[] args) throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), USERNAME);
    properties.setProperty(PGProperty.USE_AWS_IAM.getName(), Boolean.TRUE.toString());

    try (Connection conn = DriverManager.getConnection(CONNECTION_STRING, properties)) {
      try (Statement stmt1 = conn.createStatement()) {
        try (ResultSet rs = stmt1.executeQuery("SELECT NOW()")) {
          while (rs.next()) {
            System.out.println(rs.getTimestamp(1));
          }
        }
      }
    }
  }
}
```

## Development

### Setup

After installing Amazon Corretto or Java as directed in the prerequisites section, use the following command to clone the driver repository:

```bash
$ git clone https://github.com/awslabs/aws-postgresql-jdbc.git
$ cd aws-postgresql-jdbc
```

You can now make changes in the repository.

### Building the AWS JDBC Driver for PostgreSQL

To build the AWS JDBC Driver without running the tests, navigate into the aws-postgresql-jdbc directory and run the following command:

```bash
gradlew build -x test
```

To build the driver and run the tests, you must first install [Docker](https://docs.docker.com/get-docker/). After installing Docker, use the following commands to create the Docker servers that the tests will run against:

```bash
$ cd aws-postgresql-jdbc/docker
$ docker-compose up -d
$ cd ../
```

Then, to build the driver, run the following command:

```bash
gradlew build
```

### Running the Tests

After building the driver, and installing and configuring Docker, you can run the tests in the ```aws-postgresql-jdbc``` directory with the following command:

```bash
gradlew test
```

To shut down the Docker servers when you've finished testing:

```bash
$ cd aws-postgresql-jdbc/docker
$ docker-compose down && docker-compose rm
$ cd ../
```

## Getting Help and Opening Issues

If you encounter a bug with the AWS JDBC Driver for PostgreSQL, we would like to hear about it. Please search the [existing issues](https://github.com/awslabs/aws-postgresql-jdbc/issues) to see if others are also experiencing the issue before opening a new issue. When opening a new issue, we will need the version of AWS JDBC Driver for PostgreSQL, Java language version, OS you’re using, and the PostgreSQL database version you're running against. Please include a reproduction case for the issue when appropriate.

The GitHub issues are intended for bug reports and feature requests. Keeping the list of open issues lean will help us respond in a timely manner.

## Documentation

For additional documentation about the AWS JDBC Driver, [please refer to the documentation for the open-source postgresql-connector-j driver that the AWS JDBC Driver was based on](https://jdbc.postgresql.org/documentation/documentation.html).

## License

This software is released under the BSD-2-Clause License.
