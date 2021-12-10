# Changelog

## [Version 0.2.0 (Public Preview)](https://github.com/awslabs/aws-postgresql-jdbc/releases/tag/0.2.0) - 2021-12-10

### Added
  * AWS IAM Authentication is now supported. Usage instructions can be found [here](https://github.com/awslabs/aws-postgresql-jdbc#aws-iam-database-authentication).

### Improvements
  * Socket timeout is set after initial connection.

### Breaking Changes
  * The driver now only accepts the AWS protocol (jdbc:postgresql:aws:) to avoid any conflicts with the community PostgreSQL JDBC driver. Please update your JDBC connection string to use the AWS protocol.

## [Version 0.1.0 (Public Preview)](https://github.com/awslabs/aws-postgresql-jdbc/releases/tag/0.1.0) - 2021-04-26

Based on the PostgreSQL JDBC 42.2.19 community driver.

### Features
  * The driver is cluster aware for Amazon Aurora PostgreSQL. It takes advantage of Amazon Aurora’s fast failover capabilities, reducing failover times from minutes to seconds.
