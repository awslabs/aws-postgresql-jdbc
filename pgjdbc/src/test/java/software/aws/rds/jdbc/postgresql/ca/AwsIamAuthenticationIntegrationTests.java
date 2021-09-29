/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.postgresql.Driver;
import org.postgresql.PGProperty;
import org.postgresql.util.PSQLException;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Disabled
public class AwsIamAuthenticationIntegrationTests {
  private static final String CONNECTION_STRING_PREFIX = "jdbc:postgresql://";
  private static final String CONNECTION_STRING = CONNECTION_STRING_PREFIX + System.getenv("PG_AURORA_INSTANCE_ENDPOINT");
  private static final String VALID_AWS_DB_USER = System.getenv("PG_AURORA_IAM_USER");

  @Test
  public void test_1_ValidAwsIamUser() throws SQLException {
    Driver.register();
    final Properties properties = createProperties(VALID_AWS_DB_USER);
    Assertions.assertNotNull(DriverManager.getConnection(CONNECTION_STRING, properties));
    Driver.deregister();
  }

  @Test
  public void test_2_InvalidAwsIamUser() throws SQLException {
    Driver.register();
    final Properties properties = createProperties("invalidUser", "invalidPass");
    Assertions.assertThrows(
        PSQLException.class,
        () -> DriverManager.getConnection(CONNECTION_STRING, properties));
    Driver.deregister();
  }

  private Properties createProperties(final String user) {
    return createProperties(user, null);
  }

  private Properties createProperties(final String user, final String password) {
    final Properties properties = new Properties();
    properties.setProperty(PGProperty.USER.getName(), user);
    if (password != null) {
      properties.setProperty(PGProperty.PASSWORD.getName(), password);
    }
    properties.setProperty(PGProperty.USE_AWS_IAM.getName(), Boolean.TRUE.toString());

    return properties;
  }
}
