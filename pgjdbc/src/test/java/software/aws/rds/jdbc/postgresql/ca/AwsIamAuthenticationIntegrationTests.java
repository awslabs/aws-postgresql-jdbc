package software.aws.rds.jdbc.postgresql.ca;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.postgresql.PGProperty;
import org.postgresql.util.PSQLException;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Disabled
public class AwsIamAuthenticationIntegrationTests {

  private static final String CONNECTION_STRING_PREFIX = "jdbc:postgresql:aws://";
  private static final String CONNECTION_STRING = CONNECTION_STRING_PREFIX + System.getenv("PG_AURORA_INSTANCE_ENDPOINT");
  private static final String VALID_AWS_DB_USER = System.getenv("PG_AURORA_IAM_USER");

  @Test
  public void test_1_ValidAwsIamUser() throws SQLException {
    final Properties properties = createProperties(VALID_AWS_DB_USER);
    Assertions.assertNotNull(DriverManager.getConnection(CONNECTION_STRING, properties));
  }

  @Test
  public void test_2_InvalidAwsIamUser() {
    final Properties properties = createProperties("invalidUser", "invalidPass");
    Assertions.assertThrows(
        PSQLException.class,
        () -> DriverManager.getConnection(CONNECTION_STRING, properties));
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
