package software.aws.rds.jdbc.postgresql.ca;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.postgresql.PGProperty;
import org.postgresql.util.PSQLException;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

@Disabled
@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class AwsIamAuthenticationIntegrationTests {

  private static final String CONNECTION_STRING_PREFIX = "jdbc:postgresql:aws://";
  private static final String CONNECTION_STRING = CONNECTION_STRING_PREFIX + System.getenv("PG_AURORA_CLUSTER_IDENTIFIER");
  private static final String SSL_CERTIFICATE = "src/test/java/software/aws/rds/jdbc/postgresql/ca/certs/rds-ca-2019-root.pem";
  private static final String SSL_MODE = "verify-full";
  private static final String VALID_AWS_DB_USER = System.getenv("AWS_DB_USER");

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
    properties.setProperty(PGProperty.SSL_MODE.getName(), SSL_MODE);
    properties.setProperty(PGProperty.SSL_ROOT_CERT.getName(), SSL_CERTIFICATE);

    return properties;
  }
}
