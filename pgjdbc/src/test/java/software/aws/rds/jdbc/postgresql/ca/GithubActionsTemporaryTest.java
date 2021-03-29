package software.aws.rds.jdbc.postgresql.ca;

import org.junit.Test;
import org.postgresql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class GithubActionsTemporaryTest {
  String url = "jdbc:postgresql://localhost:5432/";
  String user = "postgres";
  String password = "postgres";

  @Test
  public void test() throws SQLException {
    boolean success = false;
    Driver driver = new Driver();
    DriverManager.registerDriver(driver);
    try(Connection conn = DriverManager.getConnection(url, user, password)) {
      success = true;
    }
    assertTrue(success);
  }
}
