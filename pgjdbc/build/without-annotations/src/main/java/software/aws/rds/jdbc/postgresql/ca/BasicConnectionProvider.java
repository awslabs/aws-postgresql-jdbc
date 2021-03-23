/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import org.postgresql.core.BaseConnection;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.HostSpec;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * This class is a basic implementation of ConnectionProvider interface. It creates and returns an
 * instance of ConnectionImpl.
 */
public class BasicConnectionProvider implements ConnectionProvider {

  /**
   * Called once per connection that needs to be created.
   *
   * @param hostSpec The HostSpec containing the host-port information for the host to connect to
   * @param props The Properties to use for the connection
   * @param url The connection URL
   *
   * @return {@link Connection} resulting from the given connection information
   * @throws SQLException if an error occurs
   */
  @Override
  public BaseConnection connect(HostSpec hostSpec, Properties props, String url) throws SQLException {
    return new PgConnection(new HostSpec[] { hostSpec }, user(props), database(props), props, url);
  }

  /**
   * Method to retrieve the user from the connection properties
   * 
   * @param props Properties instance that contains the connection properties
   * @return The name of the user used for the database
   */
  private String user(Properties props) {
    return props.getProperty("user", "");
  }

  /**
   * Method to retrieve the user from the connection properties
   *
   * @param props Properties instance that contains the connection properties
   * @return The name of the user used for the database
   */
  private String database(Properties props) {
    return props.getProperty("PGDBNAME", "");
  }
}
