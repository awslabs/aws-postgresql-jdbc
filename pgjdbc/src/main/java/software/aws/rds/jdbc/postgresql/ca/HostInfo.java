/*
 * AWS JDBC Driver for PostgreSQL
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import software.aws.rds.jdbc.postgresql.Driver;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.util.HostSpec;

import java.util.Properties;

/**
 * A class that contains details about a host and it's connection properties.
 */

public class HostInfo {
  public static final int NO_PORT = -1;
  private static final String HOST_PORT_SEPARATOR = ":";
  private final String endpoint; // complete DNS endpoint
  private final @Nullable String instanceIdentifier;
  private final boolean isWriter;
  private final int port;

  /**
   * Constructor for HostInfo
   *
   * @param endpoint The instance endpoint (complete DNS, no port, no protocol)
   * @param instanceIdentifier The instance identifier of this connection
   * @param port The port for this connection
   * @param isWriter True if this host is a writer
   */

  public HostInfo(String endpoint, @Nullable String instanceIdentifier, int port, boolean isWriter) {
    this.endpoint = endpoint;
    this.instanceIdentifier = instanceIdentifier;
    this.port = port;
    this.isWriter = isWriter;
  }

  /**
   * Creates a connection string URL from an endpoint, port, and the database name
   *
   * @param endpoint The endpoint used in the connection
   * @param port The port number used in the connection
   * @param dbname The database name used in the connection
   * @return a connection string to a an instance
   */
  private String getUrlFromEndpoint(String endpoint, int port, @Nullable String dbname) {
    if (dbname == null) {
      dbname = "";
    }
    return String.format(
        "%s//%s:%d/%s", Driver.AWS_PROTOCOL, endpoint, port, dbname);
  }

  /**
   * Accessor method to retrieve the url of the host connection
   * @param dbname The database name used in the connection
   *
   * @return A string that contains the connection url
   */
  public @Nullable String getUrl(@Nullable String dbname) {
    return getUrlFromEndpoint(this.endpoint, this.port, dbname);
  }

  /**
   * Accessor method to retrieve the url of the host connection
   * @return A string that contains the connection url
   */
  public @Nullable String getUrl() {
    return getUrl("");
  }

  /**
   * Accessor method to retrieve the url of the host connection
   * @param props The Properties to use
   * @return A string that contains the connection url
   */
  public @Nullable String getUrl(@Nullable Properties props) {
    String dbname = props == null ? "" : props.getProperty("PGDBNAME", "");
    return getUrl(dbname);
  }

  /**
   * Retrieves the instance host
   *
   * @return the instance host (complete DNS, no port, no protocol)
   */
  public String getHost() {
    return this.endpoint;
  }

  /**
   * Retrieves the instance port
   *
   * @return the port number.
   */
  public int getPort() {
    return this.port;
  }

  /**
   * Accessor method for instanceIdentifier
   *
   * @return The identifier for an instance in the host cluster
   */
  public @Nullable String getInstanceIdentifier() {
    return this.instanceIdentifier;
  }

  /**
   * Accessor method for isWriter
   *
   * @return True if the host is a writer
   */
  public boolean isWriter() {
    return this.isWriter;
  }

  /**
   * Retrieves the host and port
   *
   * @return A string containing the host and port separated by a ":"
   */
  public String getHostPortPair() {
    return getHost() + HOST_PORT_SEPARATOR + getPort();
  }

  /**
   * Checks if the host-port-pair of this HostInfo object is the same as another HostInfo object.
   *
   * @param other The other HostInfo object, to compare to this HostInfo object
   * @return True if the host-port-pair is equal
   */
  public boolean equalsHostPortPair(@Nullable HostInfo other) {
    if (other == null) {
      return false;
    }
    return getHostPortPair().equals(other.getHostPortPair());
  }

  /**
   * Creates a HostSpec object from the current HostInfo object
   *
   * @return {@link HostSpec} object containing the host and port of this instance
   */
  public HostSpec toHostSpec() {

    return new HostSpec(this.getHost(), this.getPort());

  }

  /**
   * Retrieves a textual representation of this {@link HostInfo} object
   *
   * @return a string representation of this object.
   */
  @Override
  public String toString() {
    return super.toString() + String.format(" :: {host: \"%s\", port: %d, isWriter: %b}", this.getHost(), this.getPort(), this.isWriter());
  }

}
