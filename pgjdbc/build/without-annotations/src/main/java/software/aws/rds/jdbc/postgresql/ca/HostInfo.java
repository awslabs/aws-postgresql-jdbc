/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import org.postgresql.PGProperty;
import org.postgresql.util.HostSpec;

// import org.checkerframework.checker.nullness.qual.Nullable;

import org.postgresql.util.PSQLException;

import java.util.Properties;

/**
 * A class that contains details about a host and it's connection properties.
 */

public class HostInfo {
  public static final int NO_PORT = -1;
  private static final String HOST_PORT_SEPARATOR = ":";
  private final /* @Nullable */ String url;
  private final /* @Nullable */ String instanceIdentifier;
  private final boolean isWriter;
  private /* @Nullable */ Properties props;

  /**
   * Constructor for HostInfo
   *
   * @param url The connection string
   * @param instanceIdentifier The instance identifier of this connection
   * @param isWriter True if this host is a writer
   * @param props A {@link Properties} object that contains configurations and login details for a connection
   */

  public HostInfo(/* @Nullable */ String url, /* @Nullable */ String instanceIdentifier, boolean isWriter, Properties props) {
    this.url = url;
    this.instanceIdentifier = instanceIdentifier;
    this.isWriter = isWriter;
    this.props = props;
  }

  /**
   * Static method to help initialize properties for the HostInfo constructor
   *
   * @param host The host that you are trying to connect
   * @param port The port of the connection
   * @param database The name of the database
   * @return New {@link Properties} object to be used for the constructor
   */
  public static Properties initProps(String host, int port, String database) {

    Properties props = new Properties();
    PGProperty.PG_HOST.set(props, host);
    PGProperty.PG_PORT.set(props, port);
    PGProperty.PG_DBNAME.set(props, database);
    return props;
  }

  /**
   * Updates the host field for the class
   *
   * @param host The new host to set
   */
  public void updateHostProp(String host) {
    PGProperty.PG_HOST.set(props, host);
  }

  /**
   * Mutator method to set the properties related to the host
   *
   * @param info The new Properties object
   */
  public void setProps(Properties info) {
    props = info;
  }

  /**
   * Accessor method to retrieve the properties of the host
   *
   * @return {@link Properties} object related to the host
   */
  public Properties getProps() {
    return props;
  }

  /**
   * Creates a copy of the host's properties
   *
   * @return A new {@link Properties} object in which it's values are copied from the current host's
   *     properties
   */
  public Properties copyProps() {
    Properties newProp = new Properties(props);
    return newProp;
  }

  /**
   * Accessor method to retrieve the url of the host connection
   *
   * @return A string that contains the connection url
   */
  public /* @Nullable */ String getUrl() {
    return this.url;
  }

  /**
   * Retrieves the host from the props field
   *
   * @return the host from the props field
   */
  public String getHost() {
    return PGProperty.PG_HOST.get(props);
  }

  /**
   * Retrieves the port from the props field
   *
   * @return the port number from the props field. If it isn't parsable to an integer, it will return
   *     -1 by default
   */
  public int getPort() {
    try{
      return PGProperty.PG_PORT.getInt(props);
    } catch(PSQLException e){
      return NO_PORT;
    }
  }

  /**
   * Accessor method for instanceIdentifier
   *
   * @return The identifier for an instance in the host cluster
   */
  public /* @Nullable */ String getInstanceIdentifier() { return this.instanceIdentifier; }

  /**
   * Retrieves the database name from the props field
   *
   * @return the database name of the connection.
   */
  public /* @Nullable */ String getDatabase() {
    return PGProperty.PG_DBNAME.get(props);
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
   * Checks if the host-port-pair of a HostInfo is the same as another HostInfo object.
   *
   * @param other The HostInfo you want to compare with
   * @return True if the host-port-pair is equal
   */
  public boolean equalsHostPortPair(HostInfo other) {
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
   * Returns a string representation of this object.
   *
   * @return a string representation of this object
   */
  @Override
  public String toString() {
    StringBuilder asStr = new StringBuilder(super.toString());
    asStr.append(String.format(" :: {host: \"%s\", port: %d, isWriter: %b}", this.getHost(), this.getPort(), this.isWriter()));
    return asStr.toString();
  }

}
