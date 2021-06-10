/*
 * Copyright (c) 2004, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2.optional;

import static org.junit.Assert.assertEquals;

import org.postgresql.ds.common.BaseDataSource;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;

import javax.naming.NamingException;

/**
* tests that failover urls survive the parse/rebuild roundtrip with and without specific ports
*/
public class BaseDataSourceFailoverUrlsTest {

  boolean isAwsRegsitered = false;
  boolean isCommunityRegistered = false;

  @Before
  public void deregisterAwsDriver() throws SQLException {
    ArrayList<Driver> drivers = Collections.list(DriverManager.getDrivers());
    for (java.sql.Driver driver : drivers) {
      if (driver instanceof software.aws.rds.jdbc.postgresql.Driver) {
        DriverManager.deregisterDriver(driver);
        isAwsRegsitered = true;

      }
      if (driver instanceof org.postgresql.Driver) {
        isCommunityRegistered = true;
      }
    }
    DriverManager.registerDriver(new org.postgresql.Driver());
  }

  @After
  public void regsiterAwsDriver() throws SQLException {
    ArrayList<Driver> drivers = Collections.list(DriverManager.getDrivers());
    for (java.sql.Driver driver : drivers) {
      if (driver instanceof org.postgresql.Driver && ! isCommunityRegistered) {
        DriverManager.deregisterDriver(driver);
      }
    }
    if (isAwsRegsitered) {
      DriverManager.registerDriver(new software.aws.rds.jdbc.postgresql.Driver());
    }
  }

  private static final String DEFAULT_PORT = "5432";

  @Test
  public void testFullDefault() throws ClassNotFoundException, NamingException, IOException {
    roundTripFromUrl("jdbc:postgresql://server/database", "jdbc:postgresql://server:" + DEFAULT_PORT + "/database");
  }

  @Test
  public void testTwoNoPorts() throws ClassNotFoundException, NamingException, IOException {
    roundTripFromUrl("jdbc:postgresql://server1,server2/database", "jdbc:postgresql://server1:" + DEFAULT_PORT + ",server2:" + DEFAULT_PORT + "/database");
  }

  @Test
  public void testTwoWithPorts() throws ClassNotFoundException, NamingException, IOException {
    roundTripFromUrl("jdbc:postgresql://server1:1234,server2:2345/database", "jdbc:postgresql://server1:1234,server2:2345/database");
  }

  @Test
  public void testTwoFirstPort() throws ClassNotFoundException, NamingException, IOException {
    roundTripFromUrl("jdbc:postgresql://server1,server2:2345/database", "jdbc:postgresql://server1:" + DEFAULT_PORT + ",server2:2345/database");
  }

  @Test
  public void testTwoLastPort() throws ClassNotFoundException, NamingException, IOException {
    roundTripFromUrl("jdbc:postgresql://server1:2345,server2/database", "jdbc:postgresql://server1:2345,server2:" + DEFAULT_PORT + "/database");
  }

  @Test
  public void testNullPorts() {
    BaseDataSource bds = newDS();
    bds.setDatabaseName("database");
    bds.setPortNumbers(null);
    assertUrlWithoutParamsEquals("jdbc:postgresql://localhost/database", getUrlWithOriginalProtocol(bds));
    assertEquals(0, bds.getPortNumber());
    assertEquals(0, bds.getPortNumbers()[0]);
  }

  @Test
  public void testEmptyPorts() {
    BaseDataSource bds = newDS();
    bds.setDatabaseName("database");
    bds.setPortNumbers(new int[0]);
    assertUrlWithoutParamsEquals("jdbc:postgresql://localhost/database", getUrlWithOriginalProtocol(bds));
    assertEquals(0, bds.getPortNumber());
    assertEquals(0, bds.getPortNumbers()[0]);
  }

  private BaseDataSource newDS() {
    return new BaseDataSource() {
      @Override
      public String getDescription() {
        return "BaseDataSourceFailoverUrlsTest-DS";
      }
    };
  }

  private void roundTripFromUrl(String in, String expected) throws NamingException, ClassNotFoundException, IOException {
    BaseDataSource bds = newDS();

    bds.setUrl(in);
    assertUrlWithoutParamsEquals(expected, getUrlWithOriginalProtocol(bds));

    bds.setFromReference(bds.getReference());
    assertUrlWithoutParamsEquals(expected, getUrlWithOriginalProtocol(bds));

    bds.initializeFrom(bds);
    assertUrlWithoutParamsEquals(expected, getUrlWithOriginalProtocol(bds));
  }

  private static String jdbcUrlStripParams(String in) {
    return in.replaceAll("\\?.*$", "");
  }

  private static void assertUrlWithoutParamsEquals(String expected, String url) {
    assertEquals(expected, jdbcUrlStripParams(url));
  }

  /**
   * Replaces aws protocol with community protocol when retrieving url from BaseDataSource
   * @param bds BaseDataSource to retrieve URL
   * @return URL from BaseDataSource with community protocol
   */
  private String getUrlWithOriginalProtocol(BaseDataSource bds) {

    return bds.getUrl().replace("jdbc:postgresql:aws://", "jdbc:postgresql://");
  }
}
