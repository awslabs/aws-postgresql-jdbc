/*
 * Copyright (c) 2019, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.postgresql.ca;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import javax.net.SocketFactory;

/**
 * This is an implementation of SocketFactory. It is only used for testing.
 * The FailoverSocketFactory utilizes a subclass of Sockets called "Hanging Socket"
 * which will act as an intermediate or to the Socket class. This will allow
 * the class to throw a SocketException a connection when a host is marked "down"
 */

public class FailoverSocketFactory extends SocketFactory {

  public static final String STATUS_UNKNOWN = "?";
  public static final String STATUS_CONNECTED = "/";
  public static final String STATUS_FAILED = "\\";

  public static final long DEFAULT_TIMEOUT_MILLIS = 1000; // was 60 * 10 * 1000

  static final Set<String> IMMEDIATELY_DOWNED_HOSTS = new HashSet<>();
  static final List<String> CONNECTION_ATTEMPTS = new LinkedList<>();

  private Properties props;

  public FailoverSocketFactory(Properties info) {
    this.props = info;
  }

  public static void flushAllStaticData() {
    IMMEDIATELY_DOWNED_HOSTS.clear();
  }

  public static String getHostFromPastConnection(int pos) {
    pos = Math.abs(pos);

    if (pos == 0 || CONNECTION_ATTEMPTS.isEmpty() || CONNECTION_ATTEMPTS.size() < pos) {
      return null;
    }
    return CONNECTION_ATTEMPTS.get(CONNECTION_ATTEMPTS.size() - pos);
  }

  public static List<String> getHostsFromAllConnections() {
    return getHostsFromLastConnections(CONNECTION_ATTEMPTS.size());
  }

  public static List<String> getHostsFromLastConnections(int count) {
    count = Math.abs(count);
    int lBound = Math.max(0, CONNECTION_ATTEMPTS.size() - count);
    return CONNECTION_ATTEMPTS.subList(lBound, CONNECTION_ATTEMPTS.size());
  }

  static void sleepMillisForProperty() {
    try {
      Thread.sleep(DEFAULT_TIMEOUT_MILLIS);
    } catch (NumberFormatException e) {
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
    }
  }

  public static void downHost(String hostname) {
    IMMEDIATELY_DOWNED_HOSTS.add(hostname);
  }

  public static String getHostFromLastConnection() {
    return getHostFromPastConnection(1);
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException, UnknownHostException {
    return new HangingSocket(props, host,port);
  }

  @Override
  public Socket createSocket(InetAddress host, int port) throws IOException {
    return new HangingSocket(props, host, port);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localAdd, int localPort)
      throws IOException {
    return new HangingSocket(props, host, port, localAdd, localPort);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress localAdd, int localPort)
      throws IOException {
    return new HangingSocket(props,address, port, localAdd, localPort);
  }

  @Override
  public Socket createSocket() throws IOException {

    return new HangingSocket(props);
  }

  /**
   * The HangingSocket class which is an additional layer to the "socket" class.
   */
  public class HangingSocket extends Socket {

    Socket underlyingSocket;
    String aliasedHostname;
    Properties props;

    public HangingSocket(Properties info) throws IOException {
      this.underlyingSocket = SocketFactory.getDefault().createSocket();
      this.props = info;
    }

    public HangingSocket(Properties info, String host, int port) throws IOException {
      this.underlyingSocket = SocketFactory.getDefault().createSocket(host, port);
      this.props = info;
    }

    public HangingSocket(Properties info, InetAddress host, int port) throws IOException {
      this.underlyingSocket = SocketFactory.getDefault().createSocket(host, port);
      this.props = info;
    }

    public HangingSocket(Properties info, String host, int port, InetAddress localAdd, int localPort)
        throws IOException {
      this.underlyingSocket = SocketFactory.getDefault().createSocket(host, port, localAdd, localPort);
    }

    public HangingSocket(Properties info, InetAddress host, int port, InetAddress localAdd,
        int localPort) throws IOException {
      this.underlyingSocket = SocketFactory.getDefault().createSocket(host, port, localAdd, localPort);
      this.props = info;
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
      this.aliasedHostname = endpoint.toString();
      this.aliasedHostname = aliasedHostname.substring(0, aliasedHostname.lastIndexOf("/"));

      if (IMMEDIATELY_DOWNED_HOSTS.contains(this.aliasedHostname)) {
        sleepMillisForProperty();
        throw new SocketTimeoutException();
      }

      String result = STATUS_UNKNOWN;
      try {
        this.underlyingSocket.connect(endpoint, timeout);
        result = STATUS_CONNECTED;
      } catch (SocketException e) {
        result = STATUS_FAILED;
        throw e;
      } catch (IOException e) {
        result = STATUS_FAILED;
        throw e;
      } finally {
        CONNECTION_ATTEMPTS.add(result + aliasedHostname);
      }
    }

    @Override
    public void bind(SocketAddress bindpoint) throws IOException {
      this.underlyingSocket.bind(bindpoint);
    }

    @Override
    public synchronized void close() throws IOException {
      this.underlyingSocket.close();
    }

    @Override
    public SocketChannel getChannel() {
      return this.underlyingSocket.getChannel();
    }

    @Override
    public InetAddress getInetAddress() {
      return this.underlyingSocket.getInetAddress();
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return new HangingInputStream(this.underlyingSocket.getInputStream(),
          this.props, this.aliasedHostname);
    }

    @Override
    public boolean getKeepAlive() throws SocketException {
      return this.underlyingSocket.getKeepAlive();
    }

    @Override
    public InetAddress getLocalAddress() {
      return this.underlyingSocket.getLocalAddress();
    }

    @Override
    public int getLocalPort() {
      return this.underlyingSocket.getLocalPort();
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
      return this.underlyingSocket.getLocalSocketAddress();
    }

    @Override
    public boolean getOOBInline() throws SocketException {
      return this.underlyingSocket.getOOBInline();
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      return new HangingOutputStream(this.underlyingSocket.getOutputStream(), this.props, this.aliasedHostname);
    }

    @Override
    public int getPort() {
      return this.underlyingSocket.getPort();
    }

    @Override
    public synchronized int getReceiveBufferSize() throws SocketException {
      return this.underlyingSocket.getReceiveBufferSize();
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
      return this.underlyingSocket.getRemoteSocketAddress();
    }

    @Override
    public boolean getReuseAddress() throws SocketException {
      return this.underlyingSocket.getReuseAddress();
    }

    @Override
    public synchronized int getSendBufferSize() throws SocketException {
      return this.underlyingSocket.getSendBufferSize();
    }

    @Override
    public int getSoLinger() throws SocketException {
      return this.underlyingSocket.getSoLinger();
    }

    @Override
    public synchronized int getSoTimeout() throws SocketException {
      return this.underlyingSocket.getSoTimeout();
    }

    @Override
    public boolean getTcpNoDelay() throws SocketException {
      return this.underlyingSocket.getTcpNoDelay();
    }

    @Override
    public int getTrafficClass() throws SocketException {
      return this.underlyingSocket.getTrafficClass();
    }

    @Override
    public boolean isBound() {
      return this.underlyingSocket.isBound();
    }

    @Override
    public boolean isClosed() {
      return this.underlyingSocket.isClosed();

    }

    @Override
    public boolean isConnected() {
      return this.underlyingSocket.isConnected();
    }

    @Override
    public boolean isInputShutdown() {
      return this.underlyingSocket.isInputShutdown();
    }

    @Override
    public boolean isOutputShutdown() {
      return this.underlyingSocket.isOutputShutdown();
    }

    @Override
    public void sendUrgentData(int data) throws IOException {
      this.underlyingSocket.sendUrgentData(data);
    }

    @Override
    public void setKeepAlive(boolean on) throws SocketException {
      this.underlyingSocket.setKeepAlive(on);
    }

    @Override
    public void setOOBInline(boolean on) throws SocketException {
      this.underlyingSocket.setOOBInline(on);
    }

    @Override
    public synchronized void setReceiveBufferSize(int size) throws SocketException {
      this.underlyingSocket.setReceiveBufferSize(size);
    }

    @Override
    public void setReuseAddress(boolean on) throws SocketException {
      this.underlyingSocket.setReuseAddress(on);
    }

    @Override
    public synchronized void setSendBufferSize(int size) throws SocketException {
      this.underlyingSocket.setSendBufferSize(size);
    }

    @Override
    public void setSoLinger(boolean on, int linger) throws SocketException {
      this.underlyingSocket.setSoLinger(on, linger);
    }

    @Override
    public synchronized void setSoTimeout(int timeout) throws SocketException {
      this.underlyingSocket.setSoTimeout(timeout);
    }

    @Override
    public void setTcpNoDelay(boolean on) throws SocketException {
      this.underlyingSocket.setTcpNoDelay(on);
    }

    @Override
    public void setTrafficClass(int tc) throws SocketException {
      this.underlyingSocket.setTrafficClass(tc);
    }

    @Override
    public void shutdownInput() throws IOException {
      this.underlyingSocket.shutdownInput();
    }

    @Override
    public void shutdownOutput() throws IOException {
      this.underlyingSocket.shutdownOutput();
    }

    @Override
    public String toString() {
      return this.underlyingSocket.toString();
    }

  }

  static class HangingInputStream extends InputStream {
    final InputStream underlyingInputStream;
    final Properties propSet;
    final String aliasedHostname;

    HangingInputStream(InputStream realInputStream, Properties pset, String aliasedHostname) {
      this.underlyingInputStream = realInputStream;
      this.propSet = pset;
      this.aliasedHostname = aliasedHostname;
    }

    @Override
    public int available() throws IOException {
      return this.underlyingInputStream.available();
    }

    @Override
    public void close() throws IOException {
      this.underlyingInputStream.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
      this.underlyingInputStream.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
      return this.underlyingInputStream.markSupported();
    }

    @Override
    public synchronized void reset() throws IOException {
      this.underlyingInputStream.reset();
    }

    @Override
    public long skip(long n) throws IOException {
      failIfRequired();

      return this.underlyingInputStream.skip(n);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      failIfRequired();

      return this.underlyingInputStream.read(b, off, len);
    }

    @Override
    public int read(byte[] b) throws IOException {
      failIfRequired();

      return this.underlyingInputStream.read(b);
    }

    @Override
    public int read() throws IOException {
      failIfRequired();

      return this.underlyingInputStream.read();
    }

    private void failIfRequired() throws SocketTimeoutException {

      if (IMMEDIATELY_DOWNED_HOSTS.contains(this.aliasedHostname)) {
        sleepMillisForProperty();
        throw new SocketTimeoutException();
      }
    }
  }

  static class HangingOutputStream extends OutputStream {

    final Properties propSet;
    final String aliasedHostname;
    final OutputStream underlyingOutputStream;

    HangingOutputStream(OutputStream realOutputStream, Properties pset, String aliasedHostname) {
      this.underlyingOutputStream = realOutputStream;
      this.propSet = pset;
      this.aliasedHostname = aliasedHostname;
    }

    @Override
    public void close() throws IOException {
      failIfRequired();
      this.underlyingOutputStream.close();
    }

    @Override
    public void flush() throws IOException {
      this.underlyingOutputStream.flush();
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      failIfRequired();
      this.underlyingOutputStream.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
      failIfRequired();
      this.underlyingOutputStream.write(b);
    }

    @Override
    public void write(int b) throws IOException {
      failIfRequired();
      this.underlyingOutputStream.write(b);
    }

    private void failIfRequired() throws SocketTimeoutException {
      if (IMMEDIATELY_DOWNED_HOSTS.contains(this.aliasedHostname)) {
        sleepMillisForProperty();
        throw new SocketTimeoutException();
      }
    }
  }

}
