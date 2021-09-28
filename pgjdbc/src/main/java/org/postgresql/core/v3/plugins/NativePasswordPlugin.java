/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3.plugins;

import java.nio.charset.StandardCharsets;

public class NativePasswordPlugin implements IAuthenticationPlugin {

  @Override
  public byte[] getEncodedPassword(String user, String password) {
    return password.getBytes(StandardCharsets.UTF_8);
  }
}
