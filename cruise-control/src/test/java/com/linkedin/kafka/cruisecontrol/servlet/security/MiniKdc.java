/*
 * Copyright 2020 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security;

import com.linkedin.kafka.cruisecontrol.metricsreporter.utils.CCKafkaTestUtils;
import org.apache.kerby.kerberos.kerb.KrbException;
import org.apache.kerby.kerberos.kerb.client.JaasKrbUtil;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

/**
 * A very simple KDC that can be used for testing.
 */
public class MiniKdc {

  private static final String TEMP_DIR_PROPERTY_KEY = "java.io.tmpdir";
  public static final String KEYTAB_FILE_EXTENSION = ".keytab";
  public static final String KERBY_SERVER_TEST_HARNESS_DIR_PREFIX = "kerby-server-test-harness-";
  private static final int MAX_KDC_LOGIN_ATTEMPTS = 3;
  private static final long KDC_LOGIN_RETRY_BASE_MS = 500;

  private final SimpleKdcServer _kerbyServer;
  private final File _keytab;
  private final String _realm;
  private final List<String> _principals;

  public MiniKdc(String realm, List<String> principals) throws KrbException {
    _kerbyServer = new SimpleKdcServer();
    _realm = realm;
    _principals = principals;
    _keytab = Paths.get(System.getProperty(TEMP_DIR_PROPERTY_KEY), UUID.randomUUID() + KEYTAB_FILE_EXTENSION).toFile();
  }

  public File keytab() {
    return _keytab;
  }

  /**
   * Initializes and starts the KDC.
   * @throws KrbException
   * @throws IOException
   */
  public void start() throws KrbException, IOException {
    _kerbyServer.setWorkDir(Files.createTempDirectory(KERBY_SERVER_TEST_HARNESS_DIR_PREFIX).toFile());
    _kerbyServer.setKdcRealm(_realm);
    _kerbyServer.setAllowUdp(false);
    // Use a dynamic KDC port to avoid test port collisions and TIME_WAIT "Connection reset" errors. init() saves
    // this assigned port to krb5.conf.
    _kerbyServer.setKdcTcpPort(CCKafkaTestUtils.findLocalPort());
    _kerbyServer.init();
    _kerbyServer.start();

    _kerbyServer.createAndExportPrincipals(_keytab, _principals.toArray(new String[]{}));
  }

  /**
   * Stops the KDC.
   * @throws KrbException
   */
  public void stop() throws KrbException {
    _kerbyServer.stop();
  }

  /**
   * Logs in the given principal against the KDC using its keytab, retrying briefly to absorb transient KDC
   * readiness/connection-reset failures.
   * @param principal the principal to authenticate.
   * @return the authenticated {@link Subject}.
   * @throws LoginException if the login fails after all retry attempts.
   */
  public Subject loginAs(String principal) throws LoginException {
    // The KDC's network listener may not be ready the instant start() returns; on loaded hosts the first login can
    // hit a transient "Connection reset". Retry a few times with a short backoff before giving up.
    LoginException lastException = null;
    for (int attempt = 0; attempt < MAX_KDC_LOGIN_ATTEMPTS; attempt++) {
      try {
        return JaasKrbUtil.loginUsingKeytab(principal, _keytab);
      } catch (LoginException e) {
        lastException = e;
        if (attempt < MAX_KDC_LOGIN_ATTEMPTS - 1) {
          try {
            Thread.sleep(KDC_LOGIN_RETRY_BASE_MS << attempt);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }
    throw lastException;
  }
}
