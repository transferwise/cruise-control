/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.servlet.security.spnego;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlApp;
import com.linkedin.kafka.cruisecontrol.servlet.security.MiniKdc;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.assertAuthorized;
import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.assertRejected;
import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.openConnection;
import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.runAs;

/**
 * A test util class.
 */
public final class SpnegoSecurityProviderTestUtils {

    private static final String CRUISE_CONTROL_STATE_ENDPOINT = "kafkacruisecontrol/" + STATE;

    private SpnegoSecurityProviderTestUtils() {

    }

    public static void testSuccessfulAuthentication(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal) throws Exception {
        runAs(miniKdc, principal, () -> assertAuthorized(openConnection(app, CRUISE_CONTROL_STATE_ENDPOINT)));
    }

    public static void testNotAdminServiceLogin(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal) throws Exception {
        runAs(miniKdc, principal, () -> assertRejected(openConnection(app, CRUISE_CONTROL_STATE_ENDPOINT)));
    }

}
