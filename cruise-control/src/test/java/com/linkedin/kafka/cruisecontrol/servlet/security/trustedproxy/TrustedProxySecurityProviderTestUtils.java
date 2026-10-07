/*
 * Copyright 2023 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */
package com.linkedin.kafka.cruisecontrol.servlet.security.trustedproxy;

import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlApp;
import com.linkedin.kafka.cruisecontrol.servlet.security.MiniKdc;

import static com.linkedin.kafka.cruisecontrol.servlet.CruiseControlEndPoint.STATE;
import static com.linkedin.kafka.cruisecontrol.servlet.parameters.ParameterUtils.DO_AS;
import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.assertAuthorized;
import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.assertRejected;
import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.openConnection;
import static com.linkedin.kafka.cruisecontrol.servlet.security.SecurityTestUtils.runAs;

/**
 * A test util class.
 */
public final class TrustedProxySecurityProviderTestUtils {

    private static final String CRUISE_CONTROL_STATE_ENDPOINT = "kafkacruisecontrol/" + STATE;

    private TrustedProxySecurityProviderTestUtils() {

    }

    public static void testSuccessfulAuthentication(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal, String doAs) throws Exception {
        String endpoint = CRUISE_CONTROL_STATE_ENDPOINT + '?' + DO_AS + '=' + doAs;
        runAs(miniKdc, principal, () -> assertAuthorized(openConnection(app, endpoint)));
    }

    public static void testNoDoAsParameter(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal) throws Exception {
        runAs(miniKdc, principal, () -> assertRejected(openConnection(app, CRUISE_CONTROL_STATE_ENDPOINT)));
    }

    public static void testNotAdminServiceLogin(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal, String doAs) throws Exception {
        String endpoint = CRUISE_CONTROL_STATE_ENDPOINT + '?' + DO_AS + '=' + doAs;
        runAs(miniKdc, principal, () -> assertRejected(openConnection(app, endpoint)));
    }

    public static void testSuccessfulFallbackAuthentication(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal) throws Exception {
        runAs(miniKdc, principal, () -> assertAuthorized(openConnection(app, CRUISE_CONTROL_STATE_ENDPOINT)));
    }

    public static void testUnsuccessfulFallbackAuthentication(MiniKdc miniKdc, KafkaCruiseControlApp app, String principal) throws Exception {
        runAs(miniKdc, principal, () -> assertRejected(openConnection(app, CRUISE_CONTROL_STATE_ENDPOINT)));
    }

}
