/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

/**
 * Utility class to manage feature flags. Feature flags are system properties that must be set on the JVM.
 * These are used to gate the visibility/availability of incomplete features. Fore more information, see
 * https://featureflags.io/feature-flag-introduction/
 */
public class FeatureFlags {

    /**
     * Gates the visibility of the index setting that enables segment replication.
     * Once the feature is ready for production release, this feature flag can be removed.
     */
    public static final String SEGREP_FEATURE_FLAG = "opensearch.segment_replication_feature_flag_enabled";

    /**
     * Used to test feature flags whose values are expected to be booleans.
     * This method returns true if the value is "true" (case-insensitive),
     * and false otherwise.
     */
    public static boolean isEnabled(String featureFlagName) {
        return "true".equalsIgnoreCase(System.getProperty(featureFlagName));
    }
}
