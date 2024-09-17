/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.common;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractIndexAnalyzerProvider;

/**
 * Provider for {@link PhoneNumberAnalyzer}.
 */
public class PhoneNumberAnalyzerProvider extends AbstractIndexAnalyzerProvider<PhoneNumberAnalyzer> {

    private final PhoneNumberAnalyzer analyzer;

    public PhoneNumberAnalyzerProvider(
        final IndexSettings indexSettings,
        final String name,
        final Settings settings,
        final boolean addNgrams
    ) {
        super(indexSettings, name, settings);
        this.analyzer = new PhoneNumberAnalyzer(settings, addNgrams);
    }

    @Override
    public PhoneNumberAnalyzer get() {
        return this.analyzer;
    }
}
