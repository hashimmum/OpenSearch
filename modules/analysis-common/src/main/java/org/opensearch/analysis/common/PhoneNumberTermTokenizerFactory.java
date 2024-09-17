/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.Tokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenizerFactory;

/**
 * Factory for {@link PhoneNumberTermTokenizer}.
 */
public class PhoneNumberTermTokenizerFactory extends AbstractTokenizerFactory {
    private final Settings settings;
    private final boolean addNgrams;

    public PhoneNumberTermTokenizerFactory(
        final IndexSettings indexSettings,
        final String name,
        final Settings settings,
        final boolean addNgrams
    ) {
        super(indexSettings, settings, name);
        this.settings = settings;
        this.addNgrams = addNgrams;
    }

    @Override
    public Tokenizer create() {
        return new PhoneNumberTermTokenizer(this.settings, this.addNgrams);
    }
}
