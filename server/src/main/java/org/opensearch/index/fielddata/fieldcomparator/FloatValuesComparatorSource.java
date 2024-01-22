/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.comparators.FloatComparator;
import org.apache.lucene.util.BitSet;
import org.opensearch.common.Nullable;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.NumericDoubleValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Comparator source for float values.
 *
 * @opensearch.internal
 */
public class FloatValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexNumericFieldData indexFieldData;

    public FloatValuesComparatorSource(
        IndexNumericFieldData indexFieldData,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested
    ) {
        super(missingValue, sortMode, nested);
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.FLOAT;
    }

    private NumericDoubleValues getNumericDocValues(LeafReaderContext context, float missingValue) throws IOException {
        final SortedNumericDoubleValues values = indexFieldData.load(context).getDoubleValues();
        if (nested == null) {
            return FieldData.replaceMissing(sortMode.select(values), missingValue);
        } else {
            final BitSet rootDocs = nested.rootDocs(context);
            final DocIdSetIterator innerDocs = nested.innerDocs(context);
            final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
            return sortMode.select(values, missingValue, rootDocs, innerDocs, context.reader().maxDoc(), maxChildren);
        }
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, Pruning pruning, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final float fMissingValue = (Float) missingObject(missingValue, reversed);
        return new FloatComparator(numHits, fieldname, fMissingValue, reversed, filterPruning(pruning)) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                return new FloatLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return FloatValuesComparatorSource.this.getNumericDocValues(context, fMissingValue).getRawFloatValues();
                    }
                };
            }
        };
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        return new BucketedSort.ForFloats(bigArrays, sortOrder, format, bucketSize, extra) {
            private final float dMissingValue = (Float) missingObject(missingValue, sortOrder == SortOrder.DESC);

            @Override
            public boolean needsScores() {
                return false;
            }

            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf(ctx) {
                    private final NumericDoubleValues docValues = getNumericDocValues(ctx, dMissingValue);
                    private float docValue;

                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        if (docValues.advanceExact(doc)) {
                            docValue = (float) docValues.doubleValue();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    protected float docValue() {
                        return docValue;
                    }
                };
            }
        };
    }
}
