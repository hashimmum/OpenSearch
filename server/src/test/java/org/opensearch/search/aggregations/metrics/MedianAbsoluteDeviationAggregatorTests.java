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

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.aggregations.support.AggregationInspectionHelper;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.opensearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregatorTests.ExactMedianAbsoluteDeviation.calculateMAD;
import static org.opensearch.search.aggregations.metrics.MedianAbsoluteDeviationAggregatorTests.IsCloseToRelative.closeToRelative;
import static org.hamcrest.Matchers.equalTo;

public class MedianAbsoluteDeviationAggregatorTests extends AggregatorTestCase {

    private static final int SAMPLE_MIN = -1000000;
    private static final int SAMPLE_MAX = 1000000;
    public static final String FIELD_NAME = "number";

    /** Script to return the {@code _value} provided by aggs framework. */
    private static final String VALUE_SCRIPT = "_value";
    private static final String SINGLE_SCRIPT = "single";

    private static <T extends IndexableField> CheckedConsumer<RandomIndexWriter, IOException> randomSample(
        int size,
        Function<Long, Iterable<T>> field
    ) {

        return writer -> {
            for (int i = 0; i < size; i++) {
                final long point = randomLongBetween(SAMPLE_MIN, SAMPLE_MAX);
                Iterable<T> document = field.apply(point);
                writer.addDocument(document);
            }
        };
    }

    // intentionally not writing any docs
    public void testNoDocs() throws IOException {
        testAggregation(new MatchAllDocsQuery(), writer -> {}, agg -> {
            assertThat(agg.getMedianAbsoluteDeviation(), equalTo(Double.NaN));
            assertFalse(AggregationInspectionHelper.hasValue(agg));
        });
    }

    public void testNoMatchingField() throws IOException {
        testAggregation(new MatchAllDocsQuery(), writer -> {
            writer.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
            writer.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 2)));
        }, agg -> {
            assertThat(agg.getMedianAbsoluteDeviation(), equalTo(Double.NaN));
            assertFalse(AggregationInspectionHelper.hasValue(agg));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        final int size = randomIntBetween(100, 1000);
        final List<Long> sample = new ArrayList<>(size);
        testAggregation(new DocValuesFieldExistsQuery(FIELD_NAME), randomSample(size, point -> {
            sample.add(point);
            return singleton(new SortedNumericDocValuesField(FIELD_NAME, point));
        }), agg -> {
            assertThat(agg.getMedianAbsoluteDeviation(), closeToRelative(calculateMAD(sample)));
            assertTrue(AggregationInspectionHelper.hasValue(agg));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        final int size = randomIntBetween(100, 1000);
        final List<Long> sample = new ArrayList<>(size);
        testAggregation(new DocValuesFieldExistsQuery(FIELD_NAME), randomSample(size, point -> {
            sample.add(point);
            return singleton(new NumericDocValuesField(FIELD_NAME, point));
        }), agg -> {
            assertThat(agg.getMedianAbsoluteDeviation(), closeToRelative(calculateMAD(sample)));
            assertTrue(AggregationInspectionHelper.hasValue(agg));
        });
    }

    public void testQueryFiltering() throws IOException {
        final int lowerRange = 1;
        final int upperRange = 500;
        final int[] sample = IntStream.rangeClosed(1, 1000).toArray();
        final int[] filteredSample = Arrays.stream(sample).filter(point -> point >= lowerRange && point <= upperRange).toArray();
        testAggregation(IntPoint.newRangeQuery(FIELD_NAME, lowerRange, upperRange), writer -> {
            for (int point : sample) {
                writer.addDocument(Arrays.asList(new IntPoint(FIELD_NAME, point), new SortedNumericDocValuesField(FIELD_NAME, point)));
            }
        }, agg -> {
            assertThat(agg.getMedianAbsoluteDeviation(), closeToRelative(calculateMAD(filteredSample)));
            assertTrue(AggregationInspectionHelper.hasValue(agg));
        });
    }

    public void testQueryFiltersAll() throws IOException {
        testAggregation(IntPoint.newRangeQuery(FIELD_NAME, -1, 0), writer -> {
            writer.addDocument(Arrays.asList(new IntPoint(FIELD_NAME, 1), new SortedNumericDocValuesField(FIELD_NAME, 1)));
            writer.addDocument(Arrays.asList(new IntPoint(FIELD_NAME, 2), new SortedNumericDocValuesField(FIELD_NAME, 2)));
        }, agg -> {
            assertThat(agg.getMedianAbsoluteDeviation(), equalTo(Double.NaN));
            assertFalse(AggregationInspectionHelper.hasValue(agg));
        });
    }

    public void testUnmapped() throws IOException {
        MedianAbsoluteDeviationAggregationBuilder aggregationBuilder = new MedianAbsoluteDeviationAggregationBuilder("foo").field(
            FIELD_NAME
        );

        testAggregation(aggregationBuilder, new DocValuesFieldExistsQuery(FIELD_NAME), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 7)));
            iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, 1)));
        }, agg -> {
            assertEquals(Double.NaN, agg.getMedianAbsoluteDeviation(), 0);
            assertFalse(AggregationInspectionHelper.hasValue(agg));
        }, null);
    }

    public void testUnmappedMissing() throws IOException {
        MedianAbsoluteDeviationAggregationBuilder aggregationBuilder = new MedianAbsoluteDeviationAggregationBuilder("foo").field(
            FIELD_NAME
        ).missing(1234);

        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 7)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("unrelatedField", 9)));
        }, agg -> {
            assertEquals(0, agg.getMedianAbsoluteDeviation(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(agg));
        }, null);
    }

    public void testValueScript() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD_NAME, NumberFieldMapper.NumberType.LONG);

        MedianAbsoluteDeviationAggregationBuilder aggregationBuilder = new MedianAbsoluteDeviationAggregationBuilder("foo").field(
            FIELD_NAME
        ).script(new Script(ScriptType.INLINE, MockScriptEngine.NAME, VALUE_SCRIPT, Collections.emptyMap()));

        final int size = randomIntBetween(100, 1000);
        final List<Long> sample = new ArrayList<>(size);
        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), randomSample(size, point -> {
            sample.add(point);
            return singleton(new SortedNumericDocValuesField(FIELD_NAME, point));
        }), agg -> {
            assertThat(agg.getMedianAbsoluteDeviation(), closeToRelative(calculateMAD(sample)));
            assertTrue(AggregationInspectionHelper.hasValue(agg));
        }, fieldType);
    }

    public void testSingleScript() throws IOException {
        MedianAbsoluteDeviationAggregationBuilder aggregationBuilder = new MedianAbsoluteDeviationAggregationBuilder("foo").script(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, SINGLE_SCRIPT, Collections.emptyMap())
        );

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD_NAME, NumberFieldMapper.NumberType.LONG);

        final int size = randomIntBetween(100, 1000);
        final List<Long> sample = new ArrayList<>(size);
        testAggregation(aggregationBuilder, new MatchAllDocsQuery(), iw -> {
            for (int i = 0; i < 10; i++) {
                iw.addDocument(singleton(new NumericDocValuesField(FIELD_NAME, i + 1)));
            }
        }, agg -> {
            assertEquals(0, agg.getMedianAbsoluteDeviation(), 0);
            assertTrue(AggregationInspectionHelper.hasValue(agg));
        }, fieldType);
    }

    private void testAggregation(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalMedianAbsoluteDeviation> verify
    ) throws IOException {
        MedianAbsoluteDeviationAggregationBuilder builder = new MedianAbsoluteDeviationAggregationBuilder("mad").field(FIELD_NAME)
            .compression(randomDoubleBetween(20, 1000, true));

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType(FIELD_NAME, NumberFieldMapper.NumberType.LONG);

        testAggregation(builder, query, buildIndex, verify, fieldType);
    }

    private void testAggregation(
        AggregationBuilder aggregationBuilder,
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> indexer,
        Consumer<InternalMedianAbsoluteDeviation> verify,
        MappedFieldType fieldType
    ) throws IOException {
        testCase(aggregationBuilder, query, indexer, verify, fieldType);
    }

    public static class IsCloseToRelative extends TypeSafeMatcher<Double> {

        private final double expected;
        private final double error;

        public IsCloseToRelative(double expected, double error) {
            this.expected = expected;
            this.error = error;
        }

        @Override
        protected boolean matchesSafely(Double actual) {
            final double deviation = Math.abs(actual - expected);
            final double observedError = deviation / Math.abs(expected);
            return observedError <= error;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("within ").appendValue(error * 100).appendText(" percent of ").appendValue(expected);
        }

        public static IsCloseToRelative closeToRelative(double expected, double error) {
            return new IsCloseToRelative(expected, error);
        }

        public static IsCloseToRelative closeToRelative(double expected) {
            return closeToRelative(expected, 0.25); // 0.25 = q(1-q) where q = quartile; for median, q = 0.5.
        }
    }

    /**
     * This class is an implementation of median absolute deviation that computes an exact value, rather than the approximation used in the
     * aggregation. It's used to verify that the aggregation's approximate results are close enough to the exact result
     */
    public static class ExactMedianAbsoluteDeviation {

        public static double calculateMAD(int[] sample) {
            return calculateMAD(Arrays.stream(sample).mapToDouble(point -> (double) point).toArray());
        }

        public static double calculateMAD(long[] sample) {
            return calculateMAD(Arrays.stream(sample).mapToDouble(point -> (double) point).toArray());
        }

        public static double calculateMAD(List<Long> sample) {
            return calculateMAD(sample.stream().mapToDouble(Long::doubleValue).toArray());
        }

        public static double calculateMAD(double[] sample) {
            final double median = calculateMedian(sample);

            final double[] deviations = Arrays.stream(sample).map(point -> Math.abs(median - point)).toArray();

            final double mad = calculateMedian(deviations);
            return mad;
        }

        private static double calculateMedian(double[] sample) {
            final double[] sorted = Arrays.copyOf(sample, sample.length);
            Arrays.sort(sorted);
            final int halfway = (int) Math.ceil(sorted.length / 2d);
            final double median;
            if (sorted.length % 2 == 0) {
                // even
                median = (sorted[halfway - 1] + sorted[halfway]) / 2d;
            } else {
                // odd
                median = (sorted[halfway - 1]);
            }
            return median;
        }
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return singletonList(CoreValuesSourceType.NUMERIC);
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new MedianAbsoluteDeviationAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected ScriptService getMockScriptService() {
        Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

        scripts.put(VALUE_SCRIPT, vars -> ((Number) vars.get("_value")).doubleValue() + 1);
        scripts.put(SINGLE_SCRIPT, vars -> 1);

        MockScriptEngine scriptEngine = new MockScriptEngine(MockScriptEngine.NAME, scripts, Collections.emptyMap());
        Map<String, ScriptEngine> engines = Collections.singletonMap(scriptEngine.getType(), scriptEngine);

        return new ScriptService(Settings.EMPTY, engines, ScriptModule.CORE_CONTEXTS);
    }
}
