/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.lookup.SourceLookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * This method performs type inference by analyzing the _source documents. It uses a random sample of documents to infer the field type, similar to dynamic mapping type guessing logic.
 * Unlike guessing based on the first document, where field could be missing, this method generates a random sample to make a more accurate inference.
 * This approach is especially useful for handling missing fields, which is common in nested fields within derived fields of object types.
 *
 * <p>The sample size should be chosen carefully to ensure a high probability of selecting at least one document where the field is present.
 * However, it's essential to strike a balance because a large sample size can lead to performance issues since each sample document's _source field is loaded and examined until the field is found.
 *
 * <p>Determining the sample size (<var>S</var>) is akin to deciding how many balls to draw from a bin, ensuring a high probability (>=<var>P</var>) of drawing at least one green ball (documents with the field) from a mixture of <var>R</var> red balls (documents without the field) and <var>G</var> green balls:
 * <pre>{@code
 * P >= 1 - C(R, S) / C(R + G, S)
 * }</pre>
 * Here, <var>C()</var> represents the binomial coefficient.
 * For a high confidence level, we aim for <var>P</var> >= 0.95. For example, with 10^7 documents where the field is present in 2% of them, the sample size <var>S</var> should be around 149 to achieve a probability of 0.95.
 */
public class FieldTypeInference {
    private final IndexReader indexReader;
    private final String indexName;
    private final MapperService mapperService;
    // TODO expose using a index setting
    private int sampleSize;
    private static final int DEFAULT_SAMPLE_SIZE = 150;
    private static final int MAX_SAMPLE_SIZE_ALLOWED = 1000;

    public FieldTypeInference(String indexName, MapperService mapperService, IndexReader indexReader) {
        this.indexName = indexName;
        this.mapperService = mapperService;
        this.indexReader = indexReader;
        this.sampleSize = DEFAULT_SAMPLE_SIZE;
    }

    public void setSampleSize(int sampleSize) {
        this.sampleSize = Math.min(sampleSize, MAX_SAMPLE_SIZE_ALLOWED);
    }

    public int getSampleSize() {
        return sampleSize;
    }

    public Mapper infer(ValueFetcher valueFetcher) throws IOException {
        RandomSourceValuesGenerator valuesGenerator = new RandomSourceValuesGenerator(sampleSize, indexReader, valueFetcher);
        Mapper inferredMapper = null;
        while (inferredMapper == null && valuesGenerator.hasNext()) {
            List<Object> values = valuesGenerator.next();
            if (values == null || values.isEmpty()) {
                continue;
            }
            // always use first value in case of multi value field to infer type
            inferredMapper = inferTypeFromObject(values.get(0));
        }
        return inferredMapper;
    }

    private Mapper inferTypeFromObject(Object o) throws IOException {
        if (o == null) {
            return null;
        }
        DocumentMapper mapper = mapperService.documentMapper();
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().field("field", o).endObject();
        BytesReference bytesReference = BytesReference.bytes(builder);
        SourceToParse sourceToParse = new SourceToParse(indexName, "_id", bytesReference, JsonXContent.jsonXContent.mediaType());
        ParsedDocument parsedDocument = mapper.parse(sourceToParse);
        Mapping mapping = parsedDocument.dynamicMappingsUpdate();
        return mapping.root.getMapper("field");
    }

    private static class RandomSourceValuesGenerator implements Iterator<List<Object>> {
        private final ValueFetcher valueFetcher;
        private final IndexReader indexReader;
        private final SourceLookup sourceLookup;
        private final int numLeaves;
        private final int[] docs;
        private int iter;
        private int offset;
        private LeafReaderContext leafReaderContext;
        private int leaf;
        private final int MAX_ATTEMPTS_TO_GENERATE_RANDOM_SAMPLES = 10000;

        public RandomSourceValuesGenerator(int sampleSize, IndexReader indexReader, ValueFetcher valueFetcher) {
            this.valueFetcher = valueFetcher;
            this.indexReader = indexReader;
            sampleSize = Math.min(sampleSize, indexReader.numDocs());
            this.docs = getSortedRandomNum(
                sampleSize,
                indexReader.numDocs(),
                Math.max(sampleSize, MAX_ATTEMPTS_TO_GENERATE_RANDOM_SAMPLES)
            );
            this.iter = 0;
            this.offset = 0;
            this.leaf = 0;
            this.numLeaves = indexReader.leaves().size();
            this.sourceLookup = new SourceLookup();
            this.leafReaderContext = indexReader.leaves().get(leaf);
            valueFetcher.setNextReader(leafReaderContext);
        }

        @Override
        public boolean hasNext() {
            return iter < docs.length && leaf < numLeaves;
        }

        /**
         * Ensure hasNext() is called before calling next()
         */
        @Override
        public List<Object> next() {
            int docID = docs[iter] - offset;
            if (docID >= leafReaderContext.reader().numDocs()) {
                setNextLeaf();
                return next();
            }
            // deleted docs are getting used to infer type, which should be okay?
            sourceLookup.setSegmentAndDocument(leafReaderContext, docID);
            try {
                iter++;
                return valueFetcher.fetchValues(sourceLookup);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void setNextLeaf() {
            offset += leafReaderContext.reader().numDocs();
            leaf++;
            if (leaf < numLeaves) {
                leafReaderContext = indexReader.leaves().get(leaf);
                valueFetcher.setNextReader(leafReaderContext);
            }
        }

        private static int[] getSortedRandomNum(int sampleSize, int upperBound, int attempts) {
            Set<Integer> generatedNumbers = new HashSet<>();
            Random random = new Random();
            int itr = 0;
            while (generatedNumbers.size() < sampleSize && itr++ < attempts) {
                int randomNumber = random.nextInt(upperBound);
                generatedNumbers.add(randomNumber);
            }
            int[] result = new int[generatedNumbers.size()];
            int i = 0;
            for (int number : generatedNumbers) {
                result[i++] = number;
            }
            Arrays.sort(result);
            return result;
        }
    }
}
