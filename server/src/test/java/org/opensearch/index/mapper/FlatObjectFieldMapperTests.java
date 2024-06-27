/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.CharFilterFactory;
import org.opensearch.index.analysis.CustomAnalyzer;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.LowercaseNormalizer;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.index.query.QueryShardContext;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.opensearch.index.mapper.FlatObjectFieldMapper.TypeParser.DEPTH_LIMIT;
import static org.opensearch.index.mapper.FlatObjectFieldMapper.TypeParser.IGNORE_ABOVE;
import static org.opensearch.index.mapper.FlatObjectFieldMapper.TypeParser.NORMALIZER;
import static org.opensearch.index.mapper.FlatObjectFieldMapper.TypeParser.NULL_VALUE;
import static org.opensearch.index.mapper.FlatObjectFieldMapper.TypeParser.SIMILARITY;
import static org.opensearch.index.mapper.TypeParsers.DOC_VALUES;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.IsEqual.equalTo;

public class FlatObjectFieldMapperTests extends MapperTestCase {
    private static final String FIELD_TYPE = "flat_object";
    private static final String VALUE_AND_PATH_SUFFIX = "._valueAndPath";
    private static final String VALUE_SUFFIX = "._value";

    protected boolean supportsMeta() {
        return false;
    }

    protected boolean supportsOrIgnoresBoost() {
        return false;
    }

    public void testMapperServiceHasParser() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> { minimalMapping(b); }));
        Mapper.TypeParser parser = mapperService.mapperRegistry.getMapperParsers().get(FIELD_TYPE);
        assertNotNull(parser);
        assertTrue(parser instanceof FlatObjectFieldMapper.TypeParser);
    }

    protected void assertExistsQuery(MapperService mapperService) throws IOException {
        ParseContext.Document fields = mapperService.documentMapper().parse(source(this::writeField)).rootDoc();
        QueryShardContext queryShardContext = createQueryShardContext(mapperService);
        MappedFieldType fieldType = mapperService.fieldType("field");
        Query query = fieldType.existsQuery(queryShardContext);
        assertExistsQuery(fieldType, query, fields);

    }

    protected void assertExistsQuery(MappedFieldType fieldType, Query query, ParseContext.Document fields) {
        // we always perform a term query against _field_names, even when the field
        // is not added to _field_names because it is not indexed nor stored
        assertThat(query, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) query;
        assertEquals(FieldNamesFieldMapper.NAME, termQuery.getTerm().field());
        assertEquals("field", termQuery.getTerm().text());
        if (fieldType.isSearchable() || fieldType.isStored()) {
            assertNotNull(fields.getField(FieldNamesFieldMapper.NAME));
        } else {
            assertNoFieldNamesField(fields);
        }
    }

    @Override
    public void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", FIELD_TYPE);
    }

    /**
     * Writes a sample value for the field to the provided {@link XContentBuilder}.
     * @param builder builder
     */
    protected void writeFieldValue(XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("foo", "bar");
        builder.endObject();
    }

    @Override
    public void testMinimalToMaximal() throws IOException {
        XContentBuilder orig = JsonXContent.contentBuilder().startObject();
        createMapperService(fieldMapping(this::minimalMapping)).documentMapper().mapping().toXContent(orig, ToXContent.EMPTY_PARAMS);
        orig.endObject();
        XContentBuilder parsedFromOrig = JsonXContent.contentBuilder().startObject();
        createMapperService(orig).documentMapper().mapping().toXContent(parsedFromOrig, ToXContent.EMPTY_PARAMS);
        parsedFromOrig.endObject();
        assertEquals(orig.toString(), parsedFromOrig.toString());
        assertParseMaximalWarnings();
    }

    public void testDefaults() throws Exception {
        XContentBuilder mapping = fieldMapping(this::minimalMapping);
        DocumentMapper mapper = createDocumentMapper(mapping);
        assertEquals(mapping.toString(), mapper.mappingSource().toString());

        String json = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field("foo", "bar")
            .endObject()
            .endObject()
            .toString();

        ParsedDocument doc = mapper.parse(source(json));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef("field.foo"), fields[0].binaryValue());

        IndexableFieldType fieldType = fields[0].fieldType();
        assertFalse(fieldType.tokenized());
        assertFalse(fieldType.stored());
        assertThat(fieldType.indexOptions(), equalTo(IndexOptions.DOCS));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());
        fieldType = fields[1].fieldType();
        assertThat(fieldType.indexOptions(), Matchers.equalTo(IndexOptions.NONE));
        assertEquals(DocValuesType.SORTED_SET, fieldType.docValuesType());

        // Test internal substring fields as well
        IndexableField[] fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
        assertEquals(2, fieldValues.length);
        assertTrue(fieldValues[0] instanceof KeywordFieldMapper.KeywordField);
        assertEquals(new BytesRef("bar"), fieldValues[0].binaryValue());

        IndexableField[] fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
        assertEquals(2, fieldValues.length);
        assertTrue(fieldValueAndPaths[0] instanceof KeywordFieldMapper.KeywordField);
        assertEquals(new BytesRef("field.foo=bar"), fieldValueAndPaths[0].binaryValue());
    }

    public void testIgnoreAbove() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "flat_object").field("ignore_above", 5)));

        String json = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field("foo", "bar")
            .endObject()
            .endObject()
            .toString();
        ParsedDocument doc = mapper.parse(source(json));

        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        IndexableField[] fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
        assertEquals(2, fieldValues.length);

        IndexableField[] fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
        assertEquals(2, fieldValueAndPaths.length);

        json = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field("foo", "opensearch")
            .endObject()
            .endObject()
            .toString();
        doc = mapper.parse(source(json));
        fields = doc.rootDoc().getFields("field");
        assertEquals(0, fields.length);
        fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
        assertEquals(0, fieldValues.length);

        fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
        assertEquals(0, fieldValueAndPaths.length);
    }

    public void testNullValue() throws IOException {
        // test null_value = null
        {
            // test: {"field":null}
            DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
            ParsedDocument doc = mapper.parse(source(b -> b.nullField("field")));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

            // test: {"field":[null]}
            doc = mapper.parse(source(b -> b.array("field", (String[]) null)));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

            // test: {"field":{"name":null, "age":"5", "name1":null}}
            String json = "{\"field\":{\"name\":null, \"age\":\"5\", \"name1\":null}}";
            doc = mapper.parse(source(json));
            IndexableField[] fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
            IndexableField[] fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("5"), fieldValues[0].binaryValue());
            IndexableField[] fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=5"), fieldValueAndPaths[0].binaryValue());

            // test: {"field":{"name": [null, {"d":{"name":"dsds"}}]}}
            json = "{\"field\":{\"name\": [null, {\"d\":{\"name\":\"dsds\"}}]}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(6, fields.length);
            assertEquals(new BytesRef("field.name"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.d"), fields[2].binaryValue());
            assertEquals(new BytesRef("field.name"), fields[4].binaryValue());

            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("dsds"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.name.d.name=dsds"), fieldValueAndPaths[0].binaryValue());

            // test: {"field":{"name": [{"d":{"name":"dsds"}}, null]}}
            json = "{\"field\":{\"name\": [{\"d\":{\"name\":\"dsds\"}}, null]}}";
            doc = mapper.parse(source(json));
            IndexableField[] fields1 = doc.rootDoc().getFields("field");
            assertEquals(fields1.length, fields.length);
            for (int i = 0; i < fields1.length; i++) {
                assertEquals(fields[i].toString(), fields1[i].toString());
            }
            assertEquals(6, fields.length);
            assertEquals(new BytesRef("field.name"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.d"), fields[2].binaryValue());
            assertEquals(new BytesRef("field.name"), fields[4].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("dsds"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.name.d.name=dsds"), fieldValueAndPaths[0].binaryValue());

            // test: {"field":{"name": [{"name":"age1"}, {"d":{"name":"dsds"}}]}}
            json = "{\"field\":{\"name\": [{\"name\":\"age1\"}, {\"d\":{\"name\":\"dsds\"}}]}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");

            assertEquals(8, fields.length);
            assertEquals(new BytesRef("field.name"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.name"), fields[2].binaryValue());
            assertEquals(new BytesRef("field.d"), fields[4].binaryValue());
            assertEquals(new BytesRef("field.name"), fields[6].binaryValue());

            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(4, fieldValues.length);
            assertEquals(new BytesRef("age1"), fieldValues[0].binaryValue());
            assertEquals(new BytesRef("dsds"), fieldValues[2].binaryValue());

            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(4, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.name.name=age1"), fieldValueAndPaths[0].binaryValue());
            assertEquals(new BytesRef("field.name.d.name=dsds"), fieldValueAndPaths[2].binaryValue());

            // test: {"field":{"name": null,"age":3}}
            json = "{\"field\":{\"name\": null,\"age\":3}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("3"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=3"), fieldValueAndPaths[0].binaryValue());

            // test: {"field":{"name": null,"age":3}}
            json = "{\"field\":{\"name\": null,\"age\":3}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("3"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=3"), fieldValueAndPaths[0].binaryValue());

            // test: {"field":{"name": [null,3],"age":4}}
            json = "{\"field\":{\"name\": [null,3],\"age\":4}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(4, fields.length);
            assertEquals(new BytesRef("field.name"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.age"), fields[2].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(4, fieldValues.length);
            assertEquals(new BytesRef("3"), fieldValues[0].binaryValue());
            assertEquals(new BytesRef("4"), fieldValues[2].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(4, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.name=3"), fieldValueAndPaths[0].binaryValue());
            assertEquals(new BytesRef("field.age=4"), fieldValueAndPaths[2].binaryValue());

            // test: {"field":{"age": 4,"name": [null,"3"]}}
            json = "{\"field\":{\"age\": 4,\"name\": [null,\"3\"]}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(4, fields.length);
            assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
            assertEquals(new BytesRef("field.name"), fields[2].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(4, fieldValues.length);
            assertEquals(new BytesRef("4"), fieldValues[0].binaryValue());
            assertEquals(new BytesRef("3"), fieldValues[2].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(4, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=4"), fieldValueAndPaths[0].binaryValue());
            assertEquals(new BytesRef("field.name=3"), fieldValueAndPaths[2].binaryValue());

            // test: {"field":{"labels": [null]}}
            json = "{\"field\":{\"labels\": [null]}}";
            doc = mapper.parse(source(json));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

            // test: {"field":{"name":"abc","labels": [null]}}
            json = "{\"field\":{\"name\":\"abc\",\"labels\": [null]}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            assertEquals(new BytesRef("field.name"), fields[0].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("abc"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.name=abc"), fieldValueAndPaths[0].binaryValue());

            // test: {"field":{"r1": {"labels": [null]}}}
            json = "{\"field\":{\"r1\": {\"labels\": [null]}}}";
            doc = mapper.parse(source(json));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

            // test: {"field":{"r1": {"name": null,"labels": [null]}}}
            json = "{\"field\":{\"r1\": {\"name\": null,\"labels\": [null]}}}";
            doc = mapper.parse(source(json));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field"));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_SUFFIX));
            assertArrayEquals(new IndexableField[0], doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX));

            // test: {"field":{"age":"4","labels": [null]}}
            json = "{\"field\":{\"age\":\"4\",\"labels\": [null]}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("4"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=4"), fieldValueAndPaths[0].binaryValue());

            // test: {"field":{"labels": [null], "age":"4"}}
            json = "{\"field\":{\"labels\": [null], \"age\":\"4\"}}";
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(2, fields.length);
            assertEquals(new BytesRef("field.age"), fields[0].binaryValue());
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertEquals(new BytesRef("4"), fieldValues[0].binaryValue());
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(2, fieldValueAndPaths.length);
            assertEquals(new BytesRef("field.age=4"), fieldValueAndPaths[0].binaryValue());

        }

        // test null_value = bar
        {
            DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "flat_object").field("null_value", "bar")));
            ParsedDocument doc = mapper.parse(source(b -> {}));

            IndexableField[] fields = doc.rootDoc().getFields("field");
            assertEquals(0, fields.length);

            // test: {"field":null}
            doc = mapper.parse(source(b -> b.nullField("field")));
            fields = doc.rootDoc().getFields("field");
            assertEquals(0, fields.length);

            IndexableField[] fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(2, fieldValues.length);
            assertTrue(fieldValues[0] instanceof KeywordFieldMapper.KeywordField);
            assertEquals(new BytesRef("bar"), fieldValues[0].binaryValue());

            IndexableField[] fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(0, fieldValueAndPaths.length);

            // test: {"field":{"foo":null}}
            String json = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("field")
                .field("foo", (String) null)
                .endObject()
                .endObject()
                .toString();
            doc = mapper.parse(source(json));
            fields = doc.rootDoc().getFields("field");
            assertEquals(0, fields.length);
            fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
            assertEquals(0, fieldValues.length);
            fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
            assertEquals(0, fieldValueAndPaths.length);

        }
    }

    public void testDisableDocValues() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "flat_object").field("doc_values", false)));
        String json = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field("foo", "bar")
            .endObject()
            .endObject()
            .toString();
        ParsedDocument doc = mapper.parse(source(json));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(1, fields.length);
        assertEquals(DocValuesType.NONE, fields[0].fieldType().docValuesType());

        IndexableField[] fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
        assertEquals(1, fieldValues.length);
        assertTrue(fieldValues[0] instanceof KeywordFieldMapper.KeywordField);
        assertEquals(new BytesRef("bar"), fieldValues[0].binaryValue());

        IndexableField[] fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
        assertEquals(1, fieldValueAndPaths.length);
        assertTrue(fieldValues[0] instanceof KeywordFieldMapper.KeywordField);
        assertEquals(new BytesRef("bar"), fieldValues[0].binaryValue());
    }

    public void testNormalizer() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "flat_object").field("normalizer", "lowercase")));
        String json = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field("Foo", "Bar")
            .endObject()
            .endObject()
            .toString();
        ParsedDocument doc = mapper.parse(source(json));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef("field.foo"), fields[0].binaryValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.indexOptions(), Matchers.equalTo(IndexOptions.DOCS));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        IndexableField[] fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
        assertEquals(2, fieldValues.length);
        assertEquals(new BytesRef("bar"), fieldValues[0].binaryValue());

        IndexableField[] fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
        assertEquals(2, fieldValueAndPaths.length);
        assertTrue(fieldValueAndPaths[0] instanceof KeywordFieldMapper.KeywordField);
        assertEquals(new BytesRef("field.foo=bar"), fieldValueAndPaths[0].binaryValue());
    }

    public void testDepthLimit() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(b -> b.field("type", "flat_object").field("depth_limit", "2")));
        String json = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .field("Foo", "Bar")
            .endObject()
            .endObject()
            .toString();
        ParsedDocument doc = mapper.parse(source(json));
        IndexableField[] fields = doc.rootDoc().getFields("field");
        assertEquals(2, fields.length);
        assertEquals(new BytesRef("field.Foo"), fields[0].binaryValue());
        IndexableFieldType fieldType = fields[0].fieldType();
        assertThat(fieldType.indexOptions(), Matchers.equalTo(IndexOptions.DOCS));
        assertEquals(DocValuesType.NONE, fieldType.docValuesType());

        IndexableField[] fieldValues = doc.rootDoc().getFields("field" + VALUE_SUFFIX);
        assertEquals(2, fieldValues.length);
        assertEquals(new BytesRef("Bar"), fieldValues[0].binaryValue());

        IndexableField[] fieldValueAndPaths = doc.rootDoc().getFields("field" + VALUE_AND_PATH_SUFFIX);
        assertEquals(2, fieldValueAndPaths.length);
        assertTrue(fieldValueAndPaths[0] instanceof KeywordFieldMapper.KeywordField);
        assertEquals(new BytesRef("field.Foo=Bar"), fieldValueAndPaths[0].binaryValue());

        // beyond depth_limit
        String json1 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("field")
            .startObject("field1")
            .field("Foo", "Bar")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        MapperParsingException e = expectThrows(MapperParsingException.class, () -> mapper.parse(source(json1)));
        assertThat(
            e.getRootCause().getMessage(),
            Matchers.containsString("the depth of flat_object field path [field.field1] is bigger than maximum depth [2]")
        );
    }

    public void testUpdateNormalizer() throws IOException {
        MapperService mapperService = createMapperService(
            fieldMapping(b -> b.field("type", "flat_object").field("normalizer", "lowercase"))
        );
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "flat_object").field("normalizer", "other_lowercase")))
        );
        assertEquals(
            "Mapper for [field] conflicts with existing mapping:\n"
                + "[mapper [field] has different [analyzer], mapper [field] has different [normalizer]]",
            e.getMessage()
        );
    }

    public void testConfigureSimilarity() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(b -> b.field("type", "flat_object").field("similarity", "boolean")));
        MappedFieldType ft = mapperService.documentMapper().fieldTypes().get("field");
        assertEquals("boolean", ft.getTextSearchInfo().getSimilarity().name());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> merge(mapperService, fieldMapping(b -> b.field("type", "flat_object").field("similarity", "BM25")))
        );
        assertThat(e.getMessage(), Matchers.containsString("mapper [field] has different [similarity]"));
    }

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {
        checker.registerConflictCheck(DOC_VALUES, b -> b.field(DOC_VALUES, false));
        checker.registerConflictCheck(NULL_VALUE, b -> b.field(NULL_VALUE, "foo"));
        checker.registerConflictCheck(NORMALIZER, b -> b.field(NORMALIZER, "lowercase"));
        checker.registerUpdateCheck(b -> b.field(DEPTH_LIMIT, 34), m -> assertEquals(34, ((FlatObjectFieldMapper) m).getDepthLimit()));
        checker.registerUpdateCheck(b -> b.field(IGNORE_ABOVE, 256), m -> assertEquals(256, ((FlatObjectFieldMapper) m).ignoreAbove()));
        checker.registerConflictCheck(SIMILARITY, b -> b.field(SIMILARITY, "boolean"));
    }

    @Override
    protected IndexAnalyzers createIndexAnalyzers(IndexSettings indexSettings) {
        return new IndexAnalyzers(
            singletonMap("default", new NamedAnalyzer("default", AnalyzerScope.INDEX, new StandardAnalyzer())),
            Map.of(
                "lowercase",
                new NamedAnalyzer("lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer()),
                "other_lowercase",
                new NamedAnalyzer("other_lowercase", AnalyzerScope.INDEX, new LowercaseNormalizer())
            ),
            singletonMap(
                "lowercase",
                new NamedAnalyzer(
                    "lowercase",
                    AnalyzerScope.INDEX,
                    new CustomAnalyzer(
                        TokenizerFactory.newFactory("lowercase", WhitespaceTokenizer::new),
                        new CharFilterFactory[0],
                        new TokenFilterFactory[] { new TokenFilterFactory() {

                            @Override
                            public String name() {
                                return "lowercase";
                            }

                            @Override
                            public TokenStream create(TokenStream tokenStream) {
                                return new LowerCaseFilter(tokenStream);
                            }
                        } }
                    )
                )
            )
        );
    }

}
