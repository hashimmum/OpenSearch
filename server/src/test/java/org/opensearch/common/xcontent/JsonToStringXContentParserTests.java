/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

import org.mockito.Mockito;

import static org.mockito.Mockito.when;

public class JsonToStringXContentParserTests extends OpenSearchTestCase {

    private String flattenJsonString(String fieldName, String in, int depthLimit, String nullValue, int ignoreAbove) throws IOException {
        String transformed;
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                in
            )
        ) {
            JsonToStringXContentParser jsonToStringXContentParser = new JsonToStringXContentParser(
                xContentRegistry(),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                parser,
                fieldName,
                depthLimit,
                nullValue,
                ignoreAbove
            );
            // Skip the START_OBJECT token:
            jsonToStringXContentParser.nextToken();

            XContentParser transformedParser = jsonToStringXContentParser.parseObject();
            try (XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()) {
                jsonBuilder.copyCurrentStructure(transformedParser);
                return jsonBuilder.toString();
            }
        }
    }

    public void testNestedObjects() throws IOException {
        String jsonExample = "{" + "\"first\" : \"1\"," + "\"second\" : {" + "  \"inner\":  \"2.0\"" + "}," + "\"third\": \"three\"" + "}";

        assertEquals(
            "{"
                + "\"flat\":[\"first\",\"second\",\"inner\",\"third\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner=2.0\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 100)
        );
    }

    public void testChildHasDots() throws IOException {
        // This should be exactly the same as testNestedObjects. We're just using the "flat" notation for the inner
        // object.
        String jsonExample = "{" + "\"first\" : \"1\"," + "\"second.inner\" : \"2.0\"," + "\"third\": \"three\"" + "}";

        assertEquals(
            "{"
                + "\"flat\":[\"first\",\"second\",\"inner\",\"third\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner=2.0\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 100)
        );
    }

    public void testNestChildObjectWithDots() throws IOException {
        String jsonExample = "{"
            + "\"first\" : \"1\","
            + "\"second.inner\" : {"
            + "  \"really_inner\" : \"2.0\""
            + "},"
            + "\"third\": \"three\""
            + "}";

        assertEquals(
            "{"
                + "\"flat\":[\"first\",\"second\",\"inner\",\"really_inner\",\"third\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner.really_inner=2.0\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 100)
        );
    }

    public void testNestChildObjectWithDotsAndFieldWithDots() throws IOException {
        String jsonExample = "{"
            + "\"first\" : \"1\","
            + "\"second.inner\" : {"
            + "  \"totally.absolutely.inner\" : \"2.0\""
            + "},"
            + "\"third\": \"three\""
            + "}";

        assertEquals(
            "{"
                + "\"flat\":[\"first\",\"second\",\"inner\",\"totally\",\"absolutely\",\"inner\",\"third\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner.totally.absolutely.inner=2.0\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 100)
        );
    }

    public void testDepthLimit() throws IOException {
        String jsonExample = "{"
            + "\"first\" : \"1\","
            + "\"second.inner\" : {"
            + "  \"totally.absolutely.inner\" : \"2.0\""
            + "},"
            + "\"third\": \"three\""
            + "}";
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> flattenJsonString("flat", jsonExample, 2, null, 100));
        assertThat(
            e.getRootCause().getMessage(),
            Matchers.containsString("the depth of flat_object field path [flat.second.inner] is bigger than maximum depth [2]")
        );
        assertEquals(
            "{"
                + "\"flat\":[\"first\",\"second\",\"inner\",\"totally\",\"absolutely\",\"inner\",\"third\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner.totally.absolutely.inner=2.0\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 3, null, 100)
        );
    }

    public void testIgnoreAbove() throws IOException {
        String jsonExample = "{"
            + "\"first\" : \"1\","
            + "\"second.inner\" : {"
            + "  \"totally.absolutely.inner\" : \"2.0\""
            + "},"
            + "\"third\": \"three\""
            + "}";

        assertEquals(
            "{"
                + "\"flat\":[\"first\",\"second\",\"inner\",\"totally\",\"absolutely\",\"inner\",\"third\"],"
                + "\"flat._value\":[\"1\",\"2.0\",\"three\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner.totally.absolutely.inner=2.0\",\"flat.third=three\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 5)
        );

        assertEquals(
            "{"
                + "\"flat\":[\"first\",\"second\",\"inner\",\"totally\",\"absolutely\",\"inner\"],"
                + "\"flat._value\":[\"1\",\"2.0\"],"
                + "\"flat._valueAndPath\":[\"flat.first=1\",\"flat.second.inner.totally.absolutely.inner=2.0\"]"
                + "}",
            flattenJsonString("flat", jsonExample, 5, null, 4)
        );
    }

    public void testNullValue() throws IOException {

        XContentParser mapper = Mockito.mock(XContentParser.class);
        when(mapper.currentToken()).thenReturn(XContentParser.Token.VALUE_NULL);

        JsonToStringXContentParser jsonToStringXContentParser = new JsonToStringXContentParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            mapper,
            "flat",
            5,
            "ddd",
            100
        );

        XContentParser transformedParser = jsonToStringXContentParser.parseObject();
        try (XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()) {
            jsonBuilder.copyCurrentStructure(transformedParser);
            assertEquals("{\"flat\":[],\"flat._value\":[\"ddd\"],\"flat._valueAndPath\":[]}", jsonBuilder.toString());
        }

        jsonToStringXContentParser = new JsonToStringXContentParser(
            xContentRegistry(),
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            mapper,
            "flat",
            5,
            null,
            100
        );

        transformedParser = jsonToStringXContentParser.parseObject();
        try (XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()) {
            jsonBuilder.copyCurrentStructure(transformedParser);
            assertEquals("{\"flat\":[],\"flat._value\":[],\"flat._valueAndPath\":[]}", jsonBuilder.toString());
        }
    }
}
