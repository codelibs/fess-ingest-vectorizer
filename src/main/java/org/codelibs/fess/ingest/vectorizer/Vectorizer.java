/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ingest.vectorizer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.text.StringEscapeUtils;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlResponse;
import org.codelibs.fess.util.DocumentUtil;
import org.codelibs.opensearch.runner.net.OpenSearchCurl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Vectorizer {
    private static final Logger logger = LoggerFactory.getLogger(Vectorizer.class);

    private static final String DEFAULT_LANG = "en";

    protected Set<String> supportedLanguages = Collections.emptySet();

    protected String url;

    protected String[] fields;

    protected int dimension;

    protected float[] emptyValue;

    protected void initialize() {
        try (CurlResponse response = Curl.get(url).header("Content-Type", "application/json").execute()) {
            if (response.getHttpStatusCode() == 200) {
                final Map<String, Object> contentMap = response.getContent(OpenSearchCurl.jsonParser());
                @SuppressWarnings("unchecked")
                final List<String> values = (List<String>) contentMap.get("languages");
                supportedLanguages = new HashSet<>(values);
            } else {
                logger.warn("Failed to access to {} : {}", url, response.getContentAsString());
            }
        } catch (final IOException e) {
            logger.warn("Failed to access to {}", url, e);
        }
        emptyValue = new float[dimension];
    }

    protected Map<String, float[]> emtpyResult() {
        final Map<String, float[]> map = new HashMap<>(fields.length);
        for (final String f : fields) {
            map.put(f, emptyValue);
        }
        return map;
    }

    public Map<String, float[]> vectorize(final Map<String, Object> input) {
        String lang = DocumentUtil.getValue(input, "lang", String.class);
        if (StringUtil.isBlank(lang)) {
            if (logger.isDebugEnabled()) {
                logger.debug("lang is empty.");
            }
            return emtpyResult();
        }

        if (!supportedLanguages.contains(lang)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Unsupported lang: {}", lang);
            }
            lang = DEFAULT_LANG;
        }

        final StringBuilder bodyBuf = new StringBuilder(1000);
        bodyBuf.append("{\"data\":[");
        bodyBuf.append('{');
        for (final String field : fields) {
            final String value = DocumentUtil.getValue(input, field, String.class, StringUtil.EMPTY);
            bodyBuf.append("\"").append(StringEscapeUtils.escapeJson(field)).append("\":\"").append(StringEscapeUtils.escapeJson(value))
                    .append("\",");
        }
        bodyBuf.append("\"lang\":\"").append(StringEscapeUtils.escapeJson(lang)).append("\"");
        bodyBuf.append('}');
        bodyBuf.append("]}");

        if (logger.isDebugEnabled()) {
            logger.debug(">>> {}", bodyBuf.toString());
        }

        try (CurlResponse response =
                Curl.post(url + "/vectorize").header("Content-Type", "application/json").body(bodyBuf.toString()).execute()) {
            if (response.getHttpStatusCode() == 200) {
                final Map<String, Object> contentMap = response.getContent(OpenSearchCurl.jsonParser());
                if (logger.isDebugEnabled()) {
                    logger.debug("<<< {}", response.getContentAsString());
                }
                @SuppressWarnings("unchecked")
                final List<Map<String, Object>> results = (List<Map<String, Object>>) contentMap.get("results");
                if (!results.isEmpty()) {
                    final Map<String, Object> result = results.get(0);
                    final Map<String, float[]> output = new HashMap<>(fields.length);
                    for (final String field : fields) {
                        @SuppressWarnings("unchecked")
                        final List<Number> values = (List<Number>) result.get(field);
                        final float[] data = new float[values.size()];
                        for (int i = 0; i < data.length; i++) {
                            data[i] = values.get(i).floatValue();
                        }
                        output.put(field, data);
                    }
                    return output;
                }
                logger.warn("No vectorizing data.");
            } else {
                logger.warn("Failed to vectorize: {}", response.getContentAsString());
            }
        } catch (final IOException e) {
            logger.warn("Failed to access to {}", url, e);
        }

        return emtpyResult();
    }

    public boolean isActive() {
        try (CurlResponse response = Curl.get(url + "/ping").header("Content-Type", "application/json").execute()) {
            if (response.getHttpStatusCode() != 200) {
                return false;
            }
            final Map<String, Object> contentMap = response.getContent(OpenSearchCurl.jsonParser());
            final Object status = contentMap.get("status");
            if (!"ok".equals(status)) {
                return false;
            }
        } catch (final IOException e) {
            return false;
        }
        return true;
    }

    public String[] getFields() {
        return fields;
    }

    public static Builder create() {
        return new Builder();
    }

    public static class Builder {
        private String url = "http://localhost:8900";
        private String[] fields = { "content" };
        private int dimension = 768;

        protected Builder() {
            // nothing
        }

        public Builder url(final String url) {
            if (StringUtil.isNotBlank(url)) {
                this.url = url;
            }
            return this;
        }

        public Builder fields(final String fields) {
            final String[] values = StreamUtil.split(fields, ",")
                    .get(stream -> stream.map(String::trim).filter(StringUtil::isNotEmpty).toArray(n -> new String[n]));
            if (values.length > 0) {
                return fields(values);
            }
            return this;
        }

        public Builder fields(final String... fields) {
            this.fields = fields;
            return this;
        }

        public Builder dimension(final int dimension) {
            this.dimension = dimension;
            return this;
        }

        public Vectorizer build() {
            final Vectorizer instance = new Vectorizer();
            instance.url = this.url;
            instance.fields = this.fields;
            instance.dimension = this.dimension;
            instance.initialize();
            return instance;
        }
    }
}
