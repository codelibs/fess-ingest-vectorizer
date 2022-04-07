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
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.codelibs.fesen.client.EngineInfo.EngineType;
import org.codelibs.fess.es.client.SearchEngineClient;
import org.codelibs.fess.ingest.Ingester;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse;
import org.opensearch.action.admin.indices.mapping.get.GetFieldMappingsResponse.FieldMappingMetadata;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VectorizingIngester extends Ingester {
    private static final Logger logger = LoggerFactory.getLogger(VectorizingIngester.class);

    protected Vectorizer vectorizer;

    protected String fieldSuffix = "_vector";

    @PostConstruct
    public void init() {
        EngineType engineType = getEngineType();
        if (engineType == EngineType.OPENSEARCH1) {
            logger.info("Search Engine: {}", engineType);
            final int dimension =
                    Integer.parseInt(ComponentUtil.getFessConfig().getSystemProperty("semantic_search.vectorizer.dimension", "768"));
            final String url = ComponentUtil.getFessConfig().getSystemProperty("semantic_search.vectorizer.url");
            final String fields = ComponentUtil.getFessConfig().getSystemProperty("semantic_search.vectorizer.fields");
            vectorizer = Vectorizer.create()//
                    .url(url)//
                    .fields(fields)//
                    .dimension(dimension)//
                    .build();
            createFields(dimension);
        } else {
            logger.warn("Your search engine is not supported: {}", engineType);
        }
    }

    protected EngineType getEngineType() {
        return ComponentUtil.getSearchEngineClient().getEngineInfo().getType();
    }

    protected void createFields(final int dimension) {
        for (String field : vectorizer.getFields()) {
            for (String lang : vectorizer.getLanguages()) {
                createField(field + "_" + lang + fieldSuffix, dimension);
            }
        }
    }

    protected void createField(final String field, final int dimension) {
        FessConfig fessConfig = ComponentUtil.getFessConfig();
        SearchEngineClient client = ComponentUtil.getSearchEngineClient();
        String alias = fessConfig.getIndexDocumentUpdateIndex();

        GetFieldMappingsResponse fieldMappingsResponse = client.admin().indices().prepareGetFieldMappings()//
                .setIndices(alias)//
                .setFields(field)//
                .execute().actionGet();
        final Map<String, Map<String, Map<String, FieldMappingMetadata>>> mappings = fieldMappingsResponse.mappings();
        mappings.keySet().stream().forEach(index -> {
            Map<String, Map<String, FieldMappingMetadata>> fieldMappings = mappings.get(index);
            if (!fieldMappings.isEmpty()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} field exists in {} index.", field, index);
                }
                return;
            }

            try {
                final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                        .startObject()//
                        .startObject("properties")//
                        .startObject(field)//
                        .field("type", "knn_vector")//
                        .field("dimension", dimension)//
                        .endObject()//
                        .endObject()//
                        .endObject();
                final String source = BytesReference.bytes(mappingBuilder).utf8ToString();
                AcknowledgedResponse response =
                        client.admin().indices().preparePutMapping(index).setSource(source, XContentType.JSON).execute().actionGet();
                if (response.isAcknowledged()) {
                    logger.info("{} field is created in {} index.", field, index);
                } else {
                    logger.warn("Failed to create {} field in {} index.", field, index);
                }
            } catch (IOException e) {
                logger.warn("Failed to create {} field in {} index.", field, index, e);
            }
        });
    }

    @PreDestroy
    public void destroy() {
        // nothing
    }

    @Override
    protected Map<String, Object> process(final Map<String, Object> target) {
        if (vectorizer != null) {
            vectorizer.getLanguage(target).ifPresent(lang -> vectorizer.vectorize(target).entrySet().stream().forEach(e -> {
                target.put(e.getKey() + "_" + lang + fieldSuffix, e.getValue());
            }));
        }
        return target;
    }

    public void setFieldSuffix(final String fieldSuffix) {
        this.fieldSuffix = fieldSuffix;
    }

}
