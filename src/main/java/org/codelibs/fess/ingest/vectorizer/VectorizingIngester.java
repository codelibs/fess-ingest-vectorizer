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

import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.codelibs.fess.ingest.Ingester;
import org.codelibs.fess.util.ComponentUtil;

public class VectorizingIngester extends Ingester {
    protected Vectorizer vectorizer;

    protected String fieldSuffix = "_vector";

    @PostConstruct
    public void init() {
        vectorizer = Vectorizer.create()//
                .url(ComponentUtil.getFessConfig().getSystemProperty("ingest.vectorizer.url"))//
                .fields(ComponentUtil.getFessConfig().getSystemProperty("ingest.vectorizer.fields"))//
                .build();
    }

    @PreDestroy
    public void destroy() {
        // nothing
    }

    @Override
    protected Map<String, Object> process(final Map<String, Object> target) {
        vectorizer.vectorize(target).entrySet().stream().forEach(e -> target.put(e.getKey() + fieldSuffix, e.getValue()));
        return target;
    }

    public void setFieldSuffix(final String fieldSuffix) {
        this.fieldSuffix = fieldSuffix;
    }

}
