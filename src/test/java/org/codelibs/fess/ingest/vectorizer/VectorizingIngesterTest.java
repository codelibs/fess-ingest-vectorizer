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

import java.util.HashMap;
import java.util.Map;

import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastaflute.LastaFluteTestCase;

public class VectorizingIngesterTest extends LastaFluteTestCase {

    private VectorizingIngester ingester;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        ComponentUtil.setFessConfig(new FessConfig.SimpleImpl() {
            private static final long serialVersionUID = 1L;

            @Override
            public String getSystemProperty(final String key) {
                return "";
            }
        });

        ingester = new VectorizingIngester();
        ingester.vectorizer = new Vectorizer() {
            public Map<String, float[]> vectorize(Map<String, Object> input) {
                Map<String, float[]> map = new HashMap<>();
                map.put("content", new float[10]);
                return map;
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void test_process() {
        Map<String, Object> input = new HashMap<>();
        input.put("lang", "en");
        input.put("content", "test");
        Map<String, Object> output = ingester.process(input);
        assertEquals("test", output.get("content"));
        assertEquals(10, ((float[]) output.get("content_vector")).length);
    }
}
