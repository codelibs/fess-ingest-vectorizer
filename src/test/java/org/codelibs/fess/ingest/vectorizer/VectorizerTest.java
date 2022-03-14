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

import org.codelibs.curl.Curl;
import org.codelibs.curl.CurlResponse;
import org.dbflute.utflute.core.PlainTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class VectorizerTest extends PlainTestCase {
    private static final Logger logger = LoggerFactory.getLogger(VectorizerTest.class);

    private static final String VERSION = "snapshot";

    private static final String IMAGE_TAG = "ghcr.io/codelibs/fess-text-vectorizer:" + VERSION;

    GenericContainer vectorizingServer;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        vectorizingServer = new GenericContainer<>(DockerImageName.parse(IMAGE_TAG))//
                .withExposedPorts(8900);
        vectorizingServer.start();

        final String url = getServerUrl();
        logger.info("Vectorizing Server:  " + url);
        for (int i = 0; i < 10; i++) {
            try (CurlResponse response = Curl.get(url).header("Content-Type", "application/json").execute()) {
                if (response.getHttpStatusCode() == 200) {
                    logger.info(url + " is available.");
                    break;
                }
            } catch (Exception e) {
                logger.info(e.getLocalizedMessage());
            }
            try {
                logger.info("Waiting for " + url);
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // nothing
            }
        }

    }

    private String getServerUrl() {
        return "http://" + vectorizingServer.getHost() + ":" + vectorizingServer.getFirstMappedPort();
    }

    @Override
    protected void tearDown() throws Exception {
        vectorizingServer.stop();
        super.tearDown();
    }

    public void test_isActive() {
        Vectorizer vectorizer = Vectorizer.create().url(getServerUrl()).build();
        assertTrue(vectorizer.isActive());
    }

    public void test_vectorize_en() {
        Vectorizer vectorizer = Vectorizer.create().url(getServerUrl()).build();
        Map<String, Object> input = new HashMap<>();
        input.put("lang", "en");
        input.put("content", "this is a pen.");
        Map<String, float[]> output = vectorizer.vectorize(input);
        float[] values = output.get("content");
        assertEquals(768, values.length);
    }

    public void test_vectorize_en_empty() {
        Vectorizer vectorizer = Vectorizer.create().url(getServerUrl()).build();
        Map<String, Object> input = new HashMap<>();
        input.put("lang", "en");
        input.put("content", "");
        Map<String, float[]> output = vectorizer.vectorize(input);
        float[] values = output.get("content");
        assertEquals(0, values.length);
    }

    public void test_vectorize_ja() {
        Vectorizer vectorizer = Vectorizer.create().url(getServerUrl()).build();
        Map<String, Object> input = new HashMap<>();
        input.put("lang", "ja");
        input.put("content", "これはペンです。");
        Map<String, float[]> output = vectorizer.vectorize(input);
        float[] values = output.get("content");
        assertEquals(768, values.length);
    }

    public void test_vectorize_ja_empty() {
        Vectorizer vectorizer = Vectorizer.create().url(getServerUrl()).build();
        Map<String, Object> input = new HashMap<>();
        input.put("lang", "ja");
        input.put("content", "");
        Map<String, float[]> output = vectorizer.vectorize(input);
        float[] values = output.get("content");
        assertEquals(0, values.length);
    }

    public void test_vectorize_xx() {
        Vectorizer vectorizer = Vectorizer.create().url(getServerUrl()).build();
        Map<String, Object> input = new HashMap<>();
        input.put("lang", "xx");
        input.put("content", "this is a pen.");
        Map<String, float[]> output = vectorizer.vectorize(input);
        float[] values = output.get("content");
        assertEquals(768, values.length);
    }

}
