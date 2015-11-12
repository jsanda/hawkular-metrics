/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.hawkular.metrics.component.bus.test
import com.google.common.base.Charsets
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test

import javax.jms.ConnectionFactory
import javax.jms.JMSConsumer
import javax.jms.JMSContext
import javax.jms.Topic
import javax.naming.Context
import javax.naming.InitialContext
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import static java.util.concurrent.TimeUnit.MINUTES
import static junit.framework.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertTrue
/**
 * @author Thomas Segismont
 */
class InsertedDataITest {
  static baseURI = System.getProperty('hawkular-metrics.base-uri') ?: '127.0.0.1:8080/hawkular/metrics'
  static final String TENANT_PREFIX = UUID.randomUUID().toString()
  static final AtomicInteger TENANT_ID_COUNTER = new AtomicInteger(0)
  static String tenantHeaderName = "Hawkular-Tenant";
  static RESTClient hawkularMetrics
  static defaultFailureHandler
  static final double DELTA = 0.001

  static InitialContext namingContext
  static ConnectionFactory connectionFactory

  JMSContext jmsContext

  @BeforeClass
  static void initClient() {
    hawkularMetrics = new RESTClient("http://$baseURI/", ContentType.JSON)
    defaultFailureHandler = hawkularMetrics.handler.failure
    hawkularMetrics.handler.failure = { resp ->
      def msg = "Got error response: ${resp.statusLine}"
      if (resp.entity != null && resp.entity.contentLength != 0) {
        def baos = new ByteArrayOutputStream()
        resp.entity.writeTo(baos)
        def entity = new String(baos.toByteArray(), Charsets.UTF_8)
        msg = """${msg}
=== Response body
${entity}
===
"""
      }
      System.err.println(msg)
      return resp
    }

    Properties env = new Properties();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory")
    env.put(Context.PROVIDER_URL, "http-remoting://127.0.0.1:8083")
    env.put(Context.SECURITY_PRINCIPAL, 'admin')
    env.put(Context.SECURITY_CREDENTIALS, 'redhat')
    namingContext = new InitialContext(env)
    connectionFactory = (ConnectionFactory) namingContext.lookup('jms/RemoteConnectionFactory')
  }

  static String nextTenantId() {
    return "T${TENANT_PREFIX}${TENANT_ID_COUNTER.incrementAndGet()}"
  }

  def tenantId = nextTenantId()

  @Before
  void setUp() {
    jmsContext = connectionFactory.createContext('admin', 'redhat')
  }

  @After
  void tearDown() {
    jmsContext.close()
  }

  @Test
  void testAvailData() {
    Topic topic = (Topic) namingContext.lookup('jms/topic/HawkularAvailData')
    JMSConsumer consumer = jmsContext.createConsumer(topic)
    CountDownLatch latch = new CountDownLatch(1)
    def json = null
    JsonSlurper jsonSlurper = new JsonSlurper()

    consumer.messageListener = { message ->
      json = jsonSlurper.parseText(message.text)
      println "RESPONSE = ${JsonOutput.prettyPrint(message.text)}"
      latch.countDown()
    }

    def metricName = 'test', timestamp = 13, value = 'UP'

    def response = hawkularMetrics.post(path: 'availability/data', body: [
        [
            id  : metricName,
            data: [[timestamp: timestamp, value: value]]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)


    assertTrue('No message received', latch.await(1, MINUTES))
    assertNotNull(json)

    assertEquals(metricName, json.id)
    assertEquals(1, json.data.size())
    assertEquals(timestamp, json.data[0].timestamp)
    assertEquals(value.toLowerCase(), json.data[0].value)
  }

  @Test
  void testNumericData() {
    Topic topic = (Topic) namingContext.lookup('jms/topic/HawkularMetricData')
    JMSConsumer consumer = jmsContext.createConsumer(topic)
    CountDownLatch latch = new CountDownLatch(1)
    def json = null
    JsonSlurper jsonSlurper = new JsonSlurper()

    consumer.messageListener = { message ->
      json = jsonSlurper.parseText(message.text)
      println "RESPONSE = ${JsonOutput.prettyPrint(message.text)}"
      latch.countDown()
    }


    def metricName = 'test', timestamp = 13, value = 15.3

    def response = hawkularMetrics.post(path: 'gauges/data', body: [
        [
            id  : metricName,
            data: [[timestamp: timestamp, value: value]]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)


    assertTrue('No message received', latch.await(1, MINUTES))
    assertNotNull(json)

    assertEquals(metricName, json.id)
    assertEquals(1, json.data.size())
    assertEquals(timestamp, json.data[0].timestamp)
    assertEquals(value, json.data[0].value, DELTA)
  }
}
