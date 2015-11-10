import com.google.common.base.Charsets
import groovy.json.JsonOutput
import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient
import org.hawkular.alerts.api.model.condition.ThresholdCondition
import org.hawkular.alerts.api.model.trigger.Trigger
import org.junit.BeforeClass
import org.junit.Test

import javax.jms.*
import javax.naming.Context
import javax.naming.InitialContext
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
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
/**
 * @author jsanda
 */
class DistITest {

  static String tenantHeaderName = "Hawkular-Tenant"
  static RESTClient metricsClient
  static RESTClient alertsClient

  static String tenant = '28026b36-8fe4-4332-84c8-524e173a68bf'

  @BeforeClass
  static void initClients() {
    String baseMetricsURI = System.getProperty('hawkular-metrics.base-uri') ?: '127.0.0.1:8080/hawkular/metrics'
    String baseAlertsURI = System.getProperty('hawkular-alerts.base-uri') ?: '127.0.0.1:8080/hawkular/alerts'

    def failureHandler = { resp ->
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

    metricsClient = new RESTClient("http://$baseMetricsURI/", ContentType.JSON)
    metricsClient.handler.failure = failureHandler

    alertsClient = new RESTClient("http://$baseAlertsURI/", ContentType.JSON)
    alertsClient.handler.failure = failureHandler
    alertsClient.headers.put("Hawkular-Tenant", tenant)
  }

  @Test
  void endToEndTest() {
    String metric = 'G1'

    waitForAlertsToInitialize()

    Trigger trigger = new Trigger("test-trigger-1", metric)
    trigger.enabled = true
    trigger.tags.metric = metric

    def resp = alertsClient.delete(path: "triggers/$trigger.id")
    assert(200 == resp.status || 404 == resp.status)

    resp = alertsClient.put(path: 'delete', query: [triggerIds: "$trigger.id"])
    assert(200 == resp.status || 405 == resp.status)

    resp = alertsClient.post(path: "triggers", body: trigger)
    assertEquals(200, resp.status)

    resp = alertsClient.put(path: "triggers/$trigger.id/conditions/firing", body: [
        new ThresholdCondition(trigger.id, trigger.name, ThresholdCondition.Operator.GT, 10.12)
    ])
    assertEquals(200, resp.status)
    assertEquals(1, resp.data.size())

    Properties env = new Properties();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory")
    env.put(Context.PROVIDER_URL, "http-remoting://127.0.0.1:8083")
    env.put(Context.SECURITY_PRINCIPAL, 'admin')
    env.put(Context.SECURITY_CREDENTIALS, 'redhat')

    InitialContext namingContext = new InitialContext(env)
    ConnectionFactory connectionFactory = (ConnectionFactory) namingContext.lookup('jms/RemoteConnectionFactory')
    JMSContext context = connectionFactory.createContext('admin', 'redhat')
    Topic topic = (Topic) namingContext.lookup('jms/topic/HawkularMetricData')

    JMSConsumer consumer = context.createConsumer(topic)
    CountDownLatch latch = new CountDownLatch(1)
    boolean messageReceived = false
    consumer.messageListener = { Message message ->
      messageReceived = true
      latch.countDown()
    }

    def response = metricsClient.post(path: "gauges/$metric/data", body: [
        [timestamp: System.currentTimeMillis(), value: 12.22],
        [timestamp: System.currentTimeMillis() - 500, value: 8.4],
    ], headers: [(tenantHeaderName): tenant])
    assertEquals(200, response.status)

    latch.await(5, TimeUnit.SECONDS)

    assertTrue(messageReceived)

    // Wait a little bit to allow time for the trigger to fire an alert
    Thread.sleep(3000)

    response = alertsClient.get(path: '', query: [tags:"metric|$metric",thin:true])
    assertEquals(200, response.status)

    assertEquals(1, response.data.size())

    response = metricsClient.get(path: "gauges/$metric", query: [detailed: true],
        headers: [(tenantHeaderName): tenant])
    assertEquals(200, response.status)

    String json = JsonOutput.toJson(response.data)
    String prettyJson = JsonOutput.prettyPrint(json)
    println "RESPONSE = ${JsonOutput.prettyPrint(json)}"

    assertEquals("The id property does not match. The actual response is,\n$prettyJson", metric, response.data.id)
    assertEquals("The tenantId property does not match. The actual response is, \n$prettyJson", tenant,
        response.data.tenantId)
    assertEquals("The type property does not match. The actual response is,\n$prettyJson", "gauge", response.data.type)

    assertEquals("Expected to get 1 alert. The actual response is,\n$prettyJson", 1, response.data.alerts.size())

    def actualAlertJson = response.data.alerts[0]

    // The dataId property corresponds to the "event" which in this case is the trigger
    assertEquals("The alert.dataId property does not match. The actual response is,\n$prettyJson", trigger.id,
        actualAlertJson.dataId)
    assertEquals("The alert.severity property does not match. The actual response is,\n$prettyJson", 'MEDIUM',
        actualAlertJson.severity)
    assertEquals("The alert.status property does not match. The actual response is,\n$prettyJson", 'OPEN',
        actualAlertJson.status)
    assertEquals("The alert.trigger.id property does not match. The actual response is,\n$prettyJson", trigger.id,
        actualAlertJson.trigger.id)
    assertEquals("The alert.trigger.name property does not match. The actual response is,\n$prettyJson", trigger.name,
        actualAlertJson.trigger.name)
  }

  static void waitForAlertsToInitialize() {
    def response = alertsClient.get(path: 'plugins')
    int attempts = 0
    while (response.status != 200 && attempts++ < 20) {
      Thread.sleep(1000)
      response = alertsClient.get(path: 'plugins')
    }

    assertEquals('Failed to verify that Hawkular Alerts is initialized', 200, response.status)
  }

}
