import groovyx.net.http.ContentType
import groovyx.net.http.RESTClient
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
  static RESTClient hawkularMetrics

  @Test
  void listenToTopic() {
    String baseURI = System.getProperty('hawkular-metrics.base-uri') ?: '127.0.0.1:8080/hawkular/metrics'
    hawkularMetrics = new RESTClient("http://$baseURI/", ContentType.JSON)

    Properties env = new Properties();
    env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");
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

    String tenantId = 'TEST_TENANT'
    def response = hawkularMetrics.post(path: "gauges/G1/data", body: [
        [timestamp: System.currentTimeMillis(), value: 12.22],
        [timestamp: System.currentTimeMillis() - 500, value: 15.37],
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    latch.await(5, TimeUnit.SECONDS)

    assertTrue(messageReceived)
  }

}
