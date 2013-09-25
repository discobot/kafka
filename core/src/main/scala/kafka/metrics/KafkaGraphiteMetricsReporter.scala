/**
 *
 *
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.metrics

import com.yammer.metrics.Metrics
import com.yammer.metrics.reporting.GraphiteReporter
import java.util.concurrent.TimeUnit
import kafka.utils.{Utils, VerifiableProperties, Logging}


private trait KafkaGraphiteMetricsReporterMBean extends KafkaMetricsReporterMBean


private class KafkaGraphiteMetricsReporter extends KafkaMetricsReporter
with KafkaGraphiteMetricsReporterMBean
with Logging {

  private var graphiteServer: String = null
  private var graphitePort: Int = 0
  private var graphitePrefix: String = null
  private var underlying: GraphiteReporter = null
  private var running = false
  private var initialized = false


  override def getMBeanName = "kafka:type=kafka.metrics.KafkaGraphiteMetricsReporter"


  override def init(props: VerifiableProperties) {
    info("REPORTER INIT")
    synchronized {
      if (!initialized) {
        val metricsConfig = new KafkaMetricsConfig(props)
        graphiteServer = new String(props.getString("kafka.graphite.metrics.server", "localhost"))
        graphitePort = props.getInt("kafka.graphite.metrics.port", 42000)
        graphitePrefix = new String(props.getString("kafka.graphite.metrics.prefix", "one_min.kafka_localhost"))
        info("Creating Graphite Reporter on host %s:%d with prefix %s".format(graphiteServer, graphitePort, graphitePrefix))

        underlying = new GraphiteReporter (Metrics.defaultRegistry(), graphiteServer, graphitePort, graphitePrefix)
        if (props.getBoolean("kafka.graphite.metrics.reporter.enabled", default = false)) {
          initialized = true
          startReporter(metricsConfig.pollingIntervalSecs)
        }
      }
    }
  }


  override def startReporter(pollingPeriodSecs: Long) {
    synchronized {
      if (initialized && !running) {
        underlying.start(pollingPeriodSecs, TimeUnit.SECONDS)
        running = true
        info("Started Kafka Graphite metrics reporter with polling period %d seconds".format(pollingPeriodSecs))
      }
    }
  }


  override def stopReporter() {
    synchronized {
      if (initialized && running) {
        underlying.shutdown()
        running = false
        info("Stopped Kafka Graphite metrics reporter")
        underlying = new GraphiteReporter (Metrics.defaultRegistry(), graphiteServer, graphitePort, graphitePrefix)
      }
    }
  }
}

