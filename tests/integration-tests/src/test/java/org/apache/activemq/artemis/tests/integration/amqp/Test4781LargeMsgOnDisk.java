/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.artemis.tests.integration.amqp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.protocol.amqp.broker.ProtonProtocolManagerFactory;
import org.apache.activemq.artemis.tests.integration.cluster.distribution.ClusterTestBase;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test4781LargeMsgOnDisk extends ClusterTestBase {

   protected static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

   @Test
   public void test4781() throws Exception {

      // which message sizes to send
      final int MESSAGE_SIZE_START = 90 * 1024;
      final int MESSAGE_SIZE_END = 110 * 1024;
      final int MESSAGE_SIZE_INC = 50;

      ClusterConnectionConfiguration[] clusterConf = new ClusterConnectionConfiguration[2];

      for (int serverNr = 0; serverNr < 2; serverNr++) {
         // start with basic server
         setupServer(serverNr, true, true);

         // the reproducer does not reproduce without this:
         // the default is 102400 but this is in the same range as the message sizes here
         getServer(serverNr).getConfiguration().setJournalFileSize(10 * 1024 * 1024);

         // make sure we can use the AMQP protocol (Proton = AMQP)
         getServer(serverNr).addProtocolManagerFactory(new ProtonProtocolManagerFactory());

         // configuration for the broker
         Map<String, Object> params = generateParams(serverNr, false);

         // we need CORE for cluster connections and AMQP for client connections
         params.put("protocols", "CORE,AMQP");

         // create the acceptors for the broker
         getServer(serverNr).getConfiguration().getAcceptorConfigurations()
               .add(createTransportConfiguration(false, true, params));

         // setup the whole configuration for the broker
         clusterConf[serverNr] = new ClusterConnectionConfiguration() //
               .setName("broker" + serverNr);
      }

      // make it a proper cluster by connecting the nodes
      setupClusterConnection(clusterConf[0], true, 0, 1);
      setupClusterConnection(clusterConf[1], true, 1, 0);

      // start all nodes
      startServers(0);
      startServers(1);

      // create producer connection+session+producer
      final ConnectionFactory senderConnectionFactory = new JmsConnectionFactory("amqp://localhost:61616");
      final Connection senderConnection = senderConnectionFactory.createConnection();
      senderConnection.setClientID("sender");
      senderConnection.start();
      final Session senderSession = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final Destination senderDestination = senderSession.createTopic("address1");
      final MessageProducer producer = senderSession.createProducer(senderDestination);

      // create consumer connection+session+consumer
      final ConnectionFactory receiverConnectionFactory = new JmsConnectionFactory("amqp://localhost:61617");
      final Connection receiverConnection = receiverConnectionFactory.createConnection();
      receiverConnection.setClientID("receiver");
      receiverConnection.start();
      final Session receiverSession = receiverConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final Destination receiverDestination = receiverSession.createTopic("address1");
      final MessageConsumer consumer = receiverSession.createConsumer(receiverDestination);

      class MyThread extends Thread {
         // remember which exceptions happened in this thread
         protected JMSException jmsException = null;
         protected AssertionError assertionError = null;
      }

      MyThread[] threads = new MyThread[2]; // 1 sender + 1 receiver

      // create producer thread
      threads[0] = new MyThread() {
         @Override
         public void run() {
            try {
               for (int size = MESSAGE_SIZE_START; size <= MESSAGE_SIZE_END; size += MESSAGE_SIZE_INC) {
                  // create simple message
                  TextMessage msg = senderSession.createTextMessage();

                  // give it a header...
                  String msgName = "msg#" + size;
                  msg.setStringProperty("msgName", msgName);

                  // ...and a payload of the given size
                  msg.setText("x".repeat(size));

                  // send it, prio=4 (standard), ttl=30s
                  producer.send(msg, DeliveryMode.NON_PERSISTENT, 4, 30000);
                  //System.out.println("--> " + msgName);
               }
               // cleanup
               producer.close();
               senderSession.close();
               senderConnection.close();
            } catch (JMSException ex) {
               jmsException = ex;
            } catch (AssertionError ex) {
               assertionError = ex;
            }
         }
      };

      // create consumer thread
      threads[1] = new MyThread() {
         @Override
         public void run() {
            try {
               for (int size = MESSAGE_SIZE_START; size <= MESSAGE_SIZE_END; size += MESSAGE_SIZE_INC) {
                  // receive message
                  Message msg = consumer.receive(60000);
                  assertNotNull(msg);

                  // verify header
                  String expectedMsgName = "msg#" + size;
                  String receivedMsgName = msg.getStringProperty("msgName");
                  //System.out.println("<-- " + receivedMsgName);
                  assertNotNull(receivedMsgName);
                  assertEquals(expectedMsgName, receivedMsgName);

                  // verify payload size
                  String txt = ((TextMessage) msg).getText();
                  assertEquals(size, txt.length());
               }
               // cleanup
               consumer.close();
               receiverSession.close();
               receiverConnection.close();
            } catch (JMSException ex) {
               jmsException = ex;
            } catch (AssertionError ex) {
               assertionError = ex;
            }
         }
      };

      // give the brokers a bit of time to set up their connections and acceptors
      Thread.sleep(3000);

      // start the show, but give the consumers a head-start
      threads[1].start();
      Thread.sleep(3000);
      threads[0].start();

      // wait for all threads to finish
      threads[0].join();
      threads[1].join();

      // re-raise any errors found in any of the (now finished) threads
      for (MyThread thread : threads) {
         if (thread.assertionError != null) {
            thread.assertionError.printStackTrace(System.err);
            assertNull(thread.assertionError);
         }
         if (thread.jmsException != null) {
            thread.jmsException.printStackTrace(System.err);
            assertNull(thread.jmsException);
         }
      }

      // allow visual inspection
      // just an "ls -l" of the large-messages directories of both brokers
      // no files should be left as the consumer should have consumed all messages
      // so any file that is left is a problem
      // just execute a simple pwd+ls command (before the broker instance is removed again)
      String[] cmd = new String[]{"sh", "-c", "pwd; ls -l target/tmp/Test4781LargeMsgOnDisk/junit*/large*; strings target/tmp/Test4781LargeMsgOnDisk/junit*/large*/* | fgrep 'msg#'"};
      Process process = Runtime.getRuntime().exec(cmd);
      String line;
      BufferedReader readerErr = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      while ((line = readerErr.readLine()) != null) {
         System.out.println("ERR ==> " + line);
      }
      BufferedReader readerOut = new BufferedReader(new InputStreamReader(process.getInputStream()));
      while ((line = readerOut.readLine()) != null) {
         System.out.println("OUT ==> " + line);
      }
   }
}

// End
// vim: ts=3 sw=3 expandtab
