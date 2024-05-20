package org.apache.activemq.artemis.tests.integration.amqp;

import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.runners.MethodSorters;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.utils.cli.helper.HelperCreate;
import org.apache.qpid.jms.JmsConnectionFactory;

import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ActiveMQServers;

import org.apache.activemq.artemis.util.ServerUtil;

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

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ErwinTest extends AmqpTestSupport {

	// there are 3 installation methods:
	// 1 --> use brokers from the project dev-environment (AMQP-only, needs port-forwarding)
	//			uses 55673 and 55671
	// 2 --> use embedded brokers in this test-suite (AMQP+CORE)
	//			uses 61616 and 61617
	// 3 --> use external brokers in this test-suite (AMQP+CORE)
	//			uses 61618 and 61619
	private static final int METHOD = 1;



	// ----- NO configuration below this point -----

	protected static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

	private static JMSException jmsException = null;
	private static AssertionError assertionError = null;

	private void shellCommand(String cmd) throws Exception {
		System.out.println("--> " + cmd);
		Process proc = Runtime.getRuntime().exec(cmd);
		InputStream o = proc.getInputStream();
		for(;;) { int c = o.read(); if(c < 0) break; System.out.print((char)c); }
		InputStream e = proc.getErrorStream();
		for(;;) { int c = e.read(); if(c < 0) break; System.out.print((char)c); }
		proc.waitFor();
	}

	private ActiveMQServer senderServer = null;
	private ActiveMQServer receiverServer = null;

	private void before2() throws Exception {
		// remove any previous instance
		shellCommand("rm -rf target/data");

		// Configure senderServer
		Configuration senderServerConfig = new ConfigurationImpl()
			.setPersistenceEnabled(true)
			.setJournalDirectory("target/data/journal1")
			.setBindingsDirectory("target/data/bindings1")
			.setPagingDirectory("target/data/paging1")
			.setLargeMessagesDirectory("target/data/largemessages1")
			.addAcceptorConfiguration("tcp", "tcp://localhost:61616")
			.addConnectorConfiguration("senderServer-connector", "tcp://localhost:61616")
			.addConnectorConfiguration("receiverServer-connector", "tcp://localhost:61617")
			.setClusterUser("aap")
			.setClusterPassword("noot")
			.setSecurityEnabled(false)
			.addClusterConfiguration(new ClusterConnectionConfiguration()
					.setName("clusterA")
					.setConnectorName("senderServer-connector")
					.setRetryInterval(1000)
					.setReconnectAttempts(-1)
					.setStaticConnectors(Arrays.asList(new String[]{"receiverServer-connector"})));

		// Configure receiverServer
		Configuration receiverServerConfig = new ConfigurationImpl()
			.setPersistenceEnabled(true)
			.setJournalDirectory("target/data/journal2")
			.setBindingsDirectory("target/data/bindings2")
			.setPagingDirectory("target/data/paging2")
			.setLargeMessagesDirectory("target/data/largemessages2")
			.addAcceptorConfiguration("tcp", "tcp://localhost:61617")
			.addConnectorConfiguration("senderServer-connector", "tcp://localhost:61616")
			.addConnectorConfiguration("receiverServer-connector", "tcp://localhost:61617")
			.setClusterUser("aap")
			.setClusterPassword("noot")
			.setSecurityEnabled(false)
			.addClusterConfiguration(new ClusterConnectionConfiguration()
					.setName("clusterA")
					.setConnectorName("receiverServer-connector")
					.setRetryInterval(1000)
					.setReconnectAttempts(-1)
					.setStaticConnectors(Arrays.asList(new String[]{"senderServer-connector"})));

		// Create and start servers
		senderServer = ActiveMQServers.newActiveMQServer(senderServerConfig);
		senderServer.start();
		receiverServer = ActiveMQServers.newActiveMQServer(receiverServerConfig);
		receiverServer.start();
	}

	private void after2() throws Exception {
		// cleanup internal brokers
		senderServer.stop();
		senderServer = null;
		receiverServer.stop();
		receiverServer = null;
	}

	private Process senderProcess = null;
	private Process receiverProcess = null;

	private void before3() throws Exception {
		// remove data from the previous instance
		shellCommand("rm -rf brokerA/data brokerA/log brokerA/lock; mkdir brokerA/data");
		shellCommand("rm -rf brokerB/data brokerA/log brokerA/lock; mkdir brokerB/data");

		senderProcess = ServerUtil.startServer("brokerA", "brokerA2");
		ServerUtil.waitForServerToStart(2, 60000); // 61616+2
		receiverProcess = ServerUtil.startServer("brokerB", "brokerB2");
		ServerUtil.waitForServerToStart(3, 60000); // 61616+3
	}

	private void after3() throws Exception {
		// cleanup external brokers
		ServerUtil.killServer(senderProcess);
		senderProcess = null;
		ServerUtil.killServer(receiverProcess);
		receiverProcess = null;
	}

	public void testHelper(String protocol, int nrConsumers, int nrStartedConsumers, boolean isOnSameNode) throws Exception {

		final int NRMESSAGES = 10000000;

		final String ADDRESS = (METHOD == 1) ? "pubsub/obis/toshore/1234/rtm/events.v1" : "address1";

		System.out.printf("\n===== method %d, protocol %s, with %s consumers", METHOD, protocol, nrConsumers);
		if(nrStartedConsumers != nrConsumers) System.out.printf(", only %d started", nrStartedConsumers);
		System.out.printf(", producer/consumers on %s node", isOnSameNode ? "SAME" : "DIFFERENT");
		System.out.printf(" =====\n");

		if (METHOD == 1 && protocol == "CORE") { System.out.printf("CORE not supported for our real brokers, ignoring this test\n"); return; }
		if (METHOD == 2) before2();
		if (METHOD == 3) before3();

		// create producer connection
		ConnectionFactory senderConnectionFactory = null;
		if (METHOD == 1 && protocol == "AMQP") senderConnectionFactory = new JmsConnectionFactory("amqp://localhost:55673");
		else if (METHOD == 2 && protocol == "AMQP") senderConnectionFactory = new JmsConnectionFactory("amqp://localhost:61616");
		else if (METHOD == 2 && protocol == "CORE") senderConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
		else if (METHOD == 3 && protocol == "AMQP") senderConnectionFactory = new JmsConnectionFactory("amqp://localhost:61618");
		else if (METHOD == 3 && protocol == "CORE") senderConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61618");
		else senderConnectionFactory = null; // crash with NPE for unsupported combinations
		final Connection senderConnection = senderConnectionFactory.createConnection("ond", "2C41559524C6C2070F793E485E3D1BAEshoreond");
		senderConnection.setClientID("sender");
		senderConnection.start();
		final Session senderSession = senderConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		final Destination senderDestination = senderSession.createTopic(ADDRESS);
		final MessageProducer producer = senderSession.createProducer(senderDestination);

		// create consumer connection(s)
		final Connection receiverConnection[] = new Connection[nrConsumers];
		final Session receiverSession[] = new Session[nrConsumers];
		final MessageConsumer consumer[] = new MessageConsumer[nrConsumers];
		for (int c = 0; c < nrConsumers; c++) {
			ConnectionFactory receiverConnectionFactory;
			if (isOnSameNode) receiverConnectionFactory = senderConnectionFactory;
			else if (METHOD == 1 && protocol == "AMQP") receiverConnectionFactory = new JmsConnectionFactory("amqp://localhost:55671");
			else if (METHOD == 2 && protocol == "AMQP") receiverConnectionFactory = new JmsConnectionFactory("amqp://localhost:61617");
			else if (METHOD == 2 && protocol == "CORE") receiverConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61617");
			else if (METHOD == 3 && protocol == "AMQP") receiverConnectionFactory = new JmsConnectionFactory("amqp://localhost:61619");
			else if (METHOD == 3 && protocol == "CORE") receiverConnectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61619");
			else receiverConnectionFactory = null; // crash with NPE for unsupported combinations
			receiverConnection[c] = receiverConnectionFactory.createConnection("ond", "2C41559524C6C2070F793E485E3D1BAEshoreond");
			receiverConnection[c].setClientID("receiver" + (c + 1));
			receiverConnection[c].start();
			receiverSession[c] = receiverConnection[c].createSession(false, Session.AUTO_ACKNOWLEDGE);
			final Destination receiverDestination = receiverSession[c].createTopic(ADDRESS);
			consumer[c] = receiverSession[c].createConsumer(receiverDestination);
		}

		jmsException = null; // fresh start
		assertionError = null; // fresh start

		// create producer thread
		Thread senderThread = new Thread(){
			public void run() {
				try {
					for(int m = 0; m < NRMESSAGES && jmsException == null && assertionError == null; m++) {
						Message message = senderSession.createTextMessage("msg#" + m);
						producer.send(message, DeliveryMode.NON_PERSISTENT, 4, 60000);
					}
				} catch (JMSException ex) {
					jmsException = ex;
				} catch (AssertionError ex) {
					assertionError = ex;
				}
			}
		};

		// create consumer threads
		Thread receiverThread[] = new Thread[nrStartedConsumers];
		for (int c = 0; c < nrStartedConsumers; c++) {
			final int cc = c;
			receiverThread[cc] = new Thread(){
				public void run() {
					try {
						for(int j = 0; j < NRMESSAGES && jmsException == null && assertionError == null; j++) {
							Message message = consumer[cc].receive(60000);
							assertNotNull(message);
							String expectedId = "msg#" + j;
							String receivedId = ((TextMessage)message).getText();
							assertNotNull(receivedId);
							assertEquals(expectedId, receivedId);
						}
						// verify that no extra messages were produced
						// short timeout, because we don't expect any
						Message message = consumer[cc].receive(100);
						assertNull(message);
					} catch (JMSException ex) {
						jmsException = ex;
					} catch (AssertionError ex) {
						assertionError = ex;
					}
				}
			};
		}

		// start the show
		for (int c = 0; c < nrStartedConsumers; c++) {
			receiverThread[c].start();
		}
		Thread.sleep(3000); // give the consumers a head-start
		senderThread.start();

		// wait for all to finish
		senderThread.join();
		for (int c = 0; c < nrStartedConsumers; c++) {
			receiverThread[c].join();
		}

		// cleanup all
		producer.close();
		senderSession.close();
		senderConnection.close();
		for (int c = 0; c < nrConsumers; c++) {
			consumer[c].close();
			receiverSession[c].close();
			receiverConnection[c].close();
		}

		if (METHOD == 2) after2();
		if (METHOD == 3) after3();

		// re-raise any errors found in any of the (now finished) threads
		if(assertionError != null) {
			assertionError.printStackTrace(System.err);
			assertNull(assertionError);
		}
		if(jmsException != null) {
			jmsException.printStackTrace(System.err);
			assertNull(jmsException);
		}
	}

	@Ignore("this one always succeeds")
	@Test
	public void testCase1a() throws Exception {
		// 1 producer, 1 consumer, expect success
		testHelper("CORE", 1, 1, true);
	}

	@Test
	@Ignore("this one always succeeds")
	public void testCase1b() throws Exception {
		// 1 producer, 1 consumer, expect success
		testHelper("AMQP", 1, 1, true);
	}

	@Test
	@Ignore("this one always succeeds")
	public void testCase1c() throws Exception {
		// 1 producer, 1 consumer, expect success
		testHelper("CORE", 1, 1, false);
	}

	@Test
	@Ignore("this one always succeeds")
	public void testCase1d() throws Exception {
		// 1 producer, 1 consumer, expect success
		testHelper("AMQP", 1, 1, false);
	}

	@Test
	@Ignore("this one always succeeds")
	public void testCase2a() throws Exception {
		// 1 producer, 2 fast consumers, expect success
		testHelper("CORE", 2, 2, true);
	}

	@Test
	@Ignore("this one always succeeds")
	public void testCase2b() throws Exception {
		// 1 producer, 2 fast consumers, expect success
		testHelper("AMQP", 2, 2, true);
	}

	@Test
	@Ignore("this one always succeeds")
	public void testCase2c() throws Exception {
		// 1 producer, 2 fast consumers, expect success
		testHelper("CORE", 2, 2, false);
	}

	@Test
	@Ignore("this one always succeeds")
	public void testCase2d() throws Exception {
		// 1 producer, 2 fast consumers, expect success
		testHelper("AMQP", 2, 2, false);
	}

	@Test
	@Ignore("this one always succeeds")
	public void testCase3a() throws Exception {
		// 1 producer, 1 fast consumer, 1 slow consumer, expect success
		testHelper("CORE", 2, 1, true);
	}

	@Test
	@Ignore("this one always succeeds")
	public void testCase3b() throws Exception {
		// 1 producer, 1 fast consumer, 1 slow consumer, expect success
		testHelper("AMQP", 2, 1, true);
	}

	@Test
	@Ignore("this one behaves same as testCase3d")
	public void testCase3c() throws Exception {
		// 1 producer, 1 fast consumer, 1 slow consumer, expect FAILURE
		testHelper("CORE", 2, 1, false);
	}

	@Test
	public void testCase3d() throws Exception {
		// 1 producer, 1 fast consumer, 1 slow consumer, expect FAILURE
		testHelper("AMQP", 2, 1, false);
	}
}
// vi:ts=3:sw=3
