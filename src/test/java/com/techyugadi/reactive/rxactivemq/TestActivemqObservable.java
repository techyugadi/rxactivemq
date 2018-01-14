package com.techyugadi.reactive.rxactivemq;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.techyugadi.reactive.rxactivemq.ActivemqObservable;

import io.reactivex.Observable;
import io.reactivex.Flowable;
import io.reactivex.BackpressureStrategy;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;

import junit.framework.TestCase;

public class TestActivemqObservable extends TestCase {
	
	private static Logger LOG = 
			LoggerFactory.getLogger(TestActivemqObservable.class);
	
	public void testObservable() throws Exception {
		
		LOG.info("Unit Test ActivemqObservable");
		
		BrokerService brokerService = new BrokerService();
		brokerService.setPersistent(false);
		brokerService.setUseJmx(false);
	    brokerService.addConnector("vm://localhost:61616");
	    brokerService.start();
	    
	    Properties jmsProps = new Properties();
		jmsProps.setProperty("brokerURL", "vm://localhost:61616");
		jmsProps.setProperty("destinationName", "testQ");
		jmsProps.setProperty("destinationType", "Queue");
		jmsProps.setProperty("clientId", "testClient");
		jmsProps.setProperty("acknowledgeMode", "AUTO_ACKNOWLEDGE");
		
		jmsProps.setProperty("checkPropertyName", "STOP");
		jmsProps.setProperty("checkPropertyValue", "yes");
		
		ActivemqObservable activemqObservable = 
								new ActivemqObservable(jmsProps);
		
		Observable<Message> observable = 
				activemqObservable.retrieveObservable();

		List<Message> observed = new ArrayList<Message>();
		observable.subscribe(observed::add);
		
		ActiveMQConnectionFactory connectionFactory = 
				new ActiveMQConnectionFactory(
						"vm://" + brokerService.getBrokerName() + 
													"?create=false");
        Connection connection = connectionFactory.createConnection();
        connection.start();
 
        Session session = connection.createSession(false,
                							Session.AUTO_ACKNOWLEDGE);
        
        try {
            Queue destination = session.createQueue("testQ");
            MessageProducer producer = session.createProducer(destination);
            
            Message message = session.createTextMessage("MSG#1");
            producer.send(message);
            message = session.createTextMessage("MSG#2");
            producer.send(message);
            message = session.createTextMessage("MSG#3");
            producer.send(message);
            message = session.createTextMessage("MSG#4");
            producer.send(message);
            message = session.createTextMessage("MSG#5");
            producer.send(message);
            message = session.createTextMessage("MSG#6");
            message.setStringProperty("STOP", "yes");
            producer.send(message);
            
        } finally {
            session.close();
            connection.close();
        }
        
        Thread.sleep(3000);
		activemqObservable.cleanup();
		
		BrokerRegistry.getInstance()
		  			  .lookup(brokerService.getBrokerName())
		  			  .stop();

		assertEquals(observed.size(), 6);
		
	}
	
	public void testFlowable() throws Exception {

		LOG.info("Unit Test ActivemqObservable");
		
		BrokerService brokerService = new BrokerService();
		brokerService.setPersistent(false);
		brokerService.setUseJmx(false);
	    brokerService.addConnector("vm://localhost:61616");
	    brokerService.start();
	    
	    Properties jmsProps = new Properties();
		jmsProps.setProperty("brokerURL", "vm://localhost:61616");
		jmsProps.setProperty("destinationName", "testQ");
		jmsProps.setProperty("destinationType", "Queue");
		jmsProps.setProperty("clientId", "testClient");
		jmsProps.setProperty("acknowledgeMode", "AUTO_ACKNOWLEDGE");
		
		jmsProps.setProperty("checkPropertyName", "STOP");
		jmsProps.setProperty("checkPropertyValue", "yes");
		
		ActivemqObservable activemqObservable = 
								new ActivemqObservable(jmsProps);
		
		Flowable<Message> flowable = 
				activemqObservable.retrieveFlowable(
							BackpressureStrategy.BUFFER);

		List<Message> observed = new ArrayList<Message>();
		flowable.subscribe(observed::add);
		
		ActiveMQConnectionFactory connectionFactory = 
				new ActiveMQConnectionFactory(
						"vm://" + brokerService.getBrokerName() + 
													"?create=false");
        Connection connection = connectionFactory.createConnection();
        connection.start();
 
        Session session = connection.createSession(false,
                								Session.AUTO_ACKNOWLEDGE);
        
        try {
            Queue destination = session.createQueue("testQ");
            MessageProducer producer = session.createProducer(destination);
            
            Message message = session.createTextMessage("MSG#1");
            producer.send(message);
            message = session.createTextMessage("MSG#2");
            producer.send(message);
            message = session.createTextMessage("MSG#3");
            producer.send(message);
            message = session.createTextMessage("MSG#4");
            producer.send(message);
            message = session.createTextMessage("MSG#5");
            producer.send(message);
            message = session.createTextMessage("MSG#6");
            message.setStringProperty("STOP", "yes");
            producer.send(message);
            
        } finally {
            session.close();
            connection.close();
        }
        
        Thread.sleep(3000);
		activemqObservable.cleanup();
		
		BrokerRegistry.getInstance()
		  			  .lookup(brokerService.getBrokerName())
		  			  .stop();

		assertEquals(observed.size(), 6);
		
	}

}
