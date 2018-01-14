package com.techyugadi.reactive.rxactivemq.sample;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.techyugadi.reactive.rxactivemq.ActivemqObservable;

import io.reactivex.Observable;

/*
 * To run this application, first install ActiveMQ on your local machine
 * Then starts ActiveMQ broker and create a queue named testQ in the running
 * instance of ActiveMQ.
 * Don't change the default broker port (61616)
 */

public class SimpleApp {
	
	public static class TestMessageProducer implements Runnable {
		
        public void run() {
        	
        	System.out.println("Running TestMessageProducer +++++ ");
        	
            try {
                // Create a ConnectionFactory
                ActiveMQConnectionFactory connectionFactory = 
                		new ActiveMQConnectionFactory("tcp://localhost:61616");
 
                // Create a Connection
                Connection connection = connectionFactory.createConnection();
                connection.start();
 
                // Create a Session
                Session session = connection.createSession(
                						false, Session.AUTO_ACKNOWLEDGE);
 
                // Create the destination (Topic or Queue)
                Destination destination = session.createQueue("testQ");
 
                // Create a MessageProducer from the Session to 
                // the Topic or Queue
                MessageProducer producer = session.createProducer(destination);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
 
                // Create messages
                for (int i=0; i<=10; i++) {
                	
                	String text = "MSG #" + i;  
                	TextMessage message = session.createTextMessage(text);
                	
                	if (i == 10) {
                		message.setStringProperty("STOP", "yes");
                	}
 
                	// Tell the producer to send the message
                	producer.send(message);
                	System.out.println("SENT: " + text);
                	
                }
 
                // Clean up
                session.close();
                connection.close();
                
            } catch (Exception e) {
                System.out.println("Caught: " + e);
                e.printStackTrace();
            }
            
        }
    }
	
	public static void main(String[] args) throws Exception {
		
		Properties jmsProps = new Properties();
		jmsProps.setProperty("brokerURL", "tcp://localhost:61616");
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

		observable.subscribe(msg -> {System.out.println(
										"RECEIVED MESSAGE:" + 
										msg.toString());},
				 			err -> {System.out.println(
				 						"RECEIVED ERROR:" + 
				 								err.toString()); 
				 						err.printStackTrace();}
		);

		Runtime.getRuntime().addShutdownHook(new Thread(){
					public void run() {
						System.out.println("Cleaning up ActiveMQ Client");
						activemqObservable.cleanup();
					}
		});
		
		Thread producerThread = new Thread(new TestMessageProducer());
		producerThread.start();
		
		// Simplistic approach for this sample app
		// Ideally the producer thread and the observable will be
		// on two separate programs
		Thread.sleep(3000);
		System.exit(0);
		
	}

}
