package com.techyugadi.reactive.rxactivemq;

import java.util.Properties;

import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Message;
import javax.jms.MessageConsumer;

import org.apache.activemq.ActiveMQConnectionFactory;

import com.techyugadi.reactive.rxactivemq.ActivemqListener;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.Observable;

public class ActivemqObservable {

	private Connection connection;
	private Queue queue;
	private Topic topic;
	private Session session;
	private MessageConsumer messageConsumer;
	
	private String checkPropertyName;
	private String checkPropertyValue;
	
	public ActivemqObservable(Properties jmsProperties) 
			throws ConfigurationException, JMSException {
		
		String brokerURL = jmsProperties.getProperty("brokerURL");
		String activemqUser = jmsProperties.getProperty("activemqUser");
		String activemqPassword = jmsProperties.getProperty("activemqPassword");
		String destinationName = jmsProperties.getProperty("destinationName");
		String destinationType = jmsProperties.getProperty("destinationType");
		String clientId = jmsProperties.getProperty("clientId");
		String sessionTransactedMode = jmsProperties.getProperty("sessionTransactedMode");
		String acknowledgeMode = jmsProperties.getProperty("acknowledgeMode");
		
		if (brokerURL == null)
			throw new ConfigurationException("Activemq Broker URL not specified");
		
		if (destinationName == null)
			throw new ConfigurationException("Activemq Destination not specified");
		if (destinationType == null)
			throw new ConfigurationException("Activemq Destination Type (Queue or Topic) not specified");
		
		boolean sessionTransacted = false;
		if ("true".equals(sessionTransactedMode))
			sessionTransacted = true;
		
		int acknowledge = -1;
		if (acknowledgeMode != null) {
			if ("AUTO_ACKNOWLEDGE".equalsIgnoreCase(acknowledgeMode))
				acknowledge = Session.AUTO_ACKNOWLEDGE;
			else if ("CLIENT_ACKNOWLEDGE".equalsIgnoreCase(acknowledgeMode))
				acknowledge = Session.CLIENT_ACKNOWLEDGE;
			else if ("DUPS_OK_ACKNOWLEDGE".equalsIgnoreCase(acknowledgeMode))
				acknowledge = Session.DUPS_OK_ACKNOWLEDGE;
			else
				throw new ConfigurationException(
						"Unknown Session Acknowledge mode: " + acknowledgeMode);
		}
		
		ActiveMQConnectionFactory connectionFactory = 
								new ActiveMQConnectionFactory();
		connectionFactory.setBrokerURL(brokerURL);
		if (activemqUser != null && activemqPassword != null) {
			connectionFactory.setUserName(activemqUser);
			connectionFactory.setUserName(activemqPassword);
		}
		
		connection = connectionFactory.createConnection();
		if (clientId != null)
			connection.setClientID(clientId);

		session = connection.createSession(sessionTransacted, acknowledge);
		
		if (destinationType.equalsIgnoreCase("Queue")) {
			queue = session.createQueue(destinationName);
			messageConsumer = session.createConsumer(queue);
		} else if (destinationType.equalsIgnoreCase("Topic")) {
			topic = session.createTopic(destinationName);
			messageConsumer = session.createConsumer(topic);
		}
		
		/* 
		 * One way for the observable can stop processing messages and move to
		 * completed state is by using a specific JMSProperty in the message.
		 * If this JMSProperty (named checkPropertyName)has a certain value
		 * (checkPropertyValue), the Observable will stop processing any more
		 * messages from the stream.
		 */
		checkPropertyName = jmsProperties.getProperty("checkPropertyName");
		checkPropertyValue = jmsProperties.getProperty("checkPropertyValue");
		
	}
	
	public Observable<Message> retrieveObservable() {
		
		Observable<Message> observable = 
			Observable.create(emitter -> {
						
				ActivemqListener listener = new ActivemqListener() {
							
					@Override 
					public void onMessage(Message event) { 
						emitter.onNext(event);
							
						try {
							if (checkPropertyName != null) {
								String messagePropVal = 
									event.getStringProperty(checkPropertyName);
								
								if (messagePropVal != null &&
									messagePropVal.
										equalsIgnoreCase(checkPropertyValue)) { 
									emitter.onComplete(); 
								} 
							}
						} catch (JMSException e) {
							emitter.onError(e);
						}
					} 

					@Override 
					public void onError(Throwable e) { 
						emitter.onError(e); 
					}
							
					@Override
					public void onException(JMSException je) {
						emitter.onError(je);
					}
							
				};
						
				connection.setExceptionListener(listener);
				messageConsumer.setMessageListener(listener);
				connection.start();
						
			});
		
		return observable;
		
	}
	
	public Flowable<Message> retrieveFlowable(
			BackpressureStrategy strategy) {

		return retrieveObservable().toFlowable(strategy);

	}
	
	public void cleanup() {
		
		try {
			session.close();
		} catch (JMSException e) {
			e.printStackTrace();
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		}
		
	}

}
