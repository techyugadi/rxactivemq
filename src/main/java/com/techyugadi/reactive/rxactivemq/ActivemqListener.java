package com.techyugadi.reactive.rxactivemq;

import javax.jms.MessageListener;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;

public interface ActivemqListener extends MessageListener, ExceptionListener {
	
	public void onError(Throwable e);
	public void onException(JMSException je);

}
