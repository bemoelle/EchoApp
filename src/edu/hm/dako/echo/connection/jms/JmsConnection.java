package edu.hm.dako.echo.connection.jms;

import java.io.Serializable;
import java.util.Hashtable;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import edu.hm.dako.echo.common.EchoPDU;

public class JmsConnection implements edu.hm.dako.echo.connection.Connection {

	private QueueConnection queueConnection;
	private QueueSession queueSession;
	private QueueSender sender;
	private QueueReceiver receiver;
	private TemporaryQueue tempqueue;
	private boolean isSelectorSelected = false;
	private Queue receiverQueue;
	
	
	public JmsConnection(String remoteServerAddress, int serverPort) throws NamingException, JMSException {
		
		Hashtable<String, String> env = new Hashtable<String, String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY,"com.tibco.tibjms.naming.TibjmsInitialContextFactory");
		env.put(Context.PROVIDER_URL, "tcp://"+remoteServerAddress+":"+serverPort);
		
		Context initialContext = new InitialContext(env);
	
		QueueConnectionFactory queueConnectionFactory = (QueueConnectionFactory)initialContext.lookup("QueueConnectionFactory");
		queueConnection = queueConnectionFactory.createQueueConnection();
		
		queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
		Queue senderQueue = (Queue) initialContext.lookup("RequestQueue");
		sender = queueSession.createSender(senderQueue);
		sender.setDeliveryMode(DeliveryMode.PERSISTENT);
		
		if(isSelectorSelected) {
			
			int id = (int)Thread.currentThread().getId();
			receiverQueue = (Queue) initialContext.lookup("ResponseQueue");
			receiver = queueSession.createReceiver(receiverQueue, "selector = '"+id+"'");
		}
		else {
			tempqueue =  queueSession.createTemporaryQueue();
			receiver = queueSession.createReceiver(tempqueue);
		}
		
		queueConnection.start();
	}

	@Override
	public Serializable receive() throws Exception {
		
		ObjectMessage msg = (ObjectMessage) receiver.receive();
		return (EchoPDU)msg.getObject();
	}

	@Override
	public void send(Serializable message) throws Exception {
		
		int id = (int)Thread.currentThread().getId();
		Message jmsMessage = queueSession.createObjectMessage(message);
		jmsMessage.setObjectProperty("selector", id);
		
		if(isSelectorSelected) {
			jmsMessage.setJMSReplyTo(receiverQueue);
		}
		else {
			jmsMessage.setJMSReplyTo(tempqueue);
		}

		sender.send(jmsMessage);
	}

	@Override
	public void close() throws Exception {
		
		queueConnection.close();	
	}
	
}
