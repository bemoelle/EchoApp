package edu.hm.dako.echo.connection.jms;

import java.io.Serializable;
import java.util.Hashtable;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import edu.hm.dako.echo.common.EchoPDU;

public class JmsConnection implements edu.hm.dako.echo.connection.Connection {

	private Hashtable<String, String> env;
	private Context initialContext;
	private QueueConnectionFactory queueConnectionFactory;
	private QueueConnection queueConnection;
	private QueueSession queueSession;
	private QueueSender sender;
	private QueueReceiver receiver;
	private Queue senderQueue;
	private TemporaryQueue tempqueue;
	
	
	public JmsConnection(String remoteServerAddress, int serverPort) throws NamingException, JMSException {
		
		env = new Hashtable<String, String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY,"com.tibco.tibjms.naming.TibjmsInitialContextFactory");
		env.put(Context.PROVIDER_URL, "tcp://" + remoteServerAddress + ":" + serverPort);
		
		initialContext = new InitialContext(env);
	
		queueConnectionFactory = (QueueConnectionFactory)initialContext.lookup("QueueConnectionFactory");
		
		senderQueue = (Queue) initialContext.lookup("test");
		
		queueConnection = queueConnectionFactory.createQueueConnection();
		
		queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
		sender = queueSession.createSender(senderQueue);
		sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		
		
		tempqueue =  queueSession.createTemporaryQueue();
		receiver = queueSession.createReceiver(tempqueue);
		queueConnection.start();
	}

	@Override
	public Serializable receive() throws Exception {
		
		TextMessage message = (TextMessage) receiver.receive();
		EchoPDU pdu = new EchoPDU();
		pdu.setMessage(message.getText());
		return pdu;
	}

	@Override
	public void send(Serializable message) throws Exception {
		
		Long id = (long)Thread.currentThread().getId();
		String string_id = id.toString();
		TextMessage jmsMessage = queueSession.createTextMessage(string_id);
		jmsMessage.setJMSDestination(tempqueue);
		jmsMessage.setJMSReplyTo(tempqueue);
		sender.send(jmsMessage);
	}

	@Override
	public void close() throws Exception {
		
		queueConnection.close();	
	}
	
}
