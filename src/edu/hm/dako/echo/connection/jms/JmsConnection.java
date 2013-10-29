package edu.hm.dako.echo.connection.jms;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Queue;
import javax.jms.TextMessage;
import javax.naming.*;

import edu.hm.dako.echo.common.EchoPDU;

import java.io.Serializable;
import java.util.*;

public class JmsConnection implements edu.hm.dako.echo.connection.Connection {

	private Hashtable<String, String> env;
	private Context initialContext;
	private QueueConnectionFactory queueConnectionFactory;
	private QueueConnection queueConnection;
	private QueueSession queueSession;
	private QueueSender sender;
	private QueueReceiver receiver;
	private Queue senderQueue;
	private Queue receiverQueue;
	
	
	public JmsConnection(String remoteServerAddress, int serverPort) throws NamingException, JMSException {
		
		env = new Hashtable<String, String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY,"com.tibco.tibjms.naming.TibjmsInitialContextFactory");
		env.put(Context.PROVIDER_URL, "tcp://" + remoteServerAddress + ":" + serverPort);
		
		initialContext = new InitialContext(env);
	
		queueConnectionFactory = (QueueConnectionFactory)initialContext.lookup("FTQueueConnectionFactory");
		
		senderQueue = (Queue) initialContext.lookup("SystemA");
		receiverQueue = (Queue) initialContext.lookup("SystemB");
		
		queueConnection = queueConnectionFactory.createQueueConnection();
		
		queueSession = queueConnection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		
		sender = queueSession.createSender(senderQueue);
		sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		
		receiver = queueSession.createReceiver(receiverQueue);
		
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
		
		sender.send(queueSession.createTextMessage("test"));
	}

	@Override
	public void close() throws Exception {
		
		queueConnection.close();	
	}
	
}
