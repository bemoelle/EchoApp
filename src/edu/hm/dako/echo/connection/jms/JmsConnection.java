package edu.hm.dako.echo.connection.jms;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.Queue;
import javax.naming.*;

import java.io.Serializable;
import java.util.*;

public class JmsConnection implements edu.hm.dako.echo.connection.Connection {

	private Hashtable<String, String> env;
	private Context initialContext;
	private QueueConnectionFactory queueConnectionFactory;
	private Queue senderQueue; 
	private QueueConnection queueConnection;
	private QueueSession queueSession;
	private QueueSender sender;
	
	
	public JmsConnection(String remoteServerAddress, int serverPort) throws NamingException, JMSException {
		
		env = new Hashtable<String, String>();
		env.put(Context.INITIAL_CONTEXT_FACTORY,"com.tibco.tibjms.naming.TibjmsInitialContextFactory");
		env.put(Context.PROVIDER_URL, "tcp://" + remoteServerAddress + ":" + serverPort);
		
		initialContext = new InitialContext(env);
	
		queueConnectionFactory = (QueueConnectionFactory)initialContext.lookup("FTQueueConnectionFactory");
		senderQueue = (Queue) initialContext.lookup("SystemA"); 
		queueConnection = queueConnectionFactory.createQueueConnection();
		queueSession = queueConnection.createQueueSession(false, Session.DUPS_OK_ACKNOWLEDGE);
		sender = queueSession.createSender(senderQueue);
		sender.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

	}

	@Override
	public Serializable receive() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void send(Serializable message) throws Exception {

		sender.send(queueSession.createTextMessage((String) message));
	}

	@Override
	public void close() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
}
