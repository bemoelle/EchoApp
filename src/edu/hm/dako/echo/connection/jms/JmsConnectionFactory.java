package edu.hm.dako.echo.connection.jms;

import edu.hm.dako.echo.connection.Connection;
import edu.hm.dako.echo.connection.ConnectionFactory;

public class JmsConnectionFactory implements ConnectionFactory {

	@Override
	public Connection connectToServer(String remoteServerAddress,
			int serverPort, int localPort) throws Exception {
		JmsConnection connection = null;
		boolean connected = false;
		while (!connected) {
			try {
				connection = new JmsConnection(remoteServerAddress, serverPort);
				connected = true;
			} catch (Exception e) {
				System.out.println("Something wrong with the JMS Connection to" + remoteServerAddress + ":" + serverPort);
			}
		}
		return connection;
		
	}

}
