package com.noambens.servers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.noambens.LoggingServer;
import com.noambens.Message;
import com.noambens.Util;

public class TCPServer extends Thread implements LoggingServer {

    private final Logger logger;
    private final LinkedBlockingQueue<Message> incoming;
    private final InetSocketAddress address;

    private final ServerSocket serverSocket;

    public TCPServer(LinkedBlockingQueue<Message> incoming, InetSocketAddress address) throws IOException {
        this.logger = LoggingServer.createLogger("TCPServer", "TCPServer", true);
        this.incoming = incoming;
        this.address = address;
        this.serverSocket = new ServerSocket(address.getPort());
        this.setDaemon(true);
    }

    public void shutdown() {
        this.interrupt();
        try {
            this.serverSocket.close();
        } catch (IOException e) {
            this.logger.log(Level.FINE,
                    "@{0} (TCPServer): Exception was thrown while trying to close the server socket",
                    new Object[] { this.address });
        }
    }

    @Override
    public void run() {
        this.logger.log(Level.FINE, "@{0} (TCPServer): Starting TCP Server", new Object[] { this.address });

        // while the server is not shutdown, accept messages and do your work
        while (!this.isInterrupted()) {

            Message messageReceived;
            Socket receiveSocket;
            // create the server socket and get the message
            try {
                receiveSocket = serverSocket.accept();
                messageReceived = new Message(Util.readAllBytesFromNetwork(receiveSocket.getInputStream()));
                this.logger.log(Level.FINE, "@{0} (TCPServer): Received message: {1}", new Object[] { this.address, messageReceived });
            } catch (IOException e) {
                this.logger.log(Level.FINE, "@{0} (TCPServer): Exception was thrown while trying to receive a message",
                        new Object[] { this.address });
                shutdown();
                return;
            }

            // add the message to the outgoing queue
            try {
                this.incoming.put(messageReceived);
            } catch (InterruptedException e) {
                this.logger.log(Level.FINE, "@{0} (TCPServer): Exception was thrown while trying to put a message",
                        new Object[] { this.address });
                return;
            }
        }

        // log and shutdown
        this.shutdown();
        this.logger.log(Level.SEVERE, "@{0} (TCPServer): Exiting TCPServer run method", new Object[] { this.address });
    }
}
