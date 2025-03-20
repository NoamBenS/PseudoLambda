package com.noambens.servers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.noambens.JavaRunner;
import com.noambens.LoggingServer;
import com.noambens.Message;
import com.noambens.Util;

public class JavaRunnerFollower extends Thread implements LoggingServer {

    private final PeerServerImpl server;
    private final List<Message> cachedMessages;
    private final Logger logger;

    private final JavaRunner runner;
    private final ServerSocket serverSocket;

    /**
     * Initializes the JavaRunnerFollower with the given message, outgoing
     * queue, and logger
     *
     * @param server - the PeerServerImpl that this JavaRunnerFollower is a part of
     */
    public JavaRunnerFollower(PeerServerImpl server, List<Message> cachedMessages) throws IOException {
        // instance variables
        this.server = server;
        this.cachedMessages = cachedMessages;
        this.runner = new JavaRunner();
        this.serverSocket = new ServerSocket(this.server.getUdpPort() + 2);

        // create the logger
        this.logger = LoggingServer.createLogger("JavaRunnerFollower" + server.getUdpPort(),
                "JavaRunnerFollower-" + this.server.getServerId(), true);
    }

    public void shutdown() {
        this.interrupt();
        try {
            this.serverSocket.close();
        } catch (IOException e) {
            this.logger.log(Level.FINE,
                    "@{0} (JavaRunnerFollower): Exception was thrown while trying to close the server socket",
                    new Object[] { this.server.getUdpPort() });
        }
    }

    @Override
    public void run() {
        this.logger.log(Level.INFO, "@{0} (JavaRunnerFollower): JavaRunnerFollower started",
                new Object[] { this.server.getUdpPort() });

        while (!this.isInterrupted()) {
            // create the server socket and get the message
            Message message;
            Socket receiveSocket;
            try {
                receiveSocket = serverSocket.accept();
                message = new Message(Util.readAllBytesFromNetwork(receiveSocket.getInputStream()));
            } catch (IOException e) {
                this.logger.log(Level.FINE,
                        "@{0} (JavaRunnerFollower): Exception was thrown while trying to receive a message",
                        new Object[] { this.server.getUdpPort() });
                shutdown();
                return;
            }

            // if we have work, create a JavaRunnerFollower and start it
            Message msg = null;
            switch (message.getMessageType()) {
                case WORK:
                    this.logger.log(Level.FINE, "@{0} (JavaRunnerFollower): Received work",
                            new Object[] { this.server.getUdpPort(), new String(message.getMessageContents()) });
                    // run the JavaRunner
                    boolean errorOccured = false;
                    String result;
                    try {
                        InputStream in = new ByteArrayInputStream(message.getMessageContents());
                        result = this.runner.compileAndRun(in);
                    } catch (Exception e) {
                        this.logger.log(Level.WARNING,
                                "@{0} (JavaRunnerFollower): Exception thrown during compileAndRun:\n"
                                        + e.getLocalizedMessage(),
                                new Object[] { this.server.getUdpPort() });

                        // getting stack trace
                        StringWriter sw = new StringWriter();
                        PrintWriter pw = new PrintWriter(sw);
                        e.printStackTrace(pw);

                        // formatting
                        result = e.getMessage() + "\n" + sw.toString();
                        errorOccured = true;
                        // build the message
                    }

                    msg = new Message(Message.MessageType.COMPLETED_WORK,
                                result.getBytes(),
                                message.getReceiverHost(),
                                message.getReceiverPort(),
                                message.getSenderHost(),
                                message.getSenderPort(),
                                message.getRequestID(),
                                errorOccured);

                    // send the message back to the leader
                    sendMessage(msg, receiveSocket);
                    break;
                case NEW_LEADER_GETTING_LAST_WORK:
                    this.logger.log(Level.FINE, "@{0} (JavaRunnerFollower): Received request for cached work",
                            new Object[] { this.server.getUdpPort(), new String(message.getMessageContents()) });
                    if (this.cachedMessages.isEmpty()) {
                        this.logger.log(Level.INFO, "@{0} (JavaRunnerFollower): No cached work to send back",
                                new Object[] { this.server.getUdpPort() });
                        break;
                    }
                    msg = this.cachedMessages.remove(0);
                    Message cached = new Message(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK,
                            msg.getMessageContents(),
                            msg.getReceiverHost(),
                            msg.getReceiverPort(),
                            msg.getSenderHost(),
                            msg.getSenderPort(),
                            msg.getRequestID(),
                            msg.getErrorOccurred());
                    sendMessage(cached, receiveSocket);
                    break;
                default:
                    this.logger.log(Level.WARNING, "@{0} (JavaRunnerFollower): Received a message that was not work",
                            new Object[] { this.server.getUdpPort() });
                    continue;
            }
            // send the message back to the leader ONLY if it is still alive. otherwise,
            // cache the work and RETURN.
        }

        // report before exiting
        if (isInterrupted()) {
            this.logger.log(Level.INFO, "@{0} (JavaRunnerFollower): JavaRunnerFollower interrupted",
                    new Object[] { this.server.getUdpPort() });
        }
        this.shutdown();
        this.logger.log(Level.SEVERE, "@{0} (JavaRunnerFollower): Exiting JavaRunnerFollower run method",
                new Object[] { this.server.getUdpPort() });
    }

    private void sendMessage(Message msg, Socket receiveSocket) {
        try {
            receiveSocket.getOutputStream().write(msg.getNetworkPayload());
            this.logger.log(Level.FINE, "@{0} (JavaRunnerFollower): Sent message back to leader",
                    new Object[] { this.server.getUdpPort() });
        } catch (IOException e) {
            this.logger.log(Level.WARNING,
                    "@{0} (JavaRunnerFollower): Exception thrown during sending message back to leader:\n"
                            + e.getLocalizedMessage(),
                    new Object[] { this.server.getUdpPort() });
            if (this.server.getCurrentLeader() == null) {
                this.logger.log(Level.INFO, "@{0} (JavaRunnerFollower): Leader is dead, caching message {1}",
                        new Object[] { this.server.getUdpPort(), msg.getRequestID() });
                this.cachedMessages.add(msg);
                return;
            }
        }
    }

}
