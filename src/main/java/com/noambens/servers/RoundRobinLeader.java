package com.noambens.servers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.noambens.LoggingServer;
import com.noambens.Message;
import com.noambens.Util;

public class RoundRobinLeader extends Thread implements LoggingServer {

    private final PeerServerImpl server;
    private final List<InetSocketAddress> servers;
    private final Logger logger;

    private final LinkedBlockingQueue<Message> incoming;
    private final TCPServer tcp;

    private final Map<Long, Message> cachedMap;

    private long followerID = 0;

    /**
     * Initializes the RoundRobinLeader. This class does the leader's main job
     * in receiving and assigning work to followers. This class also employs a
     * round-robin style of assigning work, where every follower gets a turn, in
     * circular order.
     *
     * @param server    - the server of the leader that this instance of
     *                  RoundRobinLeader is a part of
     * @param servers   - the list of other (follower) servers to send work to
     * @param gatewayID - the ID of the gateway server, an observer that should be
     *                  skipped
     */
    public RoundRobinLeader(PeerServerImpl server, List<InetSocketAddress> servers, List<Message> cache) throws IOException {
        this.server = server;
        this.servers = servers;
        this.setDaemon(true);
        // create the TCP server
        this.incoming = new LinkedBlockingQueue<>();
        this.cachedMap = new HashMap<>();
        for (Message message : cache) {
            this.cachedMap.put(message.getRequestID(), message);
            System.out.println("Cached message: " + message.getRequestID() + " with contents: " + new String(message.getMessageContents()));
        }
        
        InetSocketAddress leaderTCP = new InetSocketAddress(this.server.getAddress().getHostName(),
                this.server.getUdpPort() + 2);
        this.tcp = new TCPServer(incoming, leaderTCP);
        this.logger = LoggingServer.createLogger("RoundRobinLeader" + server.getUdpPort(),
                "RoundRobinLeader-" + this.server.getServerId() + "-" + this.server.getUdpPort(), true);
    }

    public void shutdown() {
        this.interrupt();
        this.tcp.shutdown();
    }

    @Override
    public void run() {
        this.logger.log(Level.FINE, "@{0} (RoundRobinLeader): Starting RoundRobinLeader",
                new Object[] { this.server.getUdpPort() });

        // start the TCP server
        this.tcp.start();

        // create our threadpool for handling requests synchronously
        int cores = Runtime.getRuntime().availableProcessors();
        try (ThreadPoolExecutor threadPool = new ThreadPoolExecutor(cores, cores * 2, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>())) {

            // first, ask for any cached messages
            for (InetSocketAddress followerServer : this.servers) {
                this.logger.log(Level.FINE, "@{0} (RoundRobinLeader): Asking for last work from follower at {1}",
                        new Object[] { this.server.getUdpPort(), followerServer.getPort() });
                Message message = new Message(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK,
                        "nil".getBytes(),
                        this.server.getAddress().getHostName(),
                        this.server.getAddress().getPort(),
                        followerServer.getHostName(),
                        followerServer.getPort());
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(followerServer.getHostName(), followerServer.getPort() + 2));
                    socket.getOutputStream().write(message.getNetworkPayload());
                } catch (IOException e) {
                    this.logger.log(Level.SEVERE,
                            "@{0} (RoundRobinLeader): Exception was thrown while trying to get last work from a follower",
                            new Object[] { this.server.getUdpPort() });
                }
            }

            while (!this.isInterrupted()) {
                // get the message
                Message message;
                try {
                    message = this.incoming.poll(20, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    this.logger.log(Level.FINE,
                            "@{0} (RoundRobinLeader): Exception was thrown while trying to get a message",
                            new Object[] { this.server.getUdpPort() });
                    return;
                }

                // if we have no message, continue
                if (message == null) {
                    continue;
                }

                // if we have work, create a JavaRunnerFollower and start it
                switch (message.getMessageType()) {
                    case WORK:
                        // case that it is in the cache
                        if (this.cachedMap.containsKey(message.getRequestID())) {
                            this.logger.log(Level.FINE,
                                    "@{0} (RoundRobinLeader): Received work, ID: {1}. Found in cache, passing to SynchronousHandler",
                                    new Object[] { this.server.getUdpPort(), message.getRequestID() });
                            try {
                                threadPool.execute(
                                        new SynchronousHandler(this.server, message,
                                                null, message.getRequestID(), true));
                            } catch (Exception e) {
                                this.logger.log(Level.SEVERE,
                                        "@{0} (RoundRobinLeader): Exception was thrown while trying to send cached work with ID {1} to the master",
                                        new Object[] { this.server.getUdpPort(), message.getRequestID() });
                            }
                            // clear it from the cache
                            this.cachedMap.remove(message.getRequestID());
                            break;
                        }

                        // case that it is not in the cache
                        // log the message receive/send
                        this.logger.log(Level.FINE,
                                "@{0} (RoundRobinLeader): Received work, ID: {1}. created a message and passing to SynchronousHandler",
                                new Object[] { this.server.getUdpPort(), message.getRequestID() });
                        InetSocketAddress nextServerAddress = pickAWorker();
                        InetSocketAddress nextServerTCP = new InetSocketAddress(nextServerAddress.getHostName(),
                                nextServerAddress.getPort() + 2);

                        // send the work to the next server via threadpool
                        try {
                            threadPool.execute(new SynchronousHandler(this.server, message, nextServerTCP,
                                    message.getRequestID(), false));
                        } catch (Exception e) {
                            this.logger.log(Level.SEVERE,
                                    "@{0} (RoundRobinLeader): Exception was thrown while trying to send work to a follower",
                                    new Object[] { this.server.getUdpPort() });
                        }
                        break;
                    case NEW_LEADER_GETTING_LAST_WORK:
                        this.logger.log(Level.INFO,
                                "@{0} (RoundRobinLeader): Received last work from old leader, workID: {1} from server address {2}",
                                new Object[] { this.server.getUdpPort(), message.getRequestID(),
                                        message.getReceiverPort() });
                        // put it in the cache and hold onto it until the gateway asks for it
                        this.cachedMap.put(message.getRequestID(), message);
                        break;
                    default:
                        // return the message to the queue as it's not for us
                        this.logger.log(Level.WARNING,
                                "@{0} (RoundRobinLeader): Received a message that was not work. Message type was {1}",
                                new Object[] { this.server.getUdpPort(), message.getMessageType() });
                        break;
                }
            }
        }

        // shutdown log that we're done
        this.shutdown();
        this.logger.log(Level.SEVERE, "@{0} (RoundRobinLeader): Exiting RoundRobinLeader run method",
                new Object[] { this.server.getUdpPort() });
    }

    /**
     * Pick a server to send work to.
     * 
     * @return the next server to send work to
     */
    private InetSocketAddress pickAWorker() {
        // get the next server index
        int currServer = (int) this.followerID % this.servers.size();
        InetSocketAddress worker = this.servers.get(currServer);

        // if server chosen is dead, remove it and move on
        while (this.server.isPeerDead(worker)) {
            // remove it
            this.servers.remove(currServer);
            // move on
            currServer = (int) this.followerID % this.servers.size();
            worker = this.servers.get(currServer);
        }
        // increment the server index
        this.followerID++;
        return worker;
    }

    /**
     * SynchronousHandler is a class that handles the independent jobs sent to
     * workers.
     */
    private class SynchronousHandler extends Thread implements LoggingServer {

        private final PeerServerImpl server;
        private final Message message;
        private final InetSocketAddress nextServerTCP;
        private final long requestID;
        private final boolean cached;

        private final Logger logger;

        /**
         * Constructor for the SynchronousHandler
         * 
         * @param message       - the ORIGINAL message given, will be modified and sent
         *                      to the worker
         * @param nextServerTCP - the address of the next server to send the message to,
         *                      accounted for as a TCP address
         * @param jobID         - the ID of the job to be sent, used to identify the job
         *                      as well as the handler itself
         * @throws IOException
         */
        private SynchronousHandler(PeerServerImpl server, Message message, InetSocketAddress nextServerTCP,
                long requestID,
                boolean cached) throws IOException {
            this.server = server;
            this.message = message;
            this.nextServerTCP = nextServerTCP;
            this.requestID = requestID;
            this.cached = cached;
            setDaemon(true);

            this.logger = LoggingServer.createLogger("SynchronousHandler" + requestID,
                    "SynchronousHandler-" + requestID, true);
        }

        /**
         * Run method for the synchronous handler.
         * This method sends the work to the worker and waits for a response.
         */
        @Override
        public void run() {
            // log that we're starting
            this.logger.log(Level.FINE, "@{0} (SynchronousHandler): Starting SynchronousHandler for work with ID {1}",
                    new Object[] { this.requestID, this.message.getRequestID() });

            // if it's not cached, submit the task to the worker
            if (!this.cached) {
                Message send = new Message(Message.MessageType.WORK,
                        this.message.getMessageContents(),
                        this.server.getAddress().getHostName(),
                        this.server.getAddress().getPort(),
                        this.nextServerTCP.getHostName(),
                        this.nextServerTCP.getPort());

                // send outgoing message to the leader of the peers
                Message completedWork = null;
                try (Socket socket = new Socket()) {
                    socket.connect(this.nextServerTCP);
                    socket.getOutputStream().write(send.getNetworkPayload());

                    // wait for response
                    completedWork = new Message(Util.readAllBytesFromNetwork(socket.getInputStream()));
                } catch (Exception e) {
                    this.logger.log(Level.SEVERE,
                            "@{0} (SynchronousHandler): Exception was thrown while trying to send/receive work to/from a follower",
                            new Object[] { this.requestID });
                    if (this.server.isPeerDead(this.nextServerTCP.getPort() - 2)) {
                        this.logger.log(Level.SEVERE,
                                "@{0} (SynchronousHandler): Peer is dead, adding work back to the queue",
                                new Object[] { this.requestID });
                        incoming.add(this.message);
                        return;
                    }
                }

                // create the response message to return to the GatewayServer
                Message response = new Message(
                        Message.MessageType.COMPLETED_WORK,
                        completedWork.getMessageContents(),
                        this.message.getReceiverHost(),
                        this.message.getReceiverPort(),
                        this.message.getSenderHost(),
                        this.message.getSenderPort(),
                        completedWork.getRequestID(),
                        completedWork.getErrorOccurred());

                sendMessage(this.message, response);
            }
            // else if it is cached, get it and send it as is
            else {
                Message response = cachedMap.get(requestID);
                sendMessage(this.message, response);
            }

            // log that we're done
            this.logger.log(Level.SEVERE, "@{0} (SynchronousHandler): Exiting SynchronousHandler.run()",
                    new Object[] { this.requestID });
        }

        /**
         * Send a message back to the GatewayServer.
         * 
         * @param msg      - the base message of the request
         * @param response - the response submitted (or gotten from the cache) by the
         *                 worker
         */
        private void sendMessage(Message msg, Message response) {
            // try to send the message
            try (Socket responseSocket = new Socket()) {
                responseSocket.connect(new InetSocketAddress(message.getSenderHost(), message.getSenderPort()));
                responseSocket.getOutputStream().write(response.getNetworkPayload());
                this.logger.log(Level.FINE, "@{0} (SynchronousHandler): Sent completed work to the master",
                        new Object[] { this.requestID });
            } catch (IOException e) {
                this.logger.log(Level.SEVERE,
                        "@{0} (SynchronousHandler): Exception was thrown while trying to send completed work to the master",
                        new Object[] { this.requestID });
            }
        }
    }

}
