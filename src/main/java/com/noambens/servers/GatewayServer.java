package com.noambens.servers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import com.noambens.LoggingServer;
import com.noambens.Message;
import com.noambens.Util;
import com.noambens.Vote;

public class GatewayServer extends Thread implements LoggingServer {

    // the logger
    private final Logger logger;

    // ports and server info
    private final int httpPort;
    private final int udpPort;
    private long peerEpoch;
    private final Long serverID;
    private final ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress;
    private final int numberOfObservers;
    private AtomicLong workID = new AtomicLong(0);

    // server instance variables
    private final HttpServer httpServer;
    private final GatewayPeerServerImpl gatewayPeerServer;

    // cache map
    private final Map<Integer, Message> cache;
    private final Map<Long, ServerSocket> returnAddresses;

    /**
     * Constructor for the GatewayServer
     * 
     * @param httpPort          - the port for the http server
     * @param peerPort          - the udp port for the gateway peer server
     * @param peerEpoch         - the epoch for the peers
     * @param serverID          - the id of the gateway peer server
     * @param peerIDtoAddress   - the map of peer ids to their addresses
     * @param numberOfObservers - the number of observers
     * @throws IOException
     */
    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID,
            ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) throws IOException {
        // create logger
        this.logger = LoggingServer.createLogger("GatewayServer-" + serverID.toString(),
                "GatewayServer-" + serverID.toString(), true);

        // set instance variables
        this.httpPort = httpPort;
        this.udpPort = peerPort;
        this.peerEpoch = peerEpoch;
        this.serverID = serverID;
        this.peerIDtoAddress = peerIDtoAddress;
        this.numberOfObservers = numberOfObservers;

        // create the cache
        this.cache = new ConcurrentHashMap<>();
        this.returnAddresses = new ConcurrentHashMap<>();

        // create the gateway peer server
        this.gatewayPeerServer = new GatewayPeerServerImpl(this.udpPort, this.peerEpoch, this.serverID,
                this.peerIDtoAddress, this.serverID, this.numberOfObservers);

        // create server
        this.httpServer = HttpServer.create(new InetSocketAddress(this.httpPort), 0);
        this.httpServer.createContext("/compileandrun", new MessageHandler());
        this.httpServer.createContext("/electionstatus", new ElectionStatusHandler());

        int numThreads = Runtime.getRuntime().availableProcessors();
        this.httpServer.setExecutor(Executors.newFixedThreadPool(numThreads));
    }

    /**
     * Starts the server if it exists, otherwise throws an IllegalStateException
     */
    @Override
    public void start() {
        this.logger.log(Level.INFO, "starting GatewayServer");
        this.httpServer.start();
        this.getPeerServer().start();
    }

    /**
     * Stops the server if it exists, otherwise throws an IllegalStateException
     */
    public void shutdown() throws IOException {
        this.logger.log(Level.SEVERE, "stopping GatewayServer");
        interrupt();
        this.httpServer.stop(0);
        this.getPeerServer().shutdown();
        closeAllSockets();
    }

    public GatewayPeerServerImpl getPeerServer() {
        return this.gatewayPeerServer;
    }

    /**
     * The handler for if you ask for election status
     */
    private class ElectionStatusHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!exchange.getRequestMethod().equals("GET")) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            if (getPeerServer().getCurrentLeader() == null) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            String response = "\n Server " + gatewayPeerServer.getServerId() + " has role OBSERVER";
            for (Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
                if (gatewayPeerServer.isPeerDead(entry.getKey())) {
                    continue;
                }
                if (entry.getKey().equals(gatewayPeerServer.getCurrentLeader().getProposedLeaderID())) {
                    response += "\n Server " + entry.getKey() + " has role LEADER";
                }
                else {
                    response += "\n Server " + entry.getKey() + " has role FOLLOWER";
                }
            }
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }

    }

    private void closeAllSockets() throws IOException{
        for (Long key : returnAddresses.keySet()) {
            returnAddresses.remove(key).close();
        }
    }

    private void sleepASec(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            logger.log(Level.SEVERE, "Thread interrupted");
        }
    }

    /**
     * MessageHandler class for the GatewayServer
     */
    private class MessageHandler implements HttpHandler {

        /**
         * Handler method for the MessageHandler
         */
        @Override
        public void handle(HttpExchange exchange) throws IOException {

            // log that we received a message
            logger.log(Level.INFO, "@Gateway: received a message and am handling it");

            // assign a workID
            long workID = GatewayServer.this.workID.getAndIncrement();

            // check method
            if (!exchange.getRequestMethod().equals("POST")) {
                // log that it's wrong
                logger.log(Level.WARNING, "@Gateway: non-post message rejected");

                // set response
                exchange.getResponseHeaders().add("Cached-Response", "false");
                exchange.sendResponseHeaders(405, -1);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write("Invalid Method".getBytes());
                }
                return;
            }

            // check header is "text/x-java-source"
            Map<String, List<String>> headersMap = exchange.getRequestHeaders();
            List<String> contentType = headersMap.get("Content-Type");
            String type = (contentType != null && !contentType.isEmpty()) ? contentType.get(0) : null;
            if (type == null || !type.equals("text/x-java-source")) {
                // log failure
                logger.log(Level.WARNING, "@Gateway: incorrect content-type rejected - message id: " + workID);

                // send response
                exchange.getResponseHeaders().add("Cached-Response", "false");
                exchange.sendResponseHeaders(400, -1);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write("Invalid Content-Type".getBytes());
                }
                return;
            }

            // get the input stream
            InputStream input = exchange.getRequestBody();
            byte[] inputBytes = input.readAllBytes();
            int inputHash = (new String(inputBytes)).hashCode();

            // check if it's in the cache
            if (cache.containsKey(inputHash)) {
                // log success
                logger.log(Level.INFO, "@Gateway: cache hit, writing to response");

                // get the cached message and its parts
                Message cachedMessage = cache.get(inputHash);
                // give the cached message the new work ID
                Message cachedMessageWithNewId = new Message(Message.MessageType.WORK,
                        cachedMessage.getMessageContents(),
                        cachedMessage.getReceiverHost(),
                        cachedMessage.getReceiverPort(),
                        cachedMessage.getSenderHost(),
                        cachedMessage.getSenderPort(),
                        workID);
                byte[] cachedMessageContents = cachedMessageWithNewId.getMessageContents();
                int responseCode = cachedMessage.getErrorOccurred() ? 400 : 200;

                // set response
                exchange.getResponseHeaders().add("Cached-Response", "true");
                exchange.sendResponseHeaders(responseCode, cachedMessageContents.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(cachedMessageContents);
                }
                return;
            }

            while (gatewayPeerServer.getCurrentLeader() == null && !isInterrupted());

            boolean sent = false;
            Message responseMessage = null;
            while (!sent && !isInterrupted()) {

                // create return address
                ServerSocket returnAddress;
                try {
                    returnAddress = new ServerSocket(0);
                    returnAddresses.put(workID, returnAddress);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "@Gateway: failed to create return address");
                    return;
                }

                // check if you don't have a leader at all
                Vote leaderVote = getPeerServer().getCurrentLeader();
                if (leaderVote == null) {
                    continue;
                }
                InetSocketAddress leader = getPeerServer().getPeerByID(leaderVote.getProposedLeaderID());

                // send outgoing message to the leader of the peers
                try (Socket socket = new Socket()) {
                    // send outgoing message to the leader of the peers
                    String leaderHost = leader.getHostString();
                    int leaderPort = leader.getPort() + 2;

                    Message message = new Message(Message.MessageType.WORK,
                            inputBytes,
                            returnAddress.getInetAddress().getHostName(),
                            returnAddress.getLocalPort(),
                            leaderHost,
                            leaderPort,
                            workID);
                    logger.fine("Sending message to leader at " + leader.getPort() + ": " + new String(message.getMessageContents()));
                    InetSocketAddress leaderAddress = new InetSocketAddress(leaderHost, leaderPort);
                    socket.connect(leaderAddress);
                    socket.getOutputStream().write(message.getNetworkPayload());
                } catch (Exception e) {
                    leaderVote = getPeerServer().getCurrentLeader();
                    if (leaderVote == null) {
                        logger.log(Level.INFO, "@Gateway: leader died, waiting for a new one");
                        closeAllSockets();
                        continue;
                    }
                    logger.log(Level.SEVERE, "@Gateway: failed to send message to leader");
                    returnAddress.close();
                    sleepASec(500);
                    continue;
                }

                // get the response of the message
                try (Socket responseSocket = returnAddress.accept()) {
                    logger.log(Level.INFO, "@Gateway: awaiting response from leader");
                    while ((leaderVote = getPeerServer().getCurrentLeader()) != null && responseMessage == null) {
                        responseMessage = new Message(Util.readAllBytesFromNetwork(responseSocket.getInputStream()));
                    }
                } catch (Exception e) {
                    leaderVote = getPeerServer().getCurrentLeader();
                    if (leaderVote == null) {
                        logger.log(Level.INFO, "@Gateway: leader died, waiting for a new one");
                        closeAllSockets();
                        continue;
                    }
                    logger.log(Level.SEVERE, "@Gateway: failed to receive response from leader");
                    returnAddress.close();
                    sleepASec(500);
                    continue;
                }

                // close the return address once we're done with it
                returnAddress.close();
                returnAddresses.remove(workID);
                sent = true;
            }
            byte[] messageContents = responseMessage.getMessageContents();
            int responseCode = responseMessage.getErrorOccurred() ? 400 : 200;

            // set response
            exchange.getResponseHeaders().add("Cached-Response", "false");
            exchange.sendResponseHeaders(responseCode, messageContents.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(messageContents);
            }

            logger.log(Level.INFO, "@Gateway: wrote response to client");

            // cache the message
            cache.put(inputHash, responseMessage);
        }

    }

}