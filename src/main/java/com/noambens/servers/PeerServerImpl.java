package com.noambens.servers;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import com.noambens.LeaderElection;
import com.noambens.LoggingServer;
import com.noambens.Message;
import com.noambens.Message.MessageType;
import com.noambens.PeerServer;
import static com.noambens.PeerServer.ServerState.LOOKING;
import com.noambens.UDPMessageReceiver;
import com.noambens.UDPMessageSender;
import com.noambens.Vote;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class PeerServerImpl extends Thread implements PeerServer, LoggingServer {

    private final LinkedBlockingQueue<Message> outgoing;
    private final LinkedBlockingQueue<Message> incoming;
    private UDPMessageSender sender;
    private UDPMessageReceiver receiver;
    private final InetSocketAddress address;
    private final int myPort;
    private final String type;

    private final Long id;
    private final int numberOfObservers;
    private final Long gatewayID;

    private ServerState state;
    private volatile long peerEpoch;
    private volatile Vote currentLeader;

    private final Map<Long, InetSocketAddress> peerIDtoAddress;
    private final Map<InetSocketAddress, Long> addresstoPeerID;

    private List<Message> cachedWork;

    private final Logger logger;

    private final Logger summary;
    private final File summaryFile;
    private final Logger verbose;
    private final File verboseFile;

    private final HttpServer httpServer;

    /**
     * Constructor for PeerServerImpl Instance.
     * 
     * @param myPort          - the port for the udp connection to other servers
     * @param peerEpoch       - the current epoch of the server
     * @param id              - the id of the server
     * @param peerIDtoAddress - the map connecting the peers to their addresses
     * @throws IOException
     */
    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress,
            Long gatewayID, int numberOfObservers) throws IOException {

        // set all the local variables
        this.myPort = udpPort;
        this.address = new InetSocketAddress("localhost", udpPort);
        this.id = serverID;
        this.peerEpoch = peerEpoch;
        this.peerIDtoAddress = peerIDtoAddress;
        this.numberOfObservers = numberOfObservers;
        this.gatewayID = gatewayID;

        // create reverse map for peerIDtoAddress
        this.addresstoPeerID = new HashMap<>();
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            addresstoPeerID.put(entry.getValue(), entry.getKey());
        }

        // set looking and leader states
        this.state = LOOKING;

        // create the blocking queues and the attached message threads
        this.incoming = new LinkedBlockingQueue<>();
        this.outgoing = new LinkedBlockingQueue<>();

        this.cachedWork = new ArrayList<>();

        // create logger (this is the general logger)
        this.type = (this.id.equals(this.gatewayID)) ? "GatewayPeerServer" : "PeerServer";
        String fileNamePreface = this.type + "-" + this.id.toString() + "-" + this.myPort;
        // create the base logger
        this.logger = initializeLogging(fileNamePreface, true);
        // create summary logger
        this.summary = initializeLogging(fileNamePreface + "-summary", true);
        this.summaryFile = getFileName(fileNamePreface + "-summary");
        // create verbose logger
        this.verbose = initializeLogging(fileNamePreface + "-verbose", true);
        this.verboseFile = getFileName(fileNamePreface + "-verbose");

        // create server
        this.httpServer = HttpServer.create(new InetSocketAddress(this.myPort + 1), 0);
        this.httpServer.createContext("/summary", new SummaryHandler());
        this.httpServer.createContext("/verbose", new VerboseHandler());
        this.httpServer
                .setExecutor(new ThreadPoolExecutor(0, 1, 100, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()));
    }

    /**
     * Shutdown method - sets our own shutdown to true (which ends the while loop in
     * run()).
     * Also sets the sender and receiver shutdown methods
     */
    @Override
    public void shutdown() {
        this.sender.shutdown();
        this.sender.interrupt();
        this.receiver.shutdown();
        this.receiver.interrupt();
        this.httpServer.stop(0);
        this.interrupt();
    }

    /**
     * Run method - starts the UDP threads then begins the main while loop
     */
    @Override
    public void run() {
        this.logger.log(Level.FINE, "@{0} ({1}): starting Run method in PeerServerImpl",
                new Object[] { this.id, this.type });

        // start the http server
        this.httpServer.start();
        this.logger.log(Level.FINE, "@{0} ({1}): STARTED Log HTTPServer", new Object[] { this.id, this.type });

        // start the UDP threads
        try {
            sender = new UDPMessageSender(this.outgoing, this.myPort);
            sender.start();
            receiver = new UDPMessageReceiver(this.incoming, this.address, this.myPort, this);
            receiver.start();
        } catch (IOException e) {
            this.logger.log(Level.SEVERE, "@{0} ({1}): Could not start Message Receiver / Message Sender",
                    new Object[] { this.id, this.type });
        }
        this.logger.log(Level.FINE, "@{0} ({1}): STARTED UDPMessageSender and UDPMessageReceiver",
                new Object[] { this.id, this.type });

        // main server loop
        try {
            while (!isInterrupted()) {
                LeaderElection le;
                switch (getPeerState()) {
                    case LOOKING:
                        le = new LeaderElection(this, this.incoming, this.logger);
                        le.lookForLeader();
                        break;
                    case FOLLOWING:
                        following();
                        break;
                    case LEADING:
                        leading();
                        break;
                    case OBSERVER:
                        le = new LeaderElection(this, this.incoming, this.logger);
                        le.lookForLeader();
                        observing();
                        break;
                }
            }

            // if interrupted
            this.shutdown();
            this.logger.log(Level.SEVERE, "@{0} ({1}): Run Method has ended", new Object[] { this.id, this.type });
        } catch (Exception e) {
            this.logger.log(Level.SEVERE, "@{0} ({1}): Run Method threw an exception",
                    new Object[] { this.id, this.type });
                    this.shutdown();
        }
        this.shutdown();
    }

    /**
     * The leader method:
     * Instantiates a RoundRobinLeader and receives requests from the client, then
     * sends the work to the followers
     * Once a follower has completed the work and sent it to the leader, the leader
     * sends the result back to the client
     * 
     * @throws InterruptedException
     */
    private void leading() throws InterruptedException {
        this.logger.log(Level.FINE, "@{0} ({1}): Assumed LEADER role", new Object[] { this.id, this.type });

        // create a new instance of the GossipServer
        GossipServer gossiper;
        try {
            gossiper = createGossiper();
        } catch (IOException e) {
            this.logger.log(Level.SEVERE,
            "@{0} (LEADER): Could not create and start an instance of GossipServer",
            new Object[] { this.id });
            this.shutdown();
            return;
        }
        gossiper.start();
        this.logger.log(Level.FINE, "@{0} (LEADER): Started GossipServer", new Object[] { this.id });
        
        // create a new instance of the RoundRobinLeader
        // request for cached messages from the followers
        RoundRobinLeader leader;
        try {
            leader = createRobin();
        } catch (IOException e) {
            this.logger.log(Level.SEVERE,
            "@{0} (LEADER): Could not create and start an instance of RoundRobinLeader",
            new Object[] { this.id });
            gossiper.shutdown();
            this.shutdown();
            return;
        }
        leader.start();
        this.logger.log(Level.FINE, "@{0} (LEADER): Started RoundRobinLeader", new Object[] { this.id });

        // spinlock until something changes
        while (!isInterrupted() && this.state == ServerState.LEADING);

        if (this.isInterrupted()) {
            this.logger.log(Level.SEVERE, "@{0} (LEADER): Run Method was interrupted",
                    new Object[] { this.id });
            this.shutdown();
        }

        // announce that you're leaving
        leader.shutdown();
        gossiper.shutdown();
        this.logger.log(Level.SEVERE, "@{0}: Exiting LEADER role", new Object[] { this.id });
    }

    /**
     * Creates a new instance of the RoundRobinLeader and set the global
     * 
     */
    private RoundRobinLeader createRobin() throws IOException {
        List<InetSocketAddress> servers = new ArrayList<>();
        for (Entry<Long, InetSocketAddress> entry : this.peerIDtoAddress.entrySet()) {
            if (entry.getKey() == this.gatewayID || isPeerDead(entry.getKey())) {
                continue;
            }
            servers.add(entry.getValue());
        }
        return new RoundRobinLeader(this, servers, this.cachedWork);
    }

    /**
     * The follower method:
     * Instantiates a JavaRunnerFollower for every instance of work it receives from
     * the leader
     * Returns the result of the JavaRunnerFollower to the leader
     * 
     * @throws InterruptedException
     */
    private void following() throws InterruptedException {
        this.logger.log(Level.FINE, "@{0} ({1}): Assumed FOLLOWER role", new Object[] { this.id, this.type });

        // create a new instance of the GossipServer
        GossipServer gossiper;
        try {
            gossiper = createGossiper();
        } catch (IOException e) {
            this.logger.log(Level.SEVERE,
                    "@{0} (FOLLOWER): Could not create and start an instance of GossipServer",
                    new Object[] { this.id });
            return;
        }
        gossiper.start();
        this.logger.log(Level.FINE, "@{0} (FOLLOWER): Started GossipServer", new Object[] { this.id });

        // create a new instance of the JavaRunnerFollower
        JavaRunnerFollower runner;
        try {
            runner = new JavaRunnerFollower(this, this.cachedWork);
        } catch (IOException e) {
            this.logger.log(Level.SEVERE,
                    "@{0} (FOLLOWER): Could not create and start an instance of JavaRunnerFollower",
                    new Object[] { this.id });
            gossiper.shutdown();
            this.shutdown();
            return;
        }
        runner.start();
        this.logger.log(Level.FINE, "@{0} (FOLLOWER): Started JavaRunnerFollower", new Object[] { this.id });

        // spinlock until something changes
        // spinlock the observer
        while (!isInterrupted() && this.state == ServerState.FOLLOWING) {
            // if the leader is dead, leave the method
            if (this.currentLeader == null) {
                this.logger.log(Level.SEVERE,
                        "@{0} (FOLLOWER): Leader is dead, leaving current method to find a new leader",
                        new Object[] { this.id });
                break;
            }
        }

        // if interrupted, pull a FULL shutdown
        if (isInterrupted()) {
            this.logger.log(Level.SEVERE, "@{0} (FOLLOWER): Run Method was interrupted",
                    new Object[] { this.id });
            this.shutdown();
            gossiper.shutdown();
            return;
        }

        // comment you're leaving
        runner.shutdown();
        gossiper.shutdown();
        this.logger.log(Level.SEVERE, "@{0}: Exiting follower role", new Object[] { this.id });
    }

    /**
     * the observing method:
     * spin-loop until everything is closed, but also send a gossiper on its way to
     * keep the server up to date with its neighbors
     * 
     * @throws InterruptedException
     */
    private void observing() throws InterruptedException {
        this.logger.log(Level.FINE, "@{0} ({1}): Beginning OBSERVER work method", new Object[] { this.id, this.type });

        // create a new instance of the GossipServer
        GossipServer gossiper;
        try {
            gossiper = createGossiper();
        } catch (IOException e) {
            this.logger.log(Level.SEVERE,
                    "@{0} (OBSERVER): Could not create and start an instance of GossipServer",
                    new Object[] { this.id });
            return;
        }
        gossiper.start();
        this.logger.log(Level.FINE, "@{0} (OBSERVER): Started GossipServer", new Object[] { this.id });

        // spinlock the observer
        while (!isInterrupted() && this.state == ServerState.OBSERVER) {
            if (this.currentLeader == null) {
                this.logger.log(Level.SEVERE,
                        "@{0} (OBSERVER): Leader is dead, leaving current method to find a new leader",
                        new Object[] { this.id });
                gossiper.shutdown();
                break;
            }
        }

        // if interrupted, pull a FULL shutdown
        if (this.isInterrupted()) {
            this.logger.log(Level.SEVERE, "@{0} (OBSERVER): Run Method was interrupted",
                    new Object[] { this.id });
            this.shutdown();
        }

        // comment you're leaving
        gossiper.shutdown();
        this.logger.log(Level.SEVERE, "@{0}: Exiting OBSERVER work method", new Object[] { this.id });
    }

    /**
     * Creates a new instance of the GossipServer
     * 
     * @return - the new instance of the GossipServer
     * @throws IOException
     */
    private GossipServer createGossiper() throws IOException {
        List<Long> peerIds = new ArrayList<>(this.peerIDtoAddress.keySet());
        return new GossipServer(this, peerIds, this.incoming, this.summary, this.verbose);
    }

    /**
     * Helper method to get the name of the logger
     * 
     * @param name - the name of the logger
     * @return - the name of the logger
     */
    private File getFileName(String name) {
        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String suffix = date.format(formatter);
        String dirName = "logs-" + suffix + File.separator + name + "-Log.txt";
        File file = new File(dirName);
        return file;
    }

    /**
     * Handler for the /summary endpoint
     */
    private class SummaryHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            logger.log(Level.INFO, "@{0} ({1}): received a message for summary and am handling it",
                    new Object[] { id, type });
            // check method
            if (exchange.getRequestMethod().equals("GET")) {
                byte[] bytes = new byte[(int) summaryFile.length()];
                bytes = Files.readAllBytes(summaryFile.toPath());
                OutputStream os = exchange.getResponseBody();
                exchange.sendResponseHeaders(200, summaryFile.length());
                os.write(bytes);
                return;
            }
        }
    }

    /**
     * Handler for the /verbose endpoint
     */
    private class VerboseHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            logger.log(Level.INFO, "@{0} ({1}): received a message for verbose and am handling it",
                    new Object[] { id, type });
            if (exchange.getRequestMethod().equals("GET")) {
                byte[] bytes = new byte[(int) verboseFile.length()];
                bytes = Files.readAllBytes(verboseFile.toPath());
                OutputStream os = exchange.getResponseBody();
                exchange.sendResponseHeaders(200, verboseFile.length());
                os.write(bytes);
                return;
            }
        }
    }

    /**
     * @param v - the vote to set to the current leader
     */
    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        if (v == null)
            throw new IllegalArgumentException();
        this.currentLeader = v;
    }

    /**
     * @return - the current leader of the server this method is invoked on
     */
    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    /**
     * @param type            - the type of message we're sending to the other
     *                        server
     * @param messageContents - the content of the message as a byte array
     * @param target          - the address of the target recieving port
     */
    @Override
    public void sendMessage(MessageType type, byte[] messageContents, InetSocketAddress target)
            throws IllegalArgumentException {
        if (target == null || !addresstoPeerID.containsKey(target)) {
            throw new IllegalArgumentException("Address is not valid");
        }
        if (messageContents == null) {
            throw new IllegalArgumentException("Illegal message");
        }
        if (type == null || type.getChar() == 'z') {
            throw new IllegalArgumentException("Illegal message type");
        }
        // formatting for this method is - public Message(MessageType type, byte[]
        // contents, String senderHost, int senderPort, String receiverHost, int
        // receiverPort)
        this.outgoing.offer(new Message(type, messageContents, this.address.getHostString(), this.getUdpPort(),
                target.getHostString(), target.getPort()));
    }

    /**
     * sends a broadcast - i.e. sends the same message to every single other server
     * in the cluster
     * 
     * @param type            - the type of message we're sending to the other
     *                        server
     * @param messageContents - the content of the message as a byte array
     */
    @Override
    public void sendBroadcast(MessageType type, byte[] messageContents) {
        for (InetSocketAddress inet : this.peerIDtoAddress.values()) {
            this.sendMessage(type, messageContents, inet);
        }
    }

    /**
     * @return - the state of the server this method is invoked on
     */
    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    /**
     * sets a new state for the server called on
     * 
     * @param newState - the state to set the called server's state to
     */
    @Override
    public void setPeerState(ServerState newState) {
        if (newState == null) {
            throw new IllegalArgumentException();
        }
        this.summary.log(Level.INFO, "{0}: switching from {1} to {2}",
                new Object[] { this.id, this.state, newState });
        System.out
                .println(this.id + ": switching from " + this.state + " to " + newState);
        this.state = newState;
    }

    /**
     * @return - the id of the server this method is invoked on
     */
    @Override
    public Long getServerId() {
        return this.id;
    }

    /**
     * @return - the epoch of the server this method is invoked on
     */
    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    /**
     * @return - the address of the server this method is invoked on
     */
    @Override
    public InetSocketAddress getAddress() {
        return this.address;
    }

    /**
     * @return - the udp port of the server this method is invoked on
     */
    @Override
    public int getUdpPort() {
        return this.myPort;
    }

    /**
     * @param peerId - the id of the peer to get the address of
     * 
     * @return - the address of the peer with the given id
     */
    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return this.peerIDtoAddress.get(peerId);
    }

    /**
     * @param address - the address of the peer to get the id of
     * 
     * @return - the id of the peer with the given address
     */
    public Long getPeerByAddress(InetSocketAddress address) {
        return this.addresstoPeerID.get(address);
    }

    /**
     * @return - the size necessary to define a "quorum" for the cluster of servers
     *         that this server is a part of.
     *         The quorum is the minimum size of a mojority of living nodes in the
     *         cluster, minus any observers.
     */
    @Override
    public int getQuorumSize() {
        return (this.peerIDtoAddress.size() + 1 - this.numberOfObservers) / 2;
    }

    /**
     * Reports that a peer has failed. Updates the internal PeerServer methods to
     * reflect this.
     * remove the peer in question from all internal data structures
     * 
     * @param peerID - the id of the peer that has failed
     */
    @Override
    public void reportFailedPeer(long peerID) {
        InetSocketAddress address = this.getPeerByID(peerID);
        this.peerIDtoAddress.remove(peerID);
        this.addresstoPeerID.remove(address);
        // check for leader failure
        if (peerID == this.currentLeader.getProposedLeaderID()) {
            this.currentLeader = null;
            this.setPeerState(ServerState.LOOKING);
            this.peerEpoch++;
        }
    }

    /**
     * Checks if a peer is dead given a peerID
     * 
     * @param peerID - the id of the peer to check if it is dead
     */
    @Override
    public boolean isPeerDead(long peerID) {
        return (!this.peerIDtoAddress.containsKey(peerID));
    }

    /**
     * Checks if a peer is dead given an address
     * 
     * @param address - the address of the peer to check if it is dead
     */
    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        return (!this.addresstoPeerID.containsKey(address));
    }

}
