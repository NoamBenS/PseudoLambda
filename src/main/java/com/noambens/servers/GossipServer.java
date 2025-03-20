package com.noambens.servers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.noambens.LoggingServer;
import com.noambens.Message;
import com.noambens.Message.MessageType;

public class GossipServer extends Thread implements LoggingServer {

    // heartbeat variables
    private static final int GOSSIP = 3000;
    private static final int FAIL = GOSSIP * 10;
    private static final int CLEANUP = FAIL * 2;

    // instance variables
    private final PeerServerImpl server;
    private final LinkedBlockingQueue<Message> messages;

    private Random random;

    List<Long> peerIds;
    Map<Long, Pair> heartBeats;
    Map<Long, Long> failed;

    private final Logger summary;
    private final Logger verbose;

    public GossipServer(PeerServerImpl server, List<Long> peerIds, LinkedBlockingQueue<Message> messages, Logger summary, Logger verbose) throws IOException {
        this.server = server;
        this.messages = messages;
        this.summary = summary;
        this.verbose = verbose;
        this.peerIds = peerIds;
        this.random = new Random();
        this.setDaemon(true);

        this.heartBeats = new HashMap<>();
        for (long id : peerIds) {
            this.heartBeats.put(id, new Pair(System.currentTimeMillis(), 0));
        }
        this.heartBeats.put(server.getServerId(), new Pair(System.currentTimeMillis(), 1));

        this.failed = new HashMap<>();
    }

    public void shutdown() {
        interrupt();
    }

    @Override
    public void run() {
        this.summary.log(Level.INFO, "@{0} (GossipServer): GossipServer started", this.server.getUdpPort());
        this.verbose.log(Level.INFO, "@{0} (GossipServer): GossipServer started", this.server.getUdpPort());

        long lastGossip = System.currentTimeMillis();
        while (!isInterrupted()) {
            hearNoEvil();
            long now = System.currentTimeMillis();
            if (now - lastGossip >= GOSSIP) {
                lastGossip = now;
                speakNoEvil();
            }
        }
        this.shutdown();
        this.summary.log(Level.SEVERE, "@{0} (GossipServer): GossipServer stopped", this.server.getUdpPort());
        this.verbose.log(Level.SEVERE, "@{0} (GossipServer): GossipServer stopped", this.server.getUdpPort());
    }

    /**
     * The gossip-listening method.
     * Listens for incoming gossip and updates the local map.
     */
    private void hearNoEvil() {
        // get the messages from the queue and read them for new gossip
        while (!this.messages.isEmpty() && !isInterrupted()) {
            Message message = this.messages.poll();
            if (message != null) {
                // if the message is not gossip, add it back to the queue
                if (message.getMessageType() != Message.MessageType.GOSSIP) {
                    this.messages.add(message);
                    if (isInterrupted()) {
                        return;
                    }
                    continue;
                }
                // create sender address and log message reception
                InetSocketAddress sender = new InetSocketAddress(message.getSenderHost(), message.getSenderPort());
                this.verbose.log(Level.FINE, "{0}: Received gossip message from {1}", new Object[]{this.server.getServerId(), this.server.getPeerByAddress(sender)});

                ByteBuffer buffer = ByteBuffer.wrap(message.getMessageContents());
                while (buffer.hasRemaining() && !isInterrupted()) {
                    long id = buffer.getLong();
                    if (id == this.server.getUdpPort() || this.failed.containsKey(id)) {
                        buffer.getInt();
                        continue;
                    }

                    Pair pair = new Pair(System.currentTimeMillis(), buffer.getInt());
                    this.verbose.log(Level.FINE, this.server.getServerId() + ": Received heartbeat of " + pair.getHeartbeat() + " for server " + id + " from server " + this.server.getPeerByAddress(sender) + " at node time " + pair.getTime());
                    // compare to your own map and update if necessary
                    if (heartBeats.containsKey(id)) {
                        if (pair.getHeartbeat() > heartBeats.get(id).getHeartbeat()) {
                            heartBeats.put(id, pair);
                            this.summary.log(Level.FINE, this.server.getServerId() + ": updated " + id + "'s heartbeat sequence to " + pair.getHeartbeat() + " based on message from " + this.server.getPeerByAddress(sender) + " at node time " + pair.getTime());
                        }
                    }
                }
                checkForFailed();
            }
        }

        // once you've updated everything, check for failed nodes
        checkForFailed();
    }

    /**
     * a method that checks through your nodes and sees if any need to be updated / tossed
     */
    private void checkForFailed() {
        // check for failed nodes
        for (Map.Entry<Long, Pair> entry : this.heartBeats.entrySet()) {
            if (isInterrupted()) {
                return;
            }
            if (entry.getKey() == this.server.getServerId()) {
                continue;
            }
            if (System.currentTimeMillis() - entry.getValue().getTime() >= FAIL) {
                // log and print that the server has failed, then add it to our failure map
                this.summary.log(Level.WARNING, "{0}: no heartbeat from server {1} - SERVER FAILED", new Object[]{this.server.getServerId(), entry.getKey()});
                System.out.println(this.server.getServerId() + ": no heartbeat from server " + entry.getKey() + " - SERVER FAILED");
                // add to and clean up internal data structures
                this.failed.put(entry.getKey(), System.currentTimeMillis());
                // tell the server that the peer has failed
                this.server.reportFailedPeer(entry.getKey());
                this.peerIds.remove(entry.getKey());
            }
        }

        // clean up failed nodes
        for (Map.Entry<Long, Long> entry : this.failed.entrySet()) {
            if (this.heartBeats.containsKey(entry.getKey())) {
                this.heartBeats.remove(entry.getKey());
            }
            if (System.currentTimeMillis() - entry.getValue() >= CLEANUP) {
                this.failed.remove(entry.getKey());
            }
        }
    }

    /**
     * The gossip-speaking method.
     * Sends out gossip to other peers.
     */
    private void speakNoEvil() {
        byte[] gossipMessage = new byte[heartBeats.size() * 12];
        ByteBuffer buffer = ByteBuffer.wrap(gossipMessage);

        // put the data into the buffer. We only care about the heartbeat time, as clock drift makes the timeInMillis unreliable
        for (Map.Entry<Long, Pair> entry : heartBeats.entrySet()) {
            buffer.putLong(entry.getKey());
            if (entry.getKey() == this.server.getServerId()) {
                buffer.putInt(entry.getValue().getHeartbeat() + 1);
                this.heartBeats.put(entry.getKey(), new Pair(System.currentTimeMillis(), entry.getValue().getHeartbeat() + 1));
            } else {
                buffer.putInt(entry.getValue().getHeartbeat());
            }
        }

        // pick a peer to send it to
        InetSocketAddress peer = pickARandomPeer();
        // send the gossip to your chosen peer
        this.server.sendMessage(MessageType.GOSSIP, gossipMessage, peer);
    }

    /**
     * Picks a random peer from the list of peers
     * @return the InetSocketAddress of the peer
     */
    private InetSocketAddress pickARandomPeer() {
        Long peerID = this.peerIds.get(this.random.nextInt(this.peerIds.size()));
        return this.server.getPeerByID(peerID);
    }

    /**
     * private class for heartbeat information
     */
    private class Pair {
        private long time;
        private int heartbeat;

        /**
         * Constructor for Pair
         * @param time - the time (in milliseconds) of the heartbeat
         * @param heartbeat - the heartbeat number
         */
        public Pair(long time, int heartbeat) {
            this.time = time;
            this.heartbeat = heartbeat;
        }

        public long getTime() {
            return this.time;
        }

        public int getHeartbeat() {
            return this.heartbeat;
        }

    }

}
