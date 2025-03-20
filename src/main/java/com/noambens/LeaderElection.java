package com.noambens;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.noambens.PeerServer.ServerState;

/**
 * We are implemeting a simplfied version of the election algorithm. For the
 * complete version which covers all possible scenarios, see
 * https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913
 */
public class LeaderElection {

    private final Logger logger;

    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 1500;

    /**
     * The starting value of the "exponential back-off" chain
     */
    private final static int startingInterval = 50;

    /**
     * Upper bound on the amount of time between two consecutive notification
     * checks. This impacts the amount of time to get the system up again after
     * long partitions. Currently 30 seconds.
     */
    private final static int maxNotificationInterval = 5000;

    // "proposed" variables
    private long proposedEpoch;
    private long proposedLeader;

    private final PeerServer server;
    private final LinkedBlockingQueue<Message> incoming;

    private final Map<Long, ElectionNotification> votes;

    /**
     * constructor for one server's leader election
     *
     * @param server - the peer server in reference here
     * @param incomingMessages - the message queue of the server
     * @param logger - the logger of the peerserver that was passed in
     */
    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, Logger logger) {

        // create logger
        this.logger = logger;

        // set other instance variables, get the 
        this.server = server;

        this.incoming = incomingMessages;

        // set the proposed leader and epoch to the current server's values
        if (this.server.getPeerState() == ServerState.OBSERVER) {
            this.proposedEpoch = -1;
            this.proposedLeader = -1;
        }
        else {
            this.proposedEpoch = this.server.getPeerEpoch();
            this.proposedLeader = this.server.getServerId();
        }

        // create a map for the votes
        this.votes = new HashMap<>();
        this.logger.log(Level.FINE, "@{0}: STARTED LeaderElection, finished constructor", new Object[]{this.server.getUdpPort()});
    }

    /**
     * Note that the logic in the comments below does NOT cover every last
     * "technical" detail you will need to address to implement the election
     * algorithm. How you store all the relevant state, etc., are details you
     * will need to work out.
     *
     * @return the elected leader
     */
    public synchronized Vote lookForLeader() {
        try {
            //send initial notifications to get things started
            sendNotifications();
            //Loop in which we exchange notifications with other servers until we find a leader
            while (true) {
                long backout = startingInterval;
                //Remove next notification from queue
                Message msg = this.incoming.poll(backout, TimeUnit.MILLISECONDS);
                //If no notifications received...
                while (msg == null || msg.getMessageType() != Message.MessageType.ELECTION) {
                    if (msg != null) {
                        backout = startingInterval;
                        msg = this.incoming.poll(backout, TimeUnit.MILLISECONDS);
                        continue;
                    }
                    //...resend notifications to prompt a reply from others
                    sendNotifications();
                    //...use exponential back-off when notifications not received but no longer than maxNotificationInterval...
                    backout = Math.min(backout * 2, maxNotificationInterval);
                    msg = this.incoming.poll(backout, TimeUnit.MILLISECONDS);
                }

                //If we did get a message...
                ElectionNotification recv = getNotificationFromMessage(msg);

                //...if it's for an earlier epoch, or from an observer, ignore it.
                considerMessage(recv);

                //If I have enough votes to declare my currently proposed leader as the leader...
                Vote curr = new Vote(this.proposedLeader, this.proposedEpoch);
                if (haveEnoughVotes(votes, curr)) {

                    //..do a last check to see if there are any new votes for a higher ranked possible leader. If there are, continue in my election "while" loop.
                    boolean reset = false;
                    while ((msg = this.incoming.poll(LeaderElection.finalizeWait, TimeUnit.MILLISECONDS)) != null) {
                        if (msg.getMessageType() != Message.MessageType.ELECTION) continue;
                        recv = getNotificationFromMessage(msg);
                        if (considerMessage(recv)) {
                            reset = true;
                            break;
                        }
                    }
                    if (reset) continue;

                    //If there are no new relevant message from the reception queue, set my own state to either LEADING or FOLLOWING and RETURN the elected leader.
                    return this.acceptElectionWinner(new ElectionNotification(this.proposedLeader, this.server.getPeerState(), this.server.getServerId(), this.proposedEpoch));
                }
            }
        } catch (Exception e) {
            this.logger.log(Level.SEVERE, "Exception occurred during election; election canceled", e);
        }
        return null;
    }

    /**
     * Helper method for reviewing and considering the received messages
     * @param recv
     */
    private boolean considerMessage(ElectionNotification recv) {
        if (recv.getPeerEpoch() < server.getPeerEpoch() || recv.getState() == ServerState.OBSERVER) {
            return false;
        }
        else {
            //...if the received message has a vote for a leader which supersedes mine, change my vote (and send notifications to all other voters about my new vote).
            if (supersedesCurrentVote(recv.getProposedLeaderID(), recv.getPeerEpoch())) {
                this.proposedEpoch = recv.getPeerEpoch();
                this.proposedLeader = recv.getProposedLeaderID();
                sendNotifications();
                return true;
            }

            //(Be sure to keep track of the votes I received and who I received them from.)
            votes.put(recv.getSenderID(), recv);
        }
        return false;
    }

    /**
     * accepts the election winner
     *
     * @param n - the election to accept
     */
    private Vote acceptElectionWinner(ElectionNotification n) {
        Vote v = new Vote(n.getProposedLeaderID(), n.getPeerEpoch());
        
        //set my state to either LEADING or FOLLOWING, but ONLY if I'm not already an OBSERVER.
        if (v.getProposedLeaderID() == this.server.getServerId()) {
            this.server.setPeerState(ServerState.LEADING);
        }
        else {
            this.server.setPeerState(ServerState.FOLLOWING);
        }
        try {
            this.server.setCurrentLeader(v);
        } catch (IOException e) {
            this.logger.warning("could not accept election winner");
            return null;
        }
        //clear out the incoming queue before returning
        this.incoming.clear();
        this.logger.log(Level.FINE, "@{0}: ACCEPTED leader: {1}", new Object[]{this.server.getUdpPort(), v});
        return v;
    }

    /**
     * We return true if one of the following three cases hold: 1- New epoch is
     * higher 2- New epoch is the same as current epoch, but server id is
     * higher.
     *
     * @param newId - the id of the comparing leader
     * @param newEpoch - the epoch of the comparing vote
     * @return
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have
     * sufficient support for the proposal to declare the end of the election
     * round. Who voted for who isn't relevant, we only care that each server
     * has one current vote.
     *
     * @param votes - a map of server IDs to ElectionNotifiations
     * @param proposal - the proposal to check whether or not you have enough of
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        double voteCount = 1;
        for (Long id : votes.keySet()) {
            if (votes.get(id).getProposedLeaderID() == proposal.getProposedLeaderID()) {
                voteCount++;
            }
        }
        return (voteCount > (this.server.getQuorumSize() / 2.0));
        //is the number of votes for the proposal > the size of my peer server’s quorum?
    }

    /**
     * takes the given election notification and... turns it into a byte array
     * using the super method for stringify we already pass in the other aspects
     * of election notification along with
     *
     * @param notification - the notification to turn into a byte array
     * @return - a byte array representation of the notification, in the same
     * order as it would be instantiated in if you were to remove everything in
     * order.
     */
    public static byte[] buildMsgContent(ElectionNotification notification) {
        if (notification == null) throw new IllegalArgumentException();
        byte[] bytes = new byte[30];
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // adds the data in the same order as it would take to undo it and instantiate it as a new LeaderElection
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());
        return buffer.array();
    }

    /**
     * send vote notifications to all other peers *once our vote changes* - this
     * would include the original send
     */
    protected void sendNotifications() {
        this.server.sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(new ElectionNotification(this.proposedLeader, this.server.getPeerState(), this.server.getServerId(), this.proposedEpoch)));
    }

    /**
     * reads the message and extracts the election notification from it.
     *
     * @param received - the message to decode
     * @return - a new instance of election notification, which would return
     * true if called .equals() on with the original notification
     */
    public static ElectionNotification getNotificationFromMessage(Message received) {
        if (received == null || received.getMessageContents() == null) {
            throw new IllegalArgumentException("Invalid message or contents");
        }

        ByteBuffer buffer = ByteBuffer.wrap(received.getMessageContents());
        return new ElectionNotification(buffer.getLong(), PeerServer.ServerState.getServerState(buffer.getChar()), buffer.getLong(), buffer.getLong());
    }

}
