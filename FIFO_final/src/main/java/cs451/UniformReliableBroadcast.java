package cs451;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * URB layer class to ensure a uniform reliable broadcast in delivery
 */
public class UniformReliableBroadcast {

    private final PerfectLink pl;
    private final HashSet<String> delivered = new HashSet<>(); // Already delivered messages
    private final HashSet<String> pending = new HashSet<>(); // Yet to deliver messages
    private final List<Host> hosts; // List of hosts
    private final int id;
    private final int minCorrect; // Minimum number of correct processes
    private final LinkedBlockingQueue<Packet> messageToSendDown; // Messages to send to PL
    private final LinkedBlockingQueue<String> messageToSendUp; // Messages to send received from FIFO
    private final LinkedBlockingQueue<String> messageDeliveredDown; // Messages delivered from PL
    private final LinkedBlockingQueue<String> messageToDeliverUp; // Messages to deliver to FIFO
    private static final HashMap<String, Integer> ack = new HashMap<>(); // Count number of acks for each message
    private static final Object lockPending = new Object(); // Lock to avoid concurrent modifications to pending
    private static final Object lockAck = new Object(); // Lock to avoid concurrent modifications to ack

    /**
     * Init the URB layer and start sending/delivering
     * @param hosts list of hosts to use
     * @param id id of this process
     * @param messageToDeliverUp queue to send message to the layer above (FIFO) for delivery
     * @param messageToSendUp queue to receive message from the layer above (FIFO) for sending
     */
    public UniformReliableBroadcast(List<Host> hosts, int id, LinkedBlockingQueue<String> messageToSendUp,
                                    LinkedBlockingQueue<String> messageToDeliverUp) {
        // Init control structures and Perfect Link
        this.messageToSendUp = messageToSendUp;
        this.messageToDeliverUp = messageToDeliverUp;
        this.messageDeliveredDown = new LinkedBlockingQueue<>();
        this.messageToSendDown = new LinkedBlockingQueue<>();
        this.pl = new PerfectLink(id, hosts.get(id-1).getPort(), hosts, messageToSendDown, messageDeliveredDown);
        this.id = id;
        int lenHost = hosts.size();
        this.minCorrect = lenHost/2 + 1; // >N/2 correct hosts by assumption
        this.hosts = hosts;
        // Start delivering and broadcasting
        receiveAndDeliver();
        broadcast();
    }


    /**
     * Class (i.e. thread) that will perform the message broadcasting at URB
     */
    private class Broadcast extends Thread{
        /**
         * Run the broadcast thread
         * Get every message of FIFO and send them to the PL to broadcast them
         */
        @Override
        public void run() {
            while(true) {
                // Get messages to send from the layer above (all the batch you can get)
                String message = null;
                try {
                    message = messageToSendUp.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting message in URB error: " + e.toString());
                }
                List<String> sentMessages = new LinkedList<>();
                sentMessages.add(message);
                messageToSendUp.drainTo(sentMessages);
                // Add the process id in front of the message lsn
                sentMessages = sentMessages.stream().map(m ->
                        String.format("%d %s", id, m)).collect(Collectors.toList());
                synchronized (lockPending) {
                    // Add the messages to the pending set (checked later in delivering)
                    pending.addAll(sentMessages);
                }
                synchronized (lockAck) {
                    // Add the message to the Hashmap of acked, stating that one process (myself) delivered it
                    // This is basically a BEB deliver (no BEB layer in this implementation)
                    // to itself without going into the network
                    for (String sentMessage: sentMessages)
                            ack.put(sentMessage, 1);
                }
                // Send the messages, indicating they come from FIFO
                send(sentMessages, Packet.packType.FIFO);
            }
        }
    }

    /**
     * Broadcast a batch of messages (i.e. pass them to the PL) of type given (FIFO or URB, used at PL level)
     * @param messagesToSend list of messages to send
     * @param type packet type, indicating if the packet come from FIFO or is a URB re-broadcast
     */
    private void send(List<String> messagesToSend, Packet.packType type) {
        // Cycle over the host, creating a "Packet" with same message but different host endpoint
        for (Host h: hosts) {
            // If I'm at myself, continue (packet already delivered)
            if (id == h.getId()) {
                continue;
            }
            // Map each message to a packet
            List<Packet> packets = messagesToSend.stream().map(m ->
            {
                try {
                    // Create packet with message m, address h.getIp(), port h.getPort(), id = h.getId() and given type
                    return new Packet(m, InetAddress.getByName(h.getIp()), h.getPort(), h.getId(), type);
                } catch (UnknownHostException e) {
                    return null;
                }
            }).collect(Collectors.toList());
            // Add all the packets to the queue that will be read by Perfect Link
            messageToSendDown.addAll(packets);
        }
    }

    /**
     * Start the broadcasting thread
     */
    public void broadcast() {
        new Broadcast().start();
    }

    /**
     * Class (i.e. thread) that will perform the message delivering at URB
     */
    private class Receive extends Thread {

        /**
         * Check if the given message can be delivered (it was received by at least N/2 processes)
         * @param key the message to check
         * @return true if the message can be delivered, false otherwise
         */
        private synchronized boolean canDeliver(String key) {
            return ack.getOrDefault(key, 0) >= minCorrect;
        }

        /**
         * Run the receiving thread
         */
        @Override
        public void run() {
            String key;
            while (true) {
                // Get everything you can from the layer below (Perfect Link) to build a batch
                String got = null;
                List<String> messagesToSend = new LinkedList<>();
                try {
                    got = messageDeliveredDown.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting delivered packet in URB: " + e.toString());
                }
                List<String> gotPacks = new LinkedList<>();
                gotPacks.add(got);
                messageDeliveredDown.drainTo(gotPacks);
                // Process every packet received
                for (String gotPack: gotPacks) {
                    String[] gotSplit = gotPack.split(" ");
                    // Every message string can be of two type:
                    // 1. id m -> Message arrived from the original broadcaster with pid = id
                    // 2. pidR id m -> Message m from process with pid = id that was relied by pidR
                    // We get from such string the original pair "id m" and use this as key for the pending/ack maps
                    if (gotSplit.length == 2)
                        key = gotPack;
                    else
                        key = gotSplit[1] + " " + gotSplit[2];

                    synchronized (lockAck) { // Need to lock before accessing the map
                        // If it's the first time message is seen ->
                        //    add to the ack saying that 2 processes have seen it (myself and the sender)
                        //    (like before, it's like BEB delivering to ourselves)
                        if (!ack.containsKey(key))
                            ack.put(key, 2);
                        // If we have seen the message before ->
                        //    Increase the number of acks for the message by 1
                        else {
                            Integer oldAck = ack.get(key);
                            ack.put(key, oldAck + 1);
                        }
                    }

                    // Check if it's the first time we see the message. If it is, add to pending
                    // and add to set of messages will be broadcasted
                    if (!pending.contains(key)) {
                        synchronized (lockPending) {
                            pending.add(key);
                        }
                        String sentMessage = String.format("%d %s", id, key);
                        messagesToSend.add(sentMessage);
                    }
                }

                // After batch is processed, check what we can deliver
                List <String> deliverable;
                synchronized (lockPending) {
                    // Filter the pending set by checking the message can be delivered (canDeliver(m))
                    // and that it wasn't delivered before
                    deliverable = pending.stream()
                            .filter(this::canDeliver)
                            .filter(p -> !delivered.contains(p))
                            .collect(Collectors.toList());
                }
                // Add all deliverable messages to delivered set and to the queue that will be checked by FIFO
                if (deliverable.size()!=0) {
                    delivered.addAll(deliverable);
                    messageToDeliverUp.addAll(deliverable);
                }
                // Broadcast the messages we added to the pending set to everyone, indicating these are URB messages
                send(messagesToSend, Packet.packType.URB);
            }
        }
    }

    /**
     * Start the receiving thread
     */
    public void receiveAndDeliver() {
        new Receive().start();
    }

    /**
     * Check equality
     * @param o Another object
     * @return true/false if objects are or not the same
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UniformReliableBroadcast that = (UniformReliableBroadcast) o;
        return id == that.id &&
                minCorrect == that.minCorrect &&
                Objects.equals(pl, that.pl) &&
                Objects.equals(delivered, that.delivered) &&
                Objects.equals(pending, that.pending) &&
                Objects.equals(hosts, that.hosts) &&
                Objects.equals(messageToSendDown, that.messageToSendDown) &&
                Objects.equals(messageToSendUp, that.messageToSendUp) &&
                Objects.equals(messageDeliveredDown, that.messageDeliveredDown) &&
                Objects.equals(messageToDeliverUp, that.messageToDeliverUp);
    }

    /**
     * @return hashcode
     */
    @Override
    public int hashCode() {
        return Objects.hash(pl, delivered, pending, hosts, id, minCorrect, messageToSendDown,
                messageToSendUp, messageDeliveredDown, messageToDeliverUp);
    }
}
