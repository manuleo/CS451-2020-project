package cs451;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * FIFO layer class to perform in order delivery
 */
public class FIFO {
    private final UniformReliableBroadcast urb;
    private final HashMap<Integer, HashSet<String>> pending = new HashMap<>(); // Messages not delivered yet by process
    private final int[] seqNums; // Next lsn to deliver by process
    private final LinkedBlockingQueue<String> messageToSendDown; // Messages to send to URB
    private final LinkedBlockingQueue<String> messageDeliveredDown; // Messages delivered from URB
    private final LinkedBlockingQueue<String> messageToDeliverUp; // Messages to deliver to Main
    private final Coordinator coordinator; // Main coordinator for finishedBroadcasting()
    protected static int windowLimit = Constants.WINDOW_SIZE; // Number of sendable messages (changed by PL)
    protected static final Object lockSending = new Object(); // Lock waiting for more messages to be deliverable

    /**
     * Init FIFO and start broadcast/delivery
     * @param hosts list of hosts to use
     * @param id id of this process
     * @param messageToDeliverUp queue to send message to the layer above (Main) for delivery
     */
    public FIFO(List<Host> hosts, int id, LinkedBlockingQueue<String> messageToDeliverUp, Coordinator coordinator) {
        // Init control structures and URB layer
        this.messageToDeliverUp = messageToDeliverUp;
        this.coordinator = coordinator;
        this.messageDeliveredDown = new LinkedBlockingQueue<>();
        this.messageToSendDown = new LinkedBlockingQueue<>();
        this.urb = new UniformReliableBroadcast(hosts, id, messageToSendDown, messageDeliveredDown);
        this.seqNums = new int[hosts.size()];
        Arrays.fill(this.seqNums, 1);
        // Start delivering
        receiveAndDeliver();
        // Start broadcasting
        broadcast();
    }

    /**
     * Class (i.e. thread) that will perform the message broadcasting at FIFO (by sending them to URB)
     */
    private class Broadcast extends Thread {
        /**
         * Run the broadcast thread
         */
        @Override
        public void run() {
            int i = 1;
            while(i<=Main.m) {
                // Wait until Perfect Link layer says it's possible to broadcast something
                synchronized (lockSending) { // Acquire the lock before waiting
                    while(i==windowLimit+1) {
                        try {
                            lockSending.wait();
                        } catch (InterruptedException e) {
                            System.out.println("Interrupted FIFO waiting: " + e.toString());
                        }
                    }
                    try {
                        // Broadcast message i
                        messageToSendDown.put(String.valueOf(i));
                    } catch (InterruptedException e) {
                        System.out.println("Sending message in main error: " + e.toString());
                    }
                    synchronized (Main.lockOut) {
                        // Directly add the message as broadcasted, even if we fail before this is done
                        // that will still be expected behavior
                        Main.out.add("b " + i);
                    }
                    i++;
                }
            }
            // After the end of the cycle we finished broadcasting (messages will eventually be sent): we can signal it
            System.out.println("Signaling end of broadcasting messages");
            coordinator.finishedBroadcasting();
        }
    }

    /**
     * Start broadcasting thread
     */
    public void broadcast() {
        new Broadcast().start();
    }

    /**
     * Class (i.e. thread) that will perform the message delivery
     */
    private class Receive extends Thread {
        /**
         * Run receiver thread
         * This thread will check that for every message delivered from URB
         * if we can actually deliver it or no, keeping the messages to deliver in the pending map
         * and removing from that map the one we can deliver.
         */
        @Override
        public void run() {
            while (true) {
                // Get deliverable messages from the layer below (URB)
                String got = null;
                try {
                    got = messageDeliveredDown.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting delivered packet in FIFO: " + e.toString());
                }
                List<String> gotPacks = new LinkedList<>();
                // Add everything you can from the queue
                gotPacks.add(got);
                messageDeliveredDown.drainTo(gotPacks);
                // Save from who we delivered something at URB (we check only these because we may deliver something)
                HashSet<Integer> pids = new HashSet<>();
                for (String gotPack: gotPacks) {
                    // Get [pid, mess] array and add pid to pids and message to the pending of that pid
                    String[] gotSplit = gotPack.split(" ");
                    int pid = Integer.parseInt(gotSplit[0]);
                    pids.add(pid);
                    HashSet<String> messages = pending.getOrDefault(pid, new HashSet<>());
                    messages.add(gotPack);
                    pending.put(pid, messages);
                }
                // Cycle over the pids
                for (int pid: pids) {
                    // Save in this list all the message we can deliver in this round
                    List<String> allDelivers = new LinkedList<>();
                    HashSet<String> newMess = pending.get(pid);
                    while (true) {
                        // Check if we can deliver something from the pid (i.e. we have a message with the correct lsn)
                        Optional<String> toDeliver = newMess.stream()
                                .filter(x -> Integer.parseInt(x.split(" ")[1]) == seqNums[pid-1])
                                .findFirst();
                        // If we can't deliver anything anymore
                        if(toDeliver.isEmpty()) {
                            // If we can deliver at least 1 message
                            if (allDelivers.size()!=0) {
                                // Remove everything we have delivered from the pending
                                newMess.removeAll(allDelivers);
                                pending.put(pid, newMess);
                                // Send the messages for delivery in the queue
                                messageToDeliverUp.addAll(allDelivers);
                            }
                            break;
                        }
                        else {
                            // If we can deliver the message, increase the lsn we want to get
                            // and add the message to the ones will deliver
                            seqNums[pid-1]+=1;
                            allDelivers.add(toDeliver.get());
                        }
                    }
                }
            }
        }
    }

    /**
     * Start receiver thread
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
        FIFO fifo = (FIFO) o;
        return Objects.equals(urb, fifo.urb) &&
                Objects.equals(pending, fifo.pending) &&
                Arrays.equals(seqNums, fifo.seqNums) &&
                Objects.equals(messageToSendDown, fifo.messageToSendDown) &&
                Objects.equals(messageDeliveredDown, fifo.messageDeliveredDown) &&
                Objects.equals(messageToDeliverUp, fifo.messageToDeliverUp) &&
                Objects.equals(coordinator, fifo.coordinator);
    }

    /**
     * @return hashcode
     */
    @Override
    public int hashCode() {
        int result = Objects.hash(urb, pending, messageToSendDown,
                messageDeliveredDown, messageToDeliverUp, coordinator);
        result = 31 * result + Arrays.hashCode(seqNums);
        return result;
    }
}
