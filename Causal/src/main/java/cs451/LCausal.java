package cs451;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class LCausal {
    private final UniformReliableBroadcast urb;
    private final HashMap<Integer, HashSet<MessagePacket>> pending = new HashMap<>();
    private final LinkedBlockingQueue<MessagePacket> messageToSendDown; // Messages to send to URB
    private final LinkedBlockingQueue<MessagePacket> messageDeliveredDown; // Messages delivered from URB
    private final LinkedBlockingQueue<String> messageToDeliverUp; // Messages to deliver to Main
    private final Coordinator coordinator; // Main coordinator for finishedBroadcasting()
    private int[] vcSend; // Vector clock of messages to send (to ensure others will respect process' causal relations)
    private int[] vcRec; // Vector clock of messages to receive (to ensure respect of other causal relations)
    private HashSet<Integer> influences; // List of pid of processes influencing this process
    private int id; // Process id
    private int m; // Number of messages to broadcast
    protected static int windowLimit = Constants.WINDOW_SIZE; // Number of sendable messages (changed by PL)
    protected static final Object lockSending = new Object(); // Lock waiting for more messages to be deliverable
    protected static final Object lockV = new Object(); // Lock for vector clock modification

    public LCausal(List<Host> hosts, int id, LinkedBlockingQueue<String> messageToDeliverUp, Coordinator coordinator,
                   int m, HashSet<Integer> influences) {
        // Init control structures and URB layer
        this.messageToDeliverUp = messageToDeliverUp;
        this.coordinator = coordinator;
        this.messageDeliveredDown = new LinkedBlockingQueue<>();
        this.messageToSendDown = new LinkedBlockingQueue<>();
        this.urb = new UniformReliableBroadcast(hosts, id, messageToSendDown, messageDeliveredDown);
        this.vcRec = new int[hosts.size()];
        this.vcSend = new int[hosts.size()];
        this.influences = new HashSet<>();
        this.id = id;
        this.m = m;
        this.influences = influences;
        Arrays.fill(this.vcRec, 1);
        Arrays.fill(this.vcSend, 1);
        receiveAndDeliver();
        broadcast();
    }

    /**
     * Class (i.e. thread) that will perform the message broadcasting at LCausal (by sending them to URB)
     */
    private class Broadcast extends Thread {
        /**
         * Run the broadcast thread
         */
        @Override
        public void run() {
            int lsn = 1;
            while(lsn<=m) {
                // Wait until Perfect Link layer says it's possible to broadcast something
                synchronized (lockSending) { // Acquire the lock before waiting
                    while(lsn==windowLimit+1) {
                        try {
                            lockSending.wait();
                        } catch (InterruptedException e) {
                            System.out.println("Interrupted LCausal waiting: " + e.toString());
                        }
                    }
                    try {
                        // TODO: sleep here to enforce more relations to be built! Remember to remove!!
                        Thread.sleep(100);
                        int[] W;
                        synchronized (lockV) {
                            // Prepare vector clock W to send
                            W = vcSend.clone();
                            W[id - 1] = lsn; // Put process lsn inside W clock
                            synchronized (Main.lockOut) {
                                // Directly add the message as broadcasted, even if we fail before this is done
                                // Doing it here avoid the edge case where the delivery thread put the message
                                // as delivered immediately after we copied the VC but before we put it in broadcast
                                // Creating a causal relationship in the output that do not exist
                                Main.out.add("b " + lsn);
                            }
                        }
                        // Create a MessagePacket that contains lsn as header info + the vector clock
                        MessagePacket messagePacket = new MessagePacket(String.valueOf(lsn), W);
                        messageToSendDown.put(messagePacket); // Send it down
                    } catch (InterruptedException e) {
                        System.out.println("Sending message in main error: " + e.toString());
                    }
                }
                lsn++; // Increase the lsn for next packet
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
         * Check that the vector clock W associated to a message is deliverable by comparing it
         * with the vector clock of received messages
         * @param W Vector Clock to consider
         * @return true if W <= VcRec, else otherwise
         */
        private boolean deliverableVC (int[] W) {
            for (int i = 0; i < W.length; i++) {
                if (W[i] > vcRec[i])
                    return false; // At least one element is >, return false
            }
            return true; // All elements are <=
        }

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
                MessagePacket got = null;
                try {
                    got = messageDeliveredDown.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting delivered packet in LCausal: " + e.toString());
                }
                List<MessagePacket> gotPacks = new LinkedList<>();
                // Add everything you can from the queue
                gotPacks.add(got);
                messageDeliveredDown.drainTo(gotPacks);
                // Save from who we delivered something at URB (we check only these because we may deliver something)
                HashSet<Integer> pids = new HashSet<>();
                for (MessagePacket gotPack: gotPacks) {
                    // Get [pid, MessagePacket] array and add pid to pids and message to the pending of that pid
                    String[] gotSplit = gotPack.getMessage().split(" ");
                    int pid = Integer.parseInt(gotSplit[0]);
                    pids.add(pid);
                    HashSet<MessagePacket> messagePackets = pending.getOrDefault(pid, new HashSet<>());
                    messagePackets.add(gotPack);
                    pending.put(pid, messagePackets);
                }
                // Cycle over the pids
                for (int pid: pids) {
                    // Save in this list all the message we can deliver in this round
                    List<MessagePacket> allDelivers = new LinkedList<>();
                    while (true) {
                        // Check if we can deliver something from the pid
                        Optional<MessagePacket> toDeliver = pending.get(pid).stream()
                                .filter(x -> deliverableVC(x.getW()))
                                .findFirst();
                        // If we can't deliver anything anymore
                        if(toDeliver.isEmpty()) {
                            // If we can deliver at least 1 message
                            if (allDelivers.size()!=0) {
                                // Send the messages for delivery in the queue
                                messageToDeliverUp.addAll(allDelivers.stream()
                                                .map(MessagePacket::getMessage)
                                                .collect(Collectors.toList()));
                            }
                            break;
                        }
                        else {
                            // If we can deliver the message
                            vcRec[pid-1]++; // Save that we received a message for that pid by increasing its lsn
                            if (influences.contains(pid)) {
                                // If the pid influences us, save the influence in vcSend by increasing its lsn
                                // This way all the other processes will know that they have to deliver this message
                                // before delivering anything from this process
                                synchronized (lockV) {
                                    vcSend[pid-1]++;
                                }
                            }
                            // Save the message to be delivered later on in the batch
                            // and remove it from the pending list
                            allDelivers.add(toDeliver.get());
                            pending.get(pid).remove(toDeliver.get());
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

}
