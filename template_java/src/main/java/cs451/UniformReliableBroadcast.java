package cs451;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class UniformReliableBroadcast {

    private final BestEffortBroadcast beb;
    HashSet<String> delivered = new HashSet<>();
    HashSet<String> pending = new HashSet<>();
    HashMap<String, Integer> ack = new HashMap<>();
    private final int id;
    private final int minCorrect;
    private LinkedBlockingQueue<String> messageToSendDown;
    private LinkedBlockingQueue<String> messageToSendUp;
    private LinkedBlockingQueue<String> messageDeliveredDown;
    private LinkedBlockingQueue<String> messageToDeliverUp;
    private static final Object lockPending = new Object();
    private static final Object lockAck = new Object();

    public UniformReliableBroadcast(List<Host> hosts, int id, LinkedBlockingQueue<String> messageToSendUp,
                                    LinkedBlockingQueue<String> messageToDeliverUp) {
        this.messageToSendUp = messageToSendUp;
        this.messageToDeliverUp = messageToDeliverUp;
        this.messageDeliveredDown = new LinkedBlockingQueue<>();
        this.messageToSendDown = new LinkedBlockingQueue<>();
        this.beb = new BestEffortBroadcast(hosts, id, messageToSendDown, messageDeliveredDown);
        this.id = id;
        int lenHost = hosts.size();
        minCorrect = lenHost/2 + 1;
        receiveAndDeliver();
        broadcast();
    }


    private class Broadcast extends Thread{

        @Override
        public void run() {
            while(true) {
                String message = null;
                try {
                    message = messageToSendUp.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting message in URB error: " + e.toString());
                }
                String sentMessage = String.format("%d %s", id, message);
                //System.out.println("Sending " + sentMessage);
                synchronized (lockPending) {
                    pending.add(sentMessage);
                }
                synchronized (lockAck) {
                    ack.put(sentMessage, 1);
                }
                try {
                    messageToSendDown.put(sentMessage);
                } catch (InterruptedException e) {
                    System.out.println("Sending message in URB error: " + e.toString());
                }
            }
        }
    }

    public void broadcast() {
        new Broadcast().start();
    }

    private class Receive extends Thread {

        private synchronized boolean canDeliver(String key) {
            return ack.getOrDefault(key, 0) >= minCorrect;
        }

        @Override
        public void run() {
            String key;
            while (true) {
                String gotPack = null;
                try {
                    gotPack = messageDeliveredDown.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting delivered packet in URB: " + e.toString());
                }
                String[] gotSplit = gotPack.split(" ");
                //System.out.println("gotPack: " + gotPack);
                if (gotSplit.length == 2)
                    key = gotPack;
                else
                    key = gotSplit[1] + " " + gotSplit[2];

//                if (gotSplit.length==2)
//                    System.out.println("Received " + gotPack);
//                System.out.println("key: " + key);
//                System.out.println("ACKs: ");
//                for (Map.Entry<String, Integer> entry : ack.entrySet()) {
//                    System.out.println(entry.getKey() + "=" + entry.getValue());
//                }
                synchronized (lockAck) {
                    if (!ack.containsKey(key))
                        ack.put(key, 2);
                    else {
                        Integer oldAck = ack.get(key);
                        ack.put(key, oldAck + 1);
                    }
                }
                //System.out.println("ack[key]: " + ack.get(key));
                //System.out.println("Pending: " + pending);
                if (!pending.contains(key)) {
                    synchronized (lockPending) {
                        pending.add(key);
                    }
                    String sentMessage = String.format("%d %s", id, key);
                    //System.out.println("Sending " + sentMessage);
                    try {
                        messageToSendDown.put(sentMessage);
                    } catch (InterruptedException e) {
                        System.out.println("Sending message in URB error: " + e.toString());
                    }
                }
                List <String> deliverable;
                synchronized (lockPending) {
                    deliverable = pending.stream()
                            .filter(this::canDeliver)
                            .filter(p -> !delivered.contains(p))
                            .collect(Collectors.toList());
                }
                //System.out.println("Deliverable: " + deliverable);
                if (deliverable.size()!=0) {
                    String deliver = deliverable.get(0);
                    delivered.add(deliver);
                    try {
                        messageToDeliverUp.put(deliver);
                    } catch (InterruptedException e) {
                        System.out.println("Delivering message in URB error: " + e.toString());
                    }
                }
            }
        }
    }

    public void receiveAndDeliver() {
        Receive rec = new Receive();
        rec.start();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UniformReliableBroadcast that = (UniformReliableBroadcast) o;
        return id == that.id &&
                minCorrect == that.minCorrect &&
                beb.equals(that.beb) &&
                delivered.equals(that.delivered) &&
                pending.equals(that.pending) &&
                ack.equals(that.ack) &&
                messageToSendDown.equals(that.messageToSendDown) &&
                messageToSendUp.equals(that.messageToSendUp) &&
                messageDeliveredDown.equals(that.messageDeliveredDown) &&
                messageToDeliverUp.equals(that.messageToDeliverUp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(beb, delivered, pending, ack, id, minCorrect,
                messageToSendDown, messageToSendUp, messageDeliveredDown, messageToDeliverUp);
    }
}
