package cs451;

import java.util.*;
import java.util.stream.Collectors;

public class UniformReliableBroadcast {

    private final BestEffortBroadcast beb;
    HashSet<String> delivered = new HashSet<>();
    HashSet<String> pending = new HashSet<>();
    HashMap<String, Integer> ack = new HashMap<>();
    private final int id;
    private final int minCorrect;
    private static final Object lockPending = new Object();
    private static final Object lockAck = new Object();

    public UniformReliableBroadcast(List<Host> hosts, int id) {
        this.beb = new BestEffortBroadcast(hosts, id);
        this.id = id;
        int lenHost = hosts.size();
        if (lenHost==2) // Corner case: with only 2 hosts no one can crash (or "minority only can crash" not respected)
            minCorrect = 2;
        else if (lenHost%2!=0)
            minCorrect = lenHost - lenHost/2;
        else
            minCorrect = lenHost - lenHost/2 - 1;
    }


    private class Broadcast extends Thread{
        private final String message;
        public Broadcast(String message) {
            this.message = message;
        }

        @Override
        public void run() {
            String sentMessage = String.format("%d %s", id, message);
            //System.out.println("Sending " + sentMessage);
            synchronized (lockPending) {
                pending.add(sentMessage);
            }
            synchronized (lockAck) {
                ack.put(sentMessage, 1);
            }
            beb.broadcast(sentMessage);
        }
    }

    public void broadcast(String message) {
        new Broadcast(message).start();
    }

    private class Receive extends Thread {
        List<String> deliver;

        private boolean canDeliver(String key) {
            return ack.getOrDefault(key, 0) >= minCorrect;
        }

        @Override
        public void run() {
            String key;
            while (true) {
                List<String> gotPacks = beb.receiveAndDeliver();
                for (String gotPack: gotPacks) {
                    String[] gotSplit = gotPack.split(" ");
                    //System.out.println("gotPack: " + gotPack);
                    if (gotSplit.length == 2)
                        key = gotPack;
                    else
                        key = gotSplit[1] + " " + gotSplit[2];

//                    if (gotSplit.length==2)
//                        System.out.println("Received " + gotPack);
                    //System.out.println("key: " + key);
                    //System.out.println("ACKs: ");
//                for (Map.Entry<String, Integer> entry : ack.entrySet()) {
//                    System.out.println(entry.getKey() + "=" + entry.getValue());
//                }
                    synchronized (lockAck) {
                        if (!ack.containsKey(key))
                            ack.put(key, 1);
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
                        beb.broadcast(sentMessage);
                    }
                }
                List <String> deliverable;
                synchronized (lockPending) {
                     deliverable = pending.stream()
                            .filter(this::canDeliver)
                            .filter(p -> !delivered.contains(p))
                            .collect(Collectors.toList()); //TODO: check why an exception at this line
                }
                //System.out.println("Deliverable: " + deliverable);
                if (deliverable.size()!=0) {
                    deliver = deliverable;
                    delivered.addAll(deliver);
                    break;
                }
            }
        }

        public List<String> getDeliver() {
            return deliver;
        }
    }

    public List<String> receiveAndDeliver() {
        Receive rec = new Receive();
        rec.start();
        try {
            rec.join();
        } catch (InterruptedException e) {
            System.out.println("Exception when joining to receive in BestEffortBroadcast " + e.toString());
        }
        return deliver(rec.getDeliver());
    }

    private List<String> deliver(List<String> deliverable) {
        return deliverable;
    }
}
