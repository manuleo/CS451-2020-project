package cs451;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class UniformReliableBroadcast {

    private final PerfectLink pf;
    private HashSet<String> delivered = new HashSet<>();
    private HashSet<String> pending = new HashSet<>();
    private HashMap<String, Integer> ack = new HashMap<>();
    private final List<Host> hosts;
    private final int id;
    private final int minCorrect;
    private LinkedBlockingQueue<Packet> messageToSendDown;
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
        this.pf = new PerfectLink(id, hosts.get(id-1).getPort(), hosts, messageToSendDown, messageDeliveredDown);
        this.id = id;
        int lenHost = hosts.size();
        this.minCorrect = lenHost/2 + 1;
        this.hosts = hosts;
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
                //String sentMessage = String.format("%d %s", id, message);
                List<String> sentMessages = new LinkedList<>();
                sentMessages.add(message);
                try {
                    Thread.sleep(Parser.sleepSend);
                } catch (InterruptedException e) {
                    System.out.println("Sleeping in URB error: " + e.toString());
                }
                messageToSendUp.drainTo(sentMessages);
                sentMessages = sentMessages.stream().map(m ->
                        String.format("%d %s", id, m)).collect(Collectors.toList());
                //System.out.println("Sending " + sentMessage);
                synchronized (lockPending) {
                    pending.addAll(sentMessages);
                }
                synchronized (lockAck) {
                    for (String sentMessage: sentMessages)
                            ack.put(sentMessage, 1);
                }
                send(sentMessages);
            }
        }
    }

    public void send(List<String> messagesToSend) {
        for (Host h: hosts) {
            if (id == h.getId()) {
                continue;
            }
            List<Packet> packets = messagesToSend.stream().map(m ->
            {
                try {
                    return new Packet(m, InetAddress.getByName(h.getIp()), h.getPort(), h.getId());
                } catch (UnknownHostException e) {
                    return null;
                }
            }).collect(Collectors.toList());
            messageToSendDown.addAll(packets);
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
                String got = null;
                List<String> messagesToSend = new LinkedList<>();
                try {
                    got = messageDeliveredDown.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting delivered packet in URB: " + e.toString());
                }
                List<String> gotPacks = new LinkedList<>();
                gotPacks.add(got);
                try {
                    Thread.sleep(Parser.sleepDeliver);
                } catch (InterruptedException e) {
                    System.out.println("Sleeping in URB deliver: " + e.toString());
                }
                messageDeliveredDown.drainTo(gotPacks);
                for (String gotPack: gotPacks) {
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
                        messagesToSend.add(sentMessage);
                        //System.out.println("Sending " + sentMessage);
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
                    delivered.addAll(deliverable);
                    messageToDeliverUp.addAll(deliverable);
                }
                send(messagesToSend);
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
                pf.equals(that.pf) &&
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
        return Objects.hash(pf, delivered, pending, ack, id, minCorrect,
                messageToSendDown, messageToSendUp, messageDeliveredDown, messageToDeliverUp);
    }
}
