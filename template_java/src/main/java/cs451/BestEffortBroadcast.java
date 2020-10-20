package cs451;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

public class BestEffortBroadcast {
    private List<Host> hosts;
    private int id;
    private PerfectLink pf;
    private HashSet<String> myMessage = new HashSet<>();
    private LinkedBlockingQueue<Packet> messageToSendDown;
    private LinkedBlockingQueue<String> messageToSendUp;
    private LinkedBlockingQueue<String> messageDeliveredDown;
    private LinkedBlockingQueue<String> messageToDeliverUp;

    public BestEffortBroadcast(List<Host> hosts, int id, LinkedBlockingQueue<String> messageToSendUp,
                               LinkedBlockingQueue<String> messageToDeliverUp) {
        this.hosts = hosts;
        this.id = id;
        this.messageToSendUp = messageToSendUp;
        this.messageToDeliverUp = messageToDeliverUp;
        this.messageDeliveredDown = new LinkedBlockingQueue<>();
        this.messageToSendDown = new LinkedBlockingQueue<>();
        this.pf = new PerfectLink(id, hosts.get(id-1).getPort(), hosts, messageToSendDown, messageToDeliverUp);
        // No need to use an extra queue to deliver in BEB, will only decrease performances
        //receiveAndDeliver();
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
                    System.out.println("Getting message in BEB error: " + e.toString());
                }
                for (Host h: hosts) {
                    if (id == h.getId()) {
                        myMessage.add(message); //TODO: check on delivering to me
                        continue;
                    }
                    try {
                        Packet p = new Packet(message, InetAddress.getByName(h.getIp()), h.getPort(), h.getId());
                        messageToSendDown.put(p);
                    } catch (UnknownHostException | InterruptedException e) {
                        System.out.println("Error sending down in BEB " + e.toString());
                    }
                }
            }
        }
    }

    public void broadcast() {
        new Broadcast().start();
    }

    private class Receive extends Thread {

        @Override
        public void run() {
            while(true) {
                String message = null;
                try {
                    message = messageDeliveredDown.take();
                } catch (InterruptedException e) {
                    System.out.println("Error receiving from down in BEB " + e.toString());
                }
                try {
                    messageToDeliverUp.put(message);
                } catch (InterruptedException e) {
                    System.out.println("Error sending up in BEB " + e.toString());
                }
            }
        }

    }

    public void receiveAndDeliver() {
        new Receive().start();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BestEffortBroadcast that = (BestEffortBroadcast) o;
        return id == that.id &&
                hosts.equals(that.hosts) &&
                pf.equals(that.pf) &&
                myMessage.equals(that.myMessage) &&
                messageToSendDown.equals(that.messageToSendDown) &&
                messageToSendUp.equals(that.messageToSendUp) &&
                messageDeliveredDown.equals(that.messageDeliveredDown) &&
                messageToDeliverUp.equals(that.messageToDeliverUp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hosts, id, pf, myMessage,
                messageToSendDown, messageToSendUp, messageDeliveredDown, messageToDeliverUp);
    }
}
