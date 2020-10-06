package cs451;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class BestEffortBroadcast {
    private List<Host> hosts;
    private int id;
    private PerfectLink pf;
    private HashSet<String> myMessage = new HashSet<>();

    public BestEffortBroadcast(List<Host> hosts, int id) {
        this.hosts = hosts;
        this.id = id;
        this.pf = new PerfectLink(id, hosts.get(id-1).getPort(), hosts.size());
    }

    private class Broadcast extends Thread{
        private final String message;
        public Broadcast(String message) {
            this.message = message;
        }

        @Override
        public void run() {
            for (Host h: hosts) {
                if (id == h.getId()) {
                    myMessage.add(message); //TODO: check on delivering to me
                    continue;
                }
                try {
                    pf.send(message, InetAddress.getByName(h.getIp()), h.getPort(), h.getId());
                } catch (UnknownHostException e) {
                    System.out.println("Unknown host in BestEffortBroadcast " + e.toString());
                }
            }
        }
    }

    public void broadcast(String message) {
        new Broadcast(message).start();
    }

    private class Receive extends Thread {

        private String gotPack;

        @Override
        public void run() {
            gotPack = pf.receiveAndDeliver();
        }

        public String getGotPack() {
            return gotPack;
        }
    }

    public String receiveAndDeliver() {
        Receive rec = new Receive();
        rec.start();
        try {
            //System.out.println("I'm joining in BEB deliver");
            rec.join();
        } catch (InterruptedException e) {
            System.out.println("Exception when joining to receive in BestEffortBroadcast " + e.toString());
        }
        //System.out.println("I have this packet in BEB " + rec.getGotPack());
        //System.out.println("I'm returning in BEB deliver");
        return deliver(rec.getGotPack());
    }

    private String deliver(String message) {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BestEffortBroadcast that = (BestEffortBroadcast) o;
        return id == that.id &&
                pf.equals(that.pf);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, pf);
    }
}
