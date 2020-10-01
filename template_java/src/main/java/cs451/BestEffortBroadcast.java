package cs451;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class BestEffortBroadcast {
    private List<Host> hosts;
    private int id;
    private PerfectLink pf;

    public BestEffortBroadcast(List<Host> hosts, int id) {
        this.hosts = hosts;
        this.id = id;
        this.pf = new PerfectLink(id, hosts.get(id-1).getPort());
    }

    private class Broadcast extends Thread{
        private final int message;
        public Broadcast(int message) {
            this.message = message;
        }

        @Override
        public void run() {
            for (Host h: hosts) {
                if (id == h.getId())
                    continue;
                try {
                    pf.send(message, InetAddress.getByName(h.getIp()), h.getPort());
                } catch (UnknownHostException e) {
                    System.out.println("Unknown host in BestEffortBroadcast " + e.toString());
                }
            }
        }
    }

    public void broadcast(int message) {
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

    public String deliver() {
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
        return rec.getGotPack();
    }

}
