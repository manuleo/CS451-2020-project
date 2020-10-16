package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

public class PerfectLink {
    private final int id;
    private final int myPort;
    private DatagramSocket dsSend;
    private DatagramSocket dsRec;
    private HashMap<Integer, Integer> portMap;
    private LinkedBlockingQueue<Packet> messageToSend;
    private LinkedBlockingQueue<String> messageToDeliver;
    private static final HashSet<String> recMessage = new HashSet<>();
    private static final HashSet<String> sentMessage = new HashSet<>();
    private static final HashMap<Packet, Boolean> recACKs = new HashMap<>();
//    private static ArrayList<String> deliverMessages = new ArrayList<>();
//    private static ArrayList<String> gotSend = new ArrayList<>();
//    private static ArrayList<String> gotRec = new ArrayList<>();
    private static final Object lock = new Object();


    public PerfectLink(int id, int myPort, List<Host> hosts,
                       LinkedBlockingQueue<Packet> messageToSend, LinkedBlockingQueue<String> messageToDeliver) {
        this.id = id;
        this.myPort = myPort;
        this.messageToSend = messageToSend;
        this.messageToDeliver = messageToDeliver;
        // Set up sending
        try {
            this.dsSend = new DatagramSocket();
        } catch (SocketException e) {
            System.out.println("Creating socket to send in send error: " + e.toString());
        }
        assert this.dsSend != null;

        // Set up receiving
        try {
            this.dsRec = new DatagramSocket(this.myPort);
            this.dsRec.setSoTimeout(100);
        } catch (SocketException e) {
            System.out.println("Creating socket to receive ACK in send error: " + e.toString());
        }
        assert this.dsRec != null;
//        for (int p = 1; p <= numHosts; p++) {
//            if (p!=this.id)
//                recACKs.put(p, new HashSet<>());
//        }
        portMap = new HashMap<>();
        for (Host h: hosts)
            portMap.put(h.getId(), h.getPort());

        receiveAndDeliver();
        startAckCheck();
        send();
    }

    private class Send extends Thread {

        int numSend = 0;

        @Override
        public void run() {
            while(true) {
                Packet p = null;
                try {
                    p = messageToSend.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting packet in PF link error: " + e.toString());
                }
                assert p != null;
                String sendString = p.getMessage();
                InetAddress destIp = p.getDestIp();
                int destPort = p.getDestPort();
                byte[] sendBuf = sendString.getBytes();

                // Check at the beginning of a new cycle to avoid sending extra packets
                synchronized (lock) {
                    if (recACKs.getOrDefault(p, false)) {
                        continue;
                    }
                }

                // Send data
                System.out.println("Sending " + sendString + " to " + p.getDestId() + " in send");
                synchronized (lock) {
                    recACKs.put(p, false);
                }
                DatagramPacket dpSend =
                        new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
                sendOnSocket(dpSend);
                sentMessage.add(sendString);
//                numSend+=1;
//                if (numSend >= 800) {
//                    try {
//                        Thread.sleep(100);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
                //TODO: try to add some sort of "flow control" to avoid this thread to overcome the others
            }
        }
    }

    public void send() {
        Send s = new Send();
        s.setPriority(2);
        s.start();
    }

    private class ACKChecker extends Thread {
        @Override
        public void run() {
            while (true) {
                LinkedList<Packet> toAck;
                synchronized (lock) {
                    toAck =
                        recACKs.entrySet().stream()
                        .filter(x -> !x.getValue())
                        .map(Map.Entry::getKey).collect(Collectors.toCollection(LinkedList::new));
                }
                for (Packet p: toAck) {
                    try {
                        messageToSend.put(p);
                    } catch (InterruptedException e) {
                        System.out.println("Exception trying to put not acked in PF " + e.toString());
                    }
                }
                try {
                    Thread.sleep(1000); //TODO: see if we can improve performance of busy waiting o change it to wait on queue
                } catch (InterruptedException e) {
                    System.out.println("Exception trying to sleep for ACK check in PF " + e.toString());
                }
            }
        }
    }

    public void startAckCheck() {
        new ACKChecker().start();
    }

    private void sendACK(DatagramPacket dpRec, String sRec) {
        String sendString;
        byte[] sendBuf;
        InetAddress destIp = dpRec.getAddress();
        int pid = Integer.parseInt(sRec.split(" ")[0]);
        int destPort = portMap.get(pid);
        sendString = String.format("ACK %d:%s", id, sRec);
        sendBuf = sendString.getBytes();
        DatagramPacket dpSend =
                new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
        sendOnSocket(dpSend);
    }

    private class Receive extends Thread {

        @Override
        public void run() {
            byte[] recBuf = new byte[1024];
            DatagramPacket dpRec;

            while (true) {
                dpRec = new DatagramPacket(recBuf, recBuf.length);
                if (!recOnSocket(dpRec)) {
                    continue;
                }
                String sRec = new String(trim(recBuf), StandardCharsets.UTF_8);
                if (!sRec.contains("ACK")) {
                    System.out.println("Received " + sRec + " in receive");
                    if (!recMessage.contains(sRec)) {
                        recMessage.add(sRec);
                        try {
                            messageToDeliver.put(sRec);
                        } catch (InterruptedException e) {
                            System.out.println("Exception trying to deliver a message in PF " + e.toString());
                        }
                    }
                    sendACK(dpRec, sRec);
                }
                else{
                    System.out.println("Received " + sRec + " in receive");
                    String[] ackedPack = sRec.split(":");
                    int pid = Integer.parseInt(ackedPack[0].replace("ACK ", ""));
                    InetAddress address = dpRec.getAddress();
                    int port = portMap.get(pid);
                    Packet p = new Packet(ackedPack[1], address, port, pid);
                    synchronized (lock) {
                        recACKs.put(p, true);
                    }
                }
                recBuf = new byte[1024];
            }
        }
    }

    public void receiveAndDeliver(){
        Receive r = new Receive();
        r.setPriority(10);
        r.start();
    }

    private void sendOnSocket(DatagramPacket dpSend) {
        try {
            dsSend.send(dpSend);
        } catch (IOException e) {
            System.out.println("Sending ACK error: " + e.toString());
        }
    }

    private boolean recOnSocket(DatagramPacket dpRec) {
        try {
            dsRec.receive(dpRec);
        } catch (SocketTimeoutException ignored) {
            //System.out.println("Timeout");
            return false;
        } catch (IOException e) {
            System.out.println("Receiving error: " + e.toString());
        }
        //System.out.println("Received " + new String(trim(dpRec.getData()), StandardCharsets.UTF_8));
        return true;
    }

    /**
     * Found on https://stackoverflow.com/a/17004488
     * @param bytes
     * @return
     */
    private static byte[] trim(byte[] bytes)
    {
        int i = bytes.length - 1;
        while (i >= 0 && bytes[i] == 0)
            --i;
        return Arrays.copyOf(bytes, i + 1);
    }

    public static HashSet<String> getRecMessage () {
        return recMessage;
    }

    public static HashSet<String> getSentMessage() {
        return sentMessage;
    }

    public static HashMap<Packet, Boolean> getRecACKs() {
        return recACKs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PerfectLink that = (PerfectLink) o;
        return id == that.id &&
                myPort == that.myPort &&
                dsSend.equals(that.dsSend) &&
                dsRec.equals(that.dsRec) &&
                portMap.equals(that.portMap) &&
                messageToSend.equals(that.messageToSend) &&
                messageToDeliver.equals(that.messageToDeliver);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, myPort, dsSend, dsRec, portMap, messageToSend, messageToDeliver);
    }
}
