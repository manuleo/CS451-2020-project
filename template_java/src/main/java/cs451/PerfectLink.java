package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


public class PerfectLink {
    private final int id;
    private final int myPort;
    private DatagramSocket dsSend;
    private DatagramSocket dsRec;
    private HashMap<Integer, Integer> portMap;
    private LinkedBlockingQueue<Packet> messageToSend;
    private LinkedBlockingQueue<String> messageToDeliver;
    private LinkedBlockingQueue<Packet> recACKs = new LinkedBlockingQueue<>();
    private int numOutstanding;
    private int outLimit = 500;
    private static final HashSet<String> recMessage = new HashSet<>();
    private static final HashSet<String> sentMessage = new HashSet<>();
    private static final HashMap<Packet, Long> toRecACK = new HashMap<>();
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
        } catch (SocketException e) {
            System.out.println("Creating socket to receive ACK in send error: " + e.toString());
        }
        assert this.dsRec != null;
        portMap = new HashMap<>();
        for (Host h: hosts)
            portMap.put(h.getId(), h.getPort());
        this.numOutstanding = 0;
        receiveAndDeliver();
        startAckCheck();
        send();
    }

    private class Send extends Thread {

        @Override
        public void run() {
            boolean recMine = false;
            while(true) {
                ArrayList<Packet> pToSend = new ArrayList<>();
                Packet p1 = null;
                try {
                    p1 = messageToSend.take();
                } catch (InterruptedException e) {
                    System.out.println("Getting packet in PF link error: " + e.toString());
                }
                assert (p1 != null);
                pToSend.add(p1);
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    System.out.println("Sleeping to create batch in PF link error: " + e.toString());
                }
                messageToSend.drainTo(pToSend);
                for (Packet p: pToSend) {
                    int tries = 0;
                    while (numOutstanding >= outLimit) {
                        tries+=1;
                        if (tries==5) {
                            synchronized (lock) {
                                outLimit*=2;
                            }
                           // System.out.println(outLimit);
                        }
                        //System.out.println("Busy waiting");
                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            System.out.println("Sleeping for outstanding in PF link error: " + e.toString());
                        }
                    }
                    String sendString = p.getMessage();
                    InetAddress destIp = p.getDestIp();
                    int destPort = p.getDestPort();
                    byte[] sendBuf = sendString.getBytes();
                    // Send data
                    //System.out.println("Sending " + sendString + " to " + p.getDestId() + " in send");
                    synchronized (lock) {
                        toRecACK.put(p, System.nanoTime());
                        numOutstanding++;
                        //System.out.println(numOutstanding);
                    }
                    DatagramPacket dpSend =
                            new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
                    sendOnSocket(dpSend);
                    sentMessage.add(sendString);
                    if (!recMine && sentMessage.size()>=Main.m) {
                        if (sentMessage.containsAll(Main.broadcasted)) {
                            System.out.println("Signaling end of broadcasting messages");
                            Main.coordinator.finishedBroadcasting();
                            recMine = true;
                        }
                    }
                    //TODO: try to add some sort of "flow control" to avoid this thread to overcome the others
                }
            }
        }
    }

    public void send() {
        Send s = new Send();
        s.start();
    }

    private class ACKChecker extends Thread {
        Long timeout = 10L*((long) Math.pow(10, 9));
        @Override
        public void run() {
            while (true) {
                Packet recAck = null;
                try {
                    recAck = recACKs.poll(timeout, TimeUnit.NANOSECONDS);
                } catch (InterruptedException ignored) {
                }
                if (recAck!=null) {
                    List<Packet> newAcks = new LinkedList<>();
                    newAcks.add(recAck);
                    recACKs.drainTo(newAcks);
                    synchronized (lock) {
                        //toRecACK.remove(recAck);
                        numOutstanding-=newAcks.size();
                        if (numOutstanding==outLimit/2)
                            outLimit/=2;
                        toRecACK.keySet().removeAll(newAcks);
                    }
                }
                LinkedList<Packet> toAck;
                Long now = System.nanoTime();
                synchronized (lock) {
                    toAck =
                        toRecACK.entrySet().stream()
                        .filter(x -> now - x.getValue() >= timeout)
                        .map(Map.Entry::getKey).collect(Collectors.toCollection(LinkedList::new));
                }
                messageToSend.addAll(toAck);
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
                recOnSocket(dpRec);
                String sRec = new String(trim(recBuf), StandardCharsets.UTF_8);
                if (!sRec.contains("ACK")) {
                    //System.out.println("Received " + sRec + " in receive");
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
                    //System.out.println("Received " + sRec + " in receive");
                    String[] ackedPack = sRec.split(":");
                    int pid = Integer.parseInt(ackedPack[0].replace("ACK ", ""));
                    InetAddress address = dpRec.getAddress();
                    int port = portMap.get(pid);
                    Packet p = new Packet(ackedPack[1], address, port, pid);
                    try {
                        recACKs.put(p);
                    } catch (InterruptedException e) {
                        System.out.println("Exception trying to put an ACK in the queue " + e.toString());
                    }
                }
                recBuf = new byte[1024];
            }
        }
    }

    public void receiveAndDeliver(){
        Receive r = new Receive();
        r.start();
    }

    private void sendOnSocket(DatagramPacket dpSend) {
        try {
            dsSend.send(dpSend);
        } catch (IOException e) {
            System.out.println("Sending ACK error: " + e.toString());
        }
    }

    private void recOnSocket(DatagramPacket dpRec) {
        try {
            dsRec.receive(dpRec);
//        } catch (SocketTimeoutException ignored) {
//            //System.out.println("Timeout");
//            return false;
        } catch (IOException e) {
            System.out.println("Receiving error: " + e.toString());
        }
        //System.out.println("Received " + new String(trim(dpRec.getData()), StandardCharsets.UTF_8));
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

    public static HashMap<Packet, Long> getToRecACK() {
        return toRecACK;
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
