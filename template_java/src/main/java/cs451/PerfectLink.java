package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class PerfectLink {
    private final int id;
    private final int myPort;
    private DatagramSocket dsSend;
    private DatagramSocket dsRec;
    private HashMap<Integer, Integer> portMap;
    private static final HashSet<String> recMessage = new HashSet<>();
    private static final HashSet<String> sentMessage = new HashSet<>();
    private static final HashMap<Integer, HashSet<String>> recACKs = new HashMap<>();
    private static ArrayList<String> deliverMessages = new ArrayList<>();
    private static ArrayList<String> gotSend = new ArrayList<>();
    private static ArrayList<String> gotRec = new ArrayList<>();
    private static final Object lock = new Object();


    public PerfectLink(int id, int myPort, List<Host> hosts) {
        this.id = id;
        this.myPort = myPort;
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
    }

    private class Send extends Thread {

        private final String message;
        private final InetAddress destIp;
        private final int destPort;
        private final int destId;

        public Send(String message, InetAddress destIp, int destPort, int destId) {
            this.message = message;
            this.destIp = destIp;
            this.destPort = destPort;
            this.destId = destId;
        }

        @Override
        public void run() {
            //String sendString = String.format("%d %d", id, message);
            String sendString = message;
            byte[] sendBuf = sendString.getBytes();
            boolean ackRec = false;
            byte[] recBuf = new byte[1024];
            DatagramPacket dpRec;

            while (!ackRec) {
                // Check at the beginning of a new cycle to avoid sending extra packets
                if (recACKs.getOrDefault(destId, new HashSet<>()).contains(sendString)) {
                    ackRec = true;
                    continue;
                }
                // Send data
                DatagramPacket dpSend =
                        new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
                sendOnSocket(dpSend);
                sentMessage.add(sendString);
                //System.out.println("Sent " + sendString + " in send");

                // Check ack received
                dpRec = new DatagramPacket(recBuf, recBuf.length);
                if (!recOnSocket(dpRec)) {
                    continue;
                }
                String sRec = new String(trim(recBuf), StandardCharsets.UTF_8);
                //System.out.println("Received " + sRec + " in send");
                if (recACKs.getOrDefault(destId, new HashSet<>()).contains(sendString)) {
                    ackRec = true;
                }
                else if (sRec.equals(String.format("ACK %d:%s", destId, sendString))) {
                    ackRec = true;
                    updateACKs(destId, sendString);
                }
                else if (sRec.contains("ACK")) {
                    String[] ackedPack = sRec.split(":");
                    int pId = Integer.parseInt(ackedPack[0].replace("ACK ", ""));
                    updateACKs(pId, ackedPack[1]);
                }
                else if (!recMessage.contains(sRec)){
                    synchronized (lock) {
                        deliverMessages.add(sRec);
                    }
                    gotSend.add(sRec);
                    recMessage.add(sRec);
                    sendACK(dpRec, sRec);
                    try {
                        Thread.sleep(100); // Sleep for a bit instead of sending immediately, the ACK may arrive
                    } catch (InterruptedException e) {
                        System.out.println("Sender sleep error: " + e.toString());
                    }
                }
                recBuf = new byte[1024];
            }
        }
    }

    public void send(String message, InetAddress destIp, int destPort, int destId) {
        new Send(message, destIp, destPort, destId).start();
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

    private synchronized void updateACKs(int pid, String ackPack) {
        HashSet<String> acks = recACKs.getOrDefault(pid, new HashSet<>());
        acks.add(ackPack);
        recACKs.put(pid, acks);
    }

    private class Receive extends Thread {

        private final List<String> gotPacks = new ArrayList<>();

        @Override
        public void run() {
            byte[] recBuf = new byte[1024];
            DatagramPacket dpRec;

            while (true) {
                if (deliverMessages.size() > 0) {  // We have messages to deliver
                    //System.out.println(deliverMessages);
                    synchronized (lock) {
                        gotPacks.addAll(deliverMessages);
                        deliverMessages = new ArrayList<>();
                    }
                    break;
                }
                dpRec = new DatagramPacket(recBuf, recBuf.length);
                if (!recOnSocket(dpRec)) {
                    continue;
                }
                String sRec = new String(trim(recBuf), StandardCharsets.UTF_8);
                if (!sRec.contains("ACK")) {
                    //System.out.println("Received " + sRec + " in receive");
                    //System.out.println("Sent " + sendString + " in receive");
                    if (!recMessage.contains(sRec)) {
                        gotRec.add(sRec);
                        deliverMessages.add(sRec);
                        recMessage.add(sRec);
                    }
                    sendACK(dpRec, sRec);
                }
                else{
                    String[] ackedPack = sRec.split(":");
                    int pId = Integer.parseInt(ackedPack[0].replace("ACK ", ""));
                    updateACKs(pId, ackedPack[1]);
                }
                recBuf = new byte[1024];
            }
        }

        public List<String> getGotPacks() {
            return gotPacks;
        }
    }

    public List<String> receiveAndDeliver(){
        Receive rec = new Receive();
        rec.start();
        try {
            //System.out.println("I'm joining in PF receive");
            rec.join();
        } catch (InterruptedException e) {
            System.out.println("Exception when joining to receive " + e.toString());
        }
        //System.out.println("I have this packet in PF " + rec.getGotPack());
        //System.out.println("I'm returning in PF receive");
        return rec.getGotPacks();
    }

    private void sendOnSocket(DatagramPacket dpSend) {
        try {
            dsSend.send(dpSend);
        } catch (IOException e) {
            System.out.println("Sending ACK error: " + e.toString());
        }
    }

    private synchronized boolean recOnSocket(DatagramPacket dpRec) {
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

    public static HashMap<Integer, HashSet<String>> getRecACKs() {
        return recACKs;
    }

    public static ArrayList<String> getGotRec() {
        return gotRec;
    }

    public static ArrayList<String> getGotSend() {
        return gotSend;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PerfectLink that = (PerfectLink) o;
        return id == that.id &&
                myPort == that.myPort &&
                Objects.equals(dsSend, that.dsSend) &&
                Objects.equals(dsRec, that.dsRec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, myPort, dsSend, dsRec);
    }
}
