package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;

public class PerfectLink {
    private final int id;
    private final int myPort;
    private DatagramSocket dsSend;
    private DatagramSocket dsRec;
    private static final HashSet<String> recMessage = new HashSet<>();
    private static final HashSet<String> sentMessage = new HashSet<>();
    private static HashMap<Integer, HashSet<String>> recACKs = new HashMap<>();
    //private static final HashSet<String> recACKs = new HashSet<>();

    public PerfectLink(int id, int myPort, int numHosts) {
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
        for (int p = 1; p <= numHosts; p++) {
            if (p!=this.id)
                recACKs.put(p, new HashSet<>());
        }
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
                if (recACKs.get(destId).contains(sendString)) {
                    ackRec = true;
                }
                else if (sRec.equals(String.format("ACK %d %s", destId, sendString))) {
                    ackRec = true;
                    recACKs.get(destId).add(sendString);
                }
                else if (sRec.contains(sendString) && sRec.contains("ACK")) {
                    String[] ackedPack = sRec.split(" ");
                    Integer pId = Integer.valueOf(ackedPack[1]);
                    recACKs.get(pId).add(ackedPack[2]);
                }
                else //TODO: add functionality to avoid losing new received packet
                    recBuf = new byte[1024];
            }
        }
    }

    public void send(String message, InetAddress destIp, int destPort, int destId) {
        new Send(message, destIp, destPort, destId).start();
    }

    private class Receive extends Thread {

        private String gotPack;

        @Override
        public void run() {
            String sendString;
            byte[] sendBuf;
            byte[] recBuf = new byte[1024];
            DatagramPacket dpRec;

            while (true) {
                dpRec = new DatagramPacket(recBuf, recBuf.length);
                if (!recOnSocket(dpRec)) {
                    continue;
                }
                String sRec = new String(trim(recBuf), StandardCharsets.UTF_8);
                if (!sRec.contains("ACK")) {
                    //System.out.println("Received " + sRec + " in receive");
                    InetAddress destIp = dpRec.getAddress();
                    int destPort = dpRec.getPort();
                    sendString = String.format("ACK %d %s", id, sRec);
                    sendBuf = sendString.getBytes();
                    DatagramPacket dpSend =
                            new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
                    sendOnSocket(dpSend);
                    //System.out.println("Sent " + sendString + " in receive");
                    if (!recMessage.contains(sRec)) {
                        recMessage.add(sRec);
                        gotPack = sRec;
                        break;
                    }
                }
                else{
                    String[] ackedPack = sRec.split(" ");
                    Integer pId = Integer.valueOf(ackedPack[1]);
                    //System.out.println("Acked pack in receive " + ackedPack);
                    recACKs.get(pId).add(ackedPack[2]);
                }
                recBuf = new byte[1024];
            }
        }

        public String getGotPack() {
            return gotPack;
        }
    }

    public String receiveAndDeliver(){
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
        return rec.getGotPack();
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
