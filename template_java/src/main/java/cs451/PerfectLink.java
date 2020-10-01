package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class PerfectLink {
    private final int id;
    private final int myPort;
    private DatagramSocket dsSend;
    private DatagramSocket dsRec;
    private static final HashSet<String> recMessage = new HashSet<>();
    private static final HashSet<String> sentMessage = new HashSet<>();
    private static final HashSet<String> recACKs = new HashSet<>();

    public PerfectLink(int id, int myPort) {
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

    }

    private class Send extends Thread {

        private final int message;
        private final InetAddress destIp;
        private final int destPort;

        public Send(int message, InetAddress destIp, int destPort) {
            this.message = message;
            this.destIp = destIp;
            this.destPort = destPort;
        }

        @Override
        public void run() {
            String sendString = String.format("%d %d", id, message);
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
                String sRec = new String(recBuf, StandardCharsets.UTF_8);
                //System.out.println("Received " + sRec + " in send");
                if (sRec.contains(sendString) || recACKs.contains(sendString)) {
                    ackRec = true;
                    recACKs.add(sendString);
                }
                else
                    recBuf = new byte[1024];
            }
        }
    }

    public void send(int message, InetAddress destIp, int destPort) {
        new Send(message, destIp, destPort).start();
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
                String sRec = new String(recBuf, StandardCharsets.UTF_8);
                if (!sRec.contains("ACK")) {
                    //System.out.println("Received " + sRec + " in receive");
                    InetAddress destIp = dpRec.getAddress();
                    int destPort = dpRec.getPort();
                    if (!recMessage.contains(sRec)) {
                        recMessage.add(sRec);
                        gotPack = sRec;
                    }
                    sendString = String.format("ACK %s", sRec);
                    sendBuf = sendString.getBytes();
                    DatagramPacket dpSend =
                            new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
                    sendOnSocket(dpSend);
                    //System.out.println("Sent " + sendString + " in receive");
                    break;
                }
                else{
                    String ackedPack = sRec.replace("ACK ", "");
                    //System.out.println("Acked pack in receive " + ackedPack);
                    recACKs.add(ackedPack);
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
            System.out.println("I'm joining in PF receive");
            rec.join();
        } catch (InterruptedException e) {
            System.out.println("Exception when joining to receive " + e.toString());
        }
        System.out.println("I have this packet in PF " + rec.getGotPack());
        System.out.println("I'm returning in PF receive");
        return rec.getGotPack();
    }

    private synchronized void sendOnSocket(DatagramPacket dpSend) {
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
            System.out.println("Timeout");
            return false;
        } catch (IOException e) {
            System.out.println("Receiving error: " + e.toString());
        }
        return true;
    }

    public static HashSet<String> getRecMessage () {
        return recMessage;
    }

    public static HashSet<String> getSentMessage() {
        return sentMessage;
    }
}
