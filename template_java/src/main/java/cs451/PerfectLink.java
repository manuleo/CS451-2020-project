package cs451;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;

public class PerfectLink {
    private int pid;
    private InetAddress destIp;
    private int destPort;
    private int myPort;
    private String message;
    private static HashSet<String> recMessage = new HashSet<>();
    private static HashSet<String> sentMessage = new HashSet<>();

    public PerfectLink(int pid, InetAddress destIp, int destPort, int myPort, String message) {
        this.pid = pid;
        this.destIp = destIp;
        this.destPort = destPort;
        this.myPort = myPort;
        this.message = message;
    }

    public void send() throws IOException {

        // Set up sending
        DatagramSocket dsSend = new DatagramSocket();
        String sendString = String.format("%d %s", pid, message);
        byte[] sendBuf = sendString.getBytes();
        boolean ackRec = false;

        // Set up receiving
        DatagramSocket dsRec = new DatagramSocket(myPort);
        byte[] recBuf = new byte[128];
        DatagramPacket dpRec;

        while (!ackRec) {
            // Send data
            DatagramPacket dpSend =
                    new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
            dsSend.send(dpSend);
            sentMessage.add(sendString);

            // Check ack received
            dpRec = new DatagramPacket(recBuf, recBuf.length);
            dsRec.receive(dpRec);
            String sRec = new String(recBuf, StandardCharsets.UTF_8);
            if (sRec.contains(sendString))
                ackRec = true;
            else
                recBuf = new byte[128];
        }
        dsSend.close();
        dsRec.close();
    }

    public void receive() throws IOException {
        // Set up sending
        DatagramSocket dsSend = new DatagramSocket();
        String sendString;
        byte[] sendBuf;

        // Set up receiving
        DatagramSocket dsRec = new DatagramSocket(myPort);
        byte[] recBuf = new byte[128];
        DatagramPacket dpRec;

        while (true) {
            dpRec = new DatagramPacket(recBuf, recBuf.length);
            dsRec.receive(dpRec);
            String sRec = new String(recBuf, StandardCharsets.UTF_8);
            recMessage.add(sRec);
            sendString = String.format("ACK %s", sRec);
            sendBuf = sendString.getBytes();
            DatagramPacket dpSend =
                    new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
            dsSend.send(dpSend);
        }

    }

    public static HashSet<String> getRecMessage () {
        return recMessage;
    }

    public static HashSet<String> getSentMessage() {
        return sentMessage;
    }
}
