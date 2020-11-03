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
    private LinkedBlockingQueue<PacketTimeRnum> recACKs = new LinkedBlockingQueue<>();
    private int numOutstanding;
    private int outLimit = 1500;
    private List<Host> hosts;
    private ArrayList<HashMap<Packet, ArrayList<Long>>> toRecAckProcess;
    private static final HashSet<String> recMessage = new HashSet<>();
    private static final HashSet<String> sentMessage = new HashSet<>();
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
        this.hosts = hosts;
        portMap = new HashMap<>();
        toRecAckProcess = new ArrayList<>(hosts.size());
        for (Host h: hosts) {
            portMap.put(h.getId(), h.getPort());
            toRecAckProcess.add(h.getId()-1, new HashMap<>());
        }
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
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    System.out.println("Sleeping to create batch in PF link error: " + e.toString());
                }
                messageToSend.drainTo(pToSend);
                for (Packet p: pToSend) {
                    int tries = 0;
                    while (numOutstanding >= outLimit) {
                        tries+=1;
                        if (tries==2) {
                            synchronized (lock) {
                                outLimit*=2;
                            }
                           //System.out.println(outLimit);
                        }
                        //System.out.println("Busy waiting");
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            System.out.println("Sleeping for outstanding in PF link error: " + e.toString());
                        }
                    }
                    String sendString = p.getMessage();
                    InetAddress destIp = p.getDestIp();
                    int destPort = p.getDestPort();
                    String sendStringWithRet;
                    // Send data
                    //System.out.println("Sending " + sendString + " to " + p.getDestId() + " in send");
                    synchronized (lock) {
                        ArrayList<Long> retransmits =
                                toRecAckProcess.get(p.getDestId()-1).getOrDefault(p, new ArrayList<>());
                        sendStringWithRet = sendString + ",r" + retransmits.size();
                        retransmits.add(System.nanoTime());
                        toRecAckProcess.get(p.getDestId()-1).put(p, retransmits);
                        numOutstanding++;
                        //System.out.println(numOutstanding);
                    }
                    //System.out.println("Sending " + sendString + " to " + p.getDestId() + " in send");
                    byte[] sendBuf = sendStringWithRet.getBytes();
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
                    /*
                    TODO: try to add some sort of "flow control":
                     Need to add a "sliding window" for each process. How to add it?
                     1. Use numOutstanding by process and ThreadPool to send to each process.
                        Also, count the number of acks received for the window and tell FIFO when can send
                        the next batch (i.e. > n/2 ack all packets inside the batch)
                     2. At FIFO level, send packets only when acked all the packets inside the batch,
                        no limits at PL level...but URB packets will overcome...NOT DOABLE
                     3. Sliding window at PL level (like TCP)? Number all the packets from the queue at TCP level
                        and move the window when correctly received the ACKs (in order delivery? No).
                        A sliding window is helpless for the URB, whenever an ack arrive we can move it by one,
                        we don't need to receive all before BUT it's useful for FIFO: we don't send the 10th mess
                        if we didn't receive the ACK for 1..9 -> 2 windows? We can use the packet (it's build at URB,
                        we know which kind of message we are sending) to build such abstraction.
                        At PL we can build it too (depending on message size i.e. 2 or 3 fields)
                        Problems:
                                  1. what to do when URB add packets? May delay a lot the packets from FIFO.
                                  Hold both for 1 and 3...
                                  Maybe we can solve this problem by having the same "indication to continue"
                                  mechanism both for FIFO and URB, so that no one of the two class has too
                                  many outstanding packets
                                  2. Dead processes? Add all the packets we can't send in the queue in toRecAck
                                  (can happen? we limit both FIFO and URB...) so that we avoid flooding the queue again
                                  and the large timeout will avoid to send them again...
                     Summary: Need 2 sliding window at TCP level + indications to both FIFO and URB
                              (or do two different PL instances keeping different infos)
                     */
                }
            }
        }
    }

    public void send() {
        Send s = new Send();
        s.start();
    }

    private class ACKChecker extends Thread {
        boolean[] recFirst;
        Long timeout;
        Long[] RTTs;
        Long[] RTTd;
        Long[] RTO;
        Double alpha;
        Double beta;
        int[] recNoneCount;
        public ACKChecker() {
            recFirst = new boolean[hosts.size()];
            Arrays.fill(recFirst, false);
            timeout = 1000L*((long) Math.pow(10, 6));
            RTTs = new Long[hosts.size()];
            RTTd = new Long[hosts.size()];
            RTO = new Long[hosts.size()];
            recNoneCount = new int[hosts.size()];
            Arrays.fill(RTO, timeout);
            alpha = 1.0/8.0;
            beta = 1.0/4.0;
        }
        @Override
        public void run() {
            while (true) {
                PacketTimeRnum recAck = null;
                try {
                    recAck = recACKs.poll(timeout, TimeUnit.NANOSECONDS);
                } catch (InterruptedException ignored) {
                }
                boolean[] recSome = new boolean[hosts.size()];
                if (recAck!=null) {
                    List<PacketTimeRnum> newAcks = new LinkedList<>();
                    newAcks.add(recAck);
                    recACKs.drainTo(newAcks);
                    Arrays.fill(recSome, false);
                    //System.out.println("Received ACKs: " + newAcks);
                    for (PacketTimeRnum pt: newAcks) {
                        //System.out.println("Processing " + pt);
                        //System.out.println("toReckAckProcess: " + toRecAckProcess);
                        int pid = pt.getPacket().getDestId();
                        recSome[pid-1] = true;
                        recNoneCount[pid-1] = 0;
                        if (!toRecAckProcess.get(pid-1).containsKey(pt.getPacket()) ||
                                pt.getrNum() >= toRecAckProcess.get(pid-1).get(pt.getPacket()).size())
                            continue;
                        long RTTm;
                        synchronized (lock) {
                            HashMap<Packet, ArrayList<Long>> toRecACK = toRecAckProcess.get(pid-1);
                            RTTm = pt.getTimeRec() - toRecACK.get(pt.getPacket()).get(pt.getrNum());
                            //System.out.println("RTT: " + RTTm/Math.pow(10, 6) + " ms");
//                            if (RTTm <= 0) {
//                                System.out.println("Map: " + toRecAckProcess);
//                                System.out.println("Sended at: " + toRecAckProcess.get(pid-1).get(pt.getPacket()));
//                                System.out.println("Received at: " + pt.getTimeRec());
//                            }
                            toRecACK.remove(pt.getPacket());
                            toRecAckProcess.set(pid-1, toRecACK);
                            numOutstanding-=1;
                            if (numOutstanding==outLimit/2)
                                outLimit/=2;
                        }
                        if (!recFirst[pid-1]) {
                            RTTs[pid-1] = RTTm;
                            RTTd[pid-1] = RTTm/2;
                            recFirst[pid-1] = true;
                        }
                        else {
                            RTTs[pid-1] = new Double((1-alpha)*RTTs[pid-1] + alpha*RTTm).longValue();
                            RTTd[pid-1] = new Double((1-beta)*RTTd[pid-1]
                                                            + beta * Math.abs(RTTm - RTTs[pid-1])).longValue();
                        }
                        RTO[pid-1] = RTTs[pid-1] + 4*RTTd[pid-1];
                    }
                }
                LinkedList<Packet> toAck;
                Long now = System.nanoTime();
                for (int i = 0; i<hosts.size(); i++) {
                    if(!recSome[i]) {
                        recNoneCount[i] += 1;
                        if (recNoneCount[i] >= hosts.size()) {
                            recNoneCount[i] = 0;
                            // TODO: try to remove this limit and see what happens
                            RTO[i] = Math.min(RTO[i]*2, 60L*((long) Math.pow(10, 9)));
                        }
                    }
                }
                timeout = Collections.min(Arrays.asList(RTO));
//                System.out.println("New RTOs:");
//                for (int i = 0; i<hosts.size(); i++) {
//                    int pid = i+1;
//                    System.out.println("PID: " + pid + " RTO: " + RTO[i]/Math.pow(10, 6) + " ms");
//                }
//                System.out.println("New lower bound timeout: " + timeout/Math.pow(10, 6) + " ms");
                synchronized (lock) {
                    toAck = new LinkedList<>();
                    for (int pid = 1; pid<=hosts.size(); pid++) {
                        if (pid==id)
                            continue;
                        int finalPid = pid;
                        LinkedList<Packet> toSendPid =
                                toRecAckProcess.get(pid-1).entrySet().stream()
                                .filter(x -> now - x.getValue().get(x.getValue().size()-1) >= RTO[finalPid -1])
                                .map(Map.Entry::getKey)
                                .filter(p -> !messageToSend.contains(p))
                                .collect(Collectors.toCollection(LinkedList::new));
                        toAck.addAll(toSendPid);
                    }
                }
                if (!toAck.isEmpty())
                    messageToSend.addAll(toAck);
            }
        }
    }

    public void startAckCheck() {
        new ACKChecker().start();
    }

    private void sendACK(DatagramPacket dpRec, String sRec, String rNum) {
        String sendString;
        byte[] sendBuf;
        InetAddress destIp = dpRec.getAddress();
        int pid = Integer.parseInt(sRec.split(" ")[0]);
        int destPort = portMap.get(pid);
        sendString = String.format("ACK %d %s:%s", id, rNum, sRec);
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
                Long now = System.nanoTime();
                String sRec = new String(trim(recBuf), StandardCharsets.UTF_8);
                if (!sRec.contains("ACK")) {
                    String[] messAndResend = sRec.split(",");
                    //System.out.println("Received " + Arrays.asList(messAndResend) + " in receive no ACK");
                    //System.out.println("Received " + sRec + " in receive");
                    if (!recMessage.contains(messAndResend[0])) {
                        recMessage.add(messAndResend[0]);
                        try {
                            messageToDeliver.put(messAndResend[0]);
                        } catch (InterruptedException e) {
                            System.out.println("Exception trying to deliver a message in PF " + e.toString());
                        }
                    }
                    sendACK(dpRec, messAndResend[0], messAndResend[1]);
                }
                else{
                    //System.out.println("Received " + sRec + " in receive with ACK");
                    String[] ackedPack = sRec.split(":");
                    String[] packInfo = ackedPack[0].split(" ");
                    int pid = Integer.parseInt(packInfo[1]);
                    int rNum = Integer.parseInt(packInfo[2].replace("r", ""));
                    InetAddress address = dpRec.getAddress();
                    int port = portMap.get(pid);
                    Packet p = new Packet(ackedPack[1], address, port, pid);
                    //System.out.println("pid: " + pid + " rNum: " + rNum);
                    try {
                        recACKs.put(new PacketTimeRnum(p, now, rNum));
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
