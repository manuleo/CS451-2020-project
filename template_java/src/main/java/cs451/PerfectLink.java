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
    private ArrayList<Packet> packetToSendFIFO = new ArrayList<>();
    private ArrayList<Packet> packetToSendURB = new ArrayList<>();
    private int windowSize = Constants.WINDOW_SIZE;
    private int URBWindow;
    private HashMap<Integer, Window> windowProcess = new HashMap<>();
    private HashMap<Integer, Integer> outStandingURBProcesses = new HashMap<>();
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
            windowProcess.put(h.getId(), new Window(windowSize));
            outStandingURBProcesses.put(h.getId(), 0);
        }
        URBWindow = windowSize*hosts.size()*hosts.size();
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
                    p1 = messageToSend.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {}
                if (p1!=null) {
                    pToSend.add(p1);
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        System.out.println("Sleeping to create batch in PF link error: " + e.toString());
                    }
                    messageToSend.drainTo(pToSend);
                    pToSend.forEach(p -> {
                        if (p.getType() == Packet.packType.FIFO) {
                            if (!packetToSendFIFO.contains(p))
                                packetToSendFIFO.add(p);
                        } else {
                            if (!packetToSendURB.contains(p))
                                packetToSendURB.add(p);
                        }
                    });
                }
                System.out.println("Packet to send FIFO: " + packetToSendFIFO.size());
                System.out.println("FIFO window: " + windowProcess);
                for(Iterator<Packet> itFIFO = packetToSendFIFO.iterator(); itFIFO.hasNext();) {
                    Packet pFIFO = itFIFO.next();
                    //System.out.println("Processing: " + pFIFO);
                    //System.out.println("Window: " + windowProcess.get(pFIFO.getDestId()));
                    if (!windowProcess.get(pFIFO.getDestId()).canSend(pFIFO))
                        continue;
                    sendPacket(pFIFO);
                    itFIFO.remove();
                    //System.out.println("Removed. New list: " + packetToSendFIFO);
                }
                System.out.println("Packet to send URB: " + packetToSendURB.size());
                System.out.println("URB window: " + outStandingURBProcesses);
                for(Iterator<Packet> itURB = packetToSendURB.iterator(); itURB.hasNext();) {
                    Packet pURB = itURB.next();
                    if(outStandingURBProcesses.get(pURB.getDestId()) >= URBWindow)
                        continue;
                    sendPacket(pURB);
                    outStandingURBProcesses.put(pURB.getDestId(), outStandingURBProcesses.get(pURB.getDestId())+1);
                    itURB.remove();
                }
                if (!recMine) {
                    synchronized (FIFO.lockSending) {
                        if (FIFO.windowLimit>=Main.m) {
                            if (sentMessage.containsAll(Main.broadcasted)) {
                                System.out.println("Signaling end of broadcasting messages");
                                Main.coordinator.finishedBroadcasting();
                                recMine = true;
                            }
                        }
                    }
                }
            }
        }
    }

    private void sendPacket(Packet p) {
        String sendString = p.getMessage();
        InetAddress destIp = p.getDestIp();
        int destPort = p.getDestPort();
        String sendStringWithRet;
        // Send data
        //System.out.println("Sending " + sendString + " to " + p.getDestId() + " in send");
        synchronized (lock) {
            ArrayList<Long> retransmits =
                    toRecAckProcess.get(p.getDestId()-1).getOrDefault(p, new ArrayList<>());
            //System.out.println("Retransmits: " + retransmits + " r num: " + retransmits.size());
            sendStringWithRet = sendString + ",r" + retransmits.size();
            retransmits.add(System.nanoTime());
            toRecAckProcess.get(p.getDestId()-1).put(p, retransmits);
            //System.out.println("New ack entry: " + toRecAckProcess);
            //System.out.println(numOutstanding);
        }
        //System.out.println("Sending " + sendString + " to " + p.getDestId() + " in send");
        byte[] sendBuf = sendStringWithRet.getBytes();
        DatagramPacket dpSend =
                new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
        sendOnSocket(dpSend);
        sentMessage.add(sendString);
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
                } catch (InterruptedException ignored) {}
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
                                pt.getrNum() >= toRecAckProcess.get(pid-1).get(pt.getPacket()).size()) {
                            if (pt.getPacket().getType() == Packet.packType.URB)
                                outStandingURBProcesses.put(pt.getPacket().getDestId(),
                                                            outStandingURBProcesses.get(pt.getPacket().getDestId())-1);
                            continue;
                        }
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
                            if (pt.getPacket().getType() == Packet.packType.URB)
                                outStandingURBProcesses.put(pt.getPacket().getDestId(),
                                        outStandingURBProcesses.get(pt.getPacket().getDestId())-1);
                            else
                                windowProcess.get(pt.getPacket().getDestId()).markPacket(pt.getPacket());
//                            if (pt.getPacket().getType() == Packet.packType.URB)
//                                System.out.println("New URB limits: " + outStandingURBProcesses);
//                            else
//                                System.out.println("New FIFO limits: " + windowProcess);
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
                    synchronized (FIFO.lockSending) {
                        FIFO.windowLimit = windowProcess.values().stream()
                                        .mapToInt(Window::getUpperBound).max().orElse(Constants.WINDOW_SIZE);
                        System.out.println("FIFO can now send up to " + FIFO.windowLimit);
                        FIFO.lockSending.notify();
                    }
                }
                LinkedList<Packet> toAck;
                Long now = System.nanoTime();
                for (int i = 0; i<hosts.size(); i++) {
                    if(!recSome[i]) {
                        recNoneCount[i] += 1;
                        if (recNoneCount[i] >= windowSize) {
                            recNoneCount[i] = 0;
                            // TODO: try to remove this limit and see what happens
                            RTO[i] = Math.min(RTO[i]*5, 60L*((long) Math.pow(10, 9)));
                        }
                    }
                }
                timeout = Collections.min(Arrays.asList(RTO));
                System.out.println("New RTOs:");
                for (int i = 0; i<hosts.size(); i++) {
                    int pid = i+1;
                    System.out.println("PID: " + pid + " RTO: " + RTO[i]/Math.pow(10, 6) + " ms");
                }
                System.out.println("New lower bound timeout: " + timeout/Math.pow(10, 6) + " ms");
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
                    Packet.packType type;
                    if (ackedPack[1].split(" ").length == 2)
                         type = Packet.packType.FIFO;
                    else
                        type = Packet.packType.URB;
                    Packet p = new Packet(ackedPack[1], address, port, pid, type);
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
