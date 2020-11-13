package cs451;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Perfect link implementation, consisting of 3 threads:
 * 1. Sender that send the "sendable messages" per process
 * 2. ACKChecker to check which Ack still needs to be received and window resizing
 * 3. Receive that deliver something to URB or pass an ACK to the ACKChecker
 */
public class PerfectLink {

    private final int id; // My id
    private final int myPort; // My port number
    private final HashMap<Integer, Integer> portMap; // Map process to the port used by that process
    // Messages to send received from URB or from the ACKChecker
    private final LinkedBlockingQueue<Packet> messageToSend;
    private final LinkedBlockingQueue<String> messageToDeliver; // Message to deliver up to URB
    // Queue used by the deliver thread to inform the ACKChecker of the ACKs received
    private final LinkedBlockingQueue<PacketTimeRnum> recACKs = new LinkedBlockingQueue<>();
    // We keep two different windows to improve performance, the FIFO window will be substituted by a window
    // for any layer you'd want to put above URB in next implementation
    private final ArrayList<Packet> packetToSendFIFO = new ArrayList<>(); // Packet to send of FIFO type
    private final ArrayList<Packet> packetToSendURB = new ArrayList<>(); // Packet to send of URB type
    private final HashMap<Integer, Window> windowFIFO = new HashMap<>(); // Window for FIFO packets
    private final HashMap<Integer, Window> windowURB = new HashMap<>(); // Window for URB packets
    // Map each URB packets to a lsn (different by process)
    private final HashMap<Integer, HashMap<Packet, Integer>> URBlsn = new HashMap<>();
    // Keep track of next lsn to send for URB (by proc)
    private final HashMap<Integer, Integer> URBlsnCount = new HashMap<>();
    private final List<Host> hosts; // List of hosts
    // Map each process to a map of packets -> each packet to a list of time in which it was sent
    private final ArrayList<HashMap<Packet, ArrayList<Long>>> toRecAckProcess;
    private DatagramSocket dsSend; // Socket to send messages
    private DatagramSocket dsRec; // Socket to receive messages
    private static final HashSet<String> recMessage = new HashSet<>(); // Messages received
    private static final Object lockAck = new Object(); // Lock on toReckAck Map
    private static final Object lockURB = new Object(); // Lock on URB window and lsn maps
    private static final Object lockFIFO = new Object(); // Lock on FIFO window


    /**
     * Create a Perfect link
     * @param id id of this process
     * @param myPort port of this process
     * @param hosts list of hosts used
     * @param messageToDeliver queue to send message to the layer above (URB) for delivery
     * @param messageToSend queue to receive message from the layer above (URB) for sending
     */
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

        // Set up all the control structures by process
        for (Host h: hosts) {
            portMap.put(h.getId(), h.getPort());
            toRecAckProcess.add(h.getId()-1, new HashMap<>());
            windowFIFO.put(h.getId(), new Window(Constants.WINDOW_SIZE));
            windowURB.put(h.getId(), new Window(Constants.WINDOW_SIZE));
            URBlsn.put(h.getId(), new HashMap<>());
            URBlsnCount.put(h.getId(), 0);
        }

        // Start delivering, ack checker and sender
        receiveAndDeliver();
        startAckCheck();
        send();
    }

    /**
     * Class (thread) that will sent messages coming from URB or the ACKChecker
     */
    private class Send extends Thread {
        /**
         * Run the Sender thread
         */
        @Override
        public void run() {
            while(true) {
                // Get everything you can from the messageToSend queue
                ArrayList<Packet> pToSend = new ArrayList<>();
                Packet p1 = null;
                try {
                    // If nothing new in the queue, wake up after 1 second to see if there is something already
                    // in the lists
                    p1 = messageToSend.poll(1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {}
                // If we got something, add everything we got to the correct list (if not already present)
                if (p1!=null) {
                    pToSend.add(p1);
                    messageToSend.drainTo(pToSend);
                    pToSend.forEach(p -> {
                        if (p.getType() == Packet.packType.FIFO) {
                            if (!packetToSendFIFO.contains(p))
                                packetToSendFIFO.add(p); // Add to packets to send FIFO
                        } else {
                            if (!packetToSendURB.contains(p))
                                packetToSendURB.add(p); // Add to packets to send URB
                        }
                    });
                }
                // Iterate over the packets to send on FIFO and check if we can send them
                for(Iterator<Packet> itFIFO = packetToSendFIFO.iterator(); itFIFO.hasNext();) {
                    Packet pFIFO = itFIFO.next();
                    // Get lsn (second element of the message, other data will eventually come later)
                    int lsn = Integer.parseInt(pFIFO.getMessage().split(" ")[1]);
                    synchronized (lockFIFO) {
                        // Check if packet cannot be sent by looking into the process window FIFO
                        if (!windowFIFO.get(pFIFO.getDestId()).canSend(lsn)) {
                            // Check if packet (inside the window) was already acked. If it is, remove it from the sends
                            if (windowFIFO.get(pFIFO.getDestId()).alreadyAck(lsn))
                                itFIFO.remove();
                            continue;
                        }
                    }
                    // We can send the packet. Do it and remove
                    sendPacket(pFIFO);
                    itFIFO.remove();
                }
                // Iterate over the URB packet
                for(Iterator<Packet> itURB = packetToSendURB.iterator(); itURB.hasNext();) {
                    Packet pURB = itURB.next();
                    int lsn;
                    synchronized (lockURB) {
                        // Get the packet lsn from the map
                        // or generate a new one if it's the first time we see the packet
                        HashMap<Packet, Integer> lsnProcess = URBlsn.get(pURB.getDestId());
                        if (lsnProcess.containsKey(pURB))
                            lsn = lsnProcess.get(pURB); // Old packet
                        else {
                            lsn = URBlsnCount.get(pURB.getDestId()) + 1; // New one: new lsn
                            lsnProcess.put(pURB, lsn);
                            URBlsnCount.put(pURB.getDestId(), lsn);
                            URBlsn.put(pURB.getDestId(), lsnProcess);
                        }
                        // Check if we can send the packet and if it was already acked, eventually removing it
                        if (!windowURB.get(pURB.getDestId()).canSend(lsn)) {
                            if (windowURB.get(pURB.getDestId()).alreadyAck(lsn))
                                itURB.remove();
                            continue;
                        }
                    }
                    // We can send the packet: we do it
                    sendPacket(pURB);
                    itURB.remove();
                }
            }
        }
    }

    /**
     * Send the packet to the process
     * @param p packet to send
     */
    private void sendPacket(Packet p) {
        // Get info from the packets
        String sendString = p.getMessage();
        InetAddress destIp = p.getDestIp();
        int destPort = p.getDestPort();
        String sendStringWithRet;
        synchronized (lockAck) {
            // Get how many times the packet was retransmitted
            ArrayList<Long> retransmits =
                    toRecAckProcess.get(p.getDestId()-1).getOrDefault(p, new ArrayList<>());
            // Add the retransmit number to the message and send it with such number
            sendStringWithRet = sendString + ",r" + retransmits.size();
            // Add the time we are sending the packet to the retransmits list and set it into the map
            retransmits.add(System.nanoTime());
            toRecAckProcess.get(p.getDestId()-1).put(p, retransmits);
        }
        // Create the DatagramPacket of the message and send it on the socket
        byte[] sendBuf = sendStringWithRet.getBytes();
        DatagramPacket dpSend =
                new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
        sendOnSocket(dpSend);
    }

    /**
     * Start the sender thread
     */
    public void send() {
        new Send().start();
    }

    /**
     * Class (i.e. thread) that will check periodically the acks
     * Based on the acks this thread choose if increase/decrease windows
     * And choose the retransmit time for timeout based on the measured RTTs
     */
    private class ACKChecker extends Thread {
        boolean[] recFirst; // Whether we received the very first ACK from a process
        Long timeout; // Timeout after which the thread wakeup if no new acks
        Long[] RTTs; // RTT Smoothed (like TCP)
        Long[] RTTd; // RTT Deviation (like TCP)
        Long[] RTO; // Retransmission time (like TCP)
        Double alpha; // Alpha parameter for RTO estimation
        Double beta; // Beta parameter for RTO estimation
        HashMap<Integer, HashSet<Packet>> dupAck = new HashMap<>(); // Map of duplicated ack per process
        boolean[] firstTimeoutURB; // Bool vector to give a "second chance" after timeout URB
        boolean[] firstTimeoutFIFO; // Bool vector to give a "second chance" after timeout FIFO

        /**
         * Init ACKChecker
         */
        public ACKChecker() {
            recFirst = new boolean[hosts.size()];
            Arrays.fill(recFirst, false);
            // Start with a timeout of 1 second
            timeout = 1000L*((long) Math.pow(10, 6));
            RTTs = new Long[hosts.size()];
            RTTd = new Long[hosts.size()];
            RTO = new Long[hosts.size()];
            Arrays.fill(RTO, timeout);
            // Alpha and beta as per RCF specification
            alpha = 1.0/8.0;
            beta = 1.0/4.0;
            firstTimeoutURB = new boolean[hosts.size()];
            firstTimeoutFIFO = new boolean[hosts.size()];
            Arrays.fill(firstTimeoutURB, false);
            Arrays.fill(firstTimeoutFIFO, false);
        }

        /**
         * Run the ACKChecker thread
         */
        @Override
        public void run() {
            while (true) {
                // Try to receive some ACKs from the delivering thread
                PacketTimeRnum recAck = null;
                try {
                    recAck = recACKs.poll(timeout, TimeUnit.NANOSECONDS); // Go over to next checks after timeout
                } catch (InterruptedException ignored) {}
                if (recAck!=null) {
                    // If we receive something, we process it
                    List<PacketTimeRnum> newAcks = new LinkedList<>();
                    newAcks.add(recAck);
                    recACKs.drainTo(newAcks);
                    for (PacketTimeRnum pt: newAcks) {
                        int pid = pt.getPacket().getDestId();
                        boolean isDup;
                        // Check if an ACK is duplicated ACK by using the correct process window
                        if (pt.getPacket().getType() == Packet.packType.URB) {
                            synchronized (lockURB) {
                                // Get lsn and check if was already ack
                                int lsn = URBlsn.get(pt.getPacket().getDestId()).get(pt.getPacket());
                                isDup = windowURB.get(pid).alreadyAck(lsn);
                            }
                        }
                        else {
                            // Get lsn and check if was already ack
                            int lsn = Integer.parseInt(pt.getPacket().getMessage().split(" ")[1]);
                            synchronized (lockFIFO) {
                                isDup = windowFIFO.get(pid).alreadyAck(lsn);
                            }
                        }
                        if (isDup) {
                            // If is duplicated, need to check if it's first or second duplicate
                            HashSet<Packet> ackRec = dupAck.getOrDefault(pid, new HashSet<>());
                            if (ackRec.contains(pt.getPacket())) {
                                // If it's the second duplicate need to reduce window size (using .dupAck())
                                // Reduce correct window depending on packet type
                                if (pt.getPacket().getType() == Packet.packType.URB) {
                                    synchronized (lockURB) {
                                        windowURB.get(pid).dupAck();
                                    }
                                }
                                else {
                                    synchronized (lockFIFO) {
                                        windowFIFO.get(pid).dupAck();
                                    }
                                }
                            }
                            else {
                                // If it's the first duplicated packet, just add it to the set of duplicated
                                // Next one will trigger the dupAck procedure
                                ackRec.add(pt.getPacket());
                                dupAck.put(pt.getPacket().getDestId(),ackRec);
                            }
                            synchronized (lockAck) {
                                // Remove the packet if we still have it in the toReckAck map
                                // (avoiding incorrect timeouts)
                                toRecAckProcess.get(pid-1).remove(pt.getPacket());
                            }
                            continue; // No need to check the packets more
                        }
                        if (pt.getPacket().getType() == Packet.packType.URB) {
                            // Received an ack for URB, mark the packet as received and
                            // increase the URB window for the process (by 1 or doubling, depending on threshold)
                            synchronized (lockURB) {
                                int lsn = URBlsn.get(pt.getPacket().getDestId()).get(pt.getPacket());
                                windowURB.get(pid).markPacket(lsn);
                                windowURB.get(pid).increaseSize();
                            }
                        }
                        else {
                            // Received an ack for FIFO, mark the packet as received and
                            // increase the FIFO window for the process (by 1 or doubling, depending on threshold)
                            int lsn = Integer.parseInt(pt.getPacket().getMessage().split(" ")[1]);
                            synchronized (lockFIFO) {
                                windowFIFO.get(pid).markPacket(lsn);
                                windowFIFO.get(pid).increaseSize();
                            }
                        }
                        long RTTm;
                        synchronized (lockAck) {
                            // Measure the RTT for the packet and remove it from the map
                            // Note that the RTT is measured with respect to the correct element of the list
                            // inside to RecACK. Such element is defined by looking at the retransmission number
                            // The time the packet was received is inside the PacketTimeRnum
                            HashMap<Packet, ArrayList<Long>> toRecACK = toRecAckProcess.get(pid-1);
                            RTTm = pt.getTimeRec() - toRecACK.get(pt.getPacket()).get(pt.getrNum());
                            toRecACK.remove(pt.getPacket());
                            toRecAckProcess.set(pid-1, toRecACK);
                        }
                        if (!recFirst[pid-1]) {
                            // If it's the first ACK we receive from the process, define initial parameters
                            RTTs[pid-1] = RTTm;
                            RTTd[pid-1] = RTTm/2;
                            recFirst[pid-1] = true;
                        }
                        else {
                            // Update the parameters:
                            //    RTTs = (1-alpha)*RTTs + alpha*RTTm
                            //    RTTd = (1-beta)*RTTd + beta*|RTTm - RTTs|
                            RTTs[pid-1] = new Double((1-alpha)*RTTs[pid-1] + alpha*RTTm).longValue();
                            RTTd[pid-1] = new Double((1-beta)*RTTd[pid-1]
                                    + beta * Math.abs(RTTm - RTTs[pid-1])).longValue();
                        }
                        // Update RTO: RTO = RTTs + 4*RTTd
                        RTO[pid-1] = RTTs[pid-1] + 4*RTTd[pid-1];
                    }
                    synchronized (FIFO.lockSending) {
                        // Update the number of packets FIFO can send
                        // The value is updated to the maximum upper bound one of the process can handle.
                        // This way no process will be blocked by other being slower
                        // (the slower process won't receive the new packets anyway because of their windows)
                        synchronized (lockFIFO) {
                            FIFO.windowLimit = Math.max(FIFO.windowLimit,
                                    windowFIFO
                                            .values().stream()
                                            .mapToInt(Window::getUpperBound).max().orElse(Constants.WINDOW_SIZE));
                            // Notify FIFO that now can send more
                            FIFO.lockSending.notify();
                        }
                    }
                }
                LinkedList<Packet> toAck;
                timeout = Collections.max(Arrays.asList(RTO)); // Update timeout to the max of new RTOs
                Long now = System.nanoTime();
                synchronized (lockAck) {
                    toAck = new LinkedList<>();
                    // Cycle over the PIDs and define which packets must be send again
                    for (int pid = 1; pid<=hosts.size(); pid++) {
                        if (pid==id)
                            continue; // Myself -> continue
                        int finalPid = pid;
                        LinkedList<Packet> toSendPid =
                                toRecAckProcess.get(pid-1).entrySet().stream()
                                        // Filter packets for which at least RTO is passed from the last retransmit
                                        .filter(x -> now - x.getValue().get(x.getValue().size()-1) >= RTO[finalPid - 1])
                                        .map(Map.Entry::getKey)
                                        // Keep only packets not already in the queue the sender will read
                                        // (to avoid many helpless retransmits)
                                        .filter(p -> !messageToSend.contains(p))
                                        .collect(Collectors.toCollection(LinkedList::new));

                        // Cycle over the packets to see if there's something we already acked
                        // but it's still in the toRecAckProcess but we lost the second (or more) ack
                        // (and we won't send it again because the window marked it has acked)
                        for (Iterator<Packet> it = toSendPid.iterator(); it.hasNext();) {
                            Packet p = it.next();
                            if (p.getType() == Packet.packType.URB) {
                                synchronized (lockURB) {
                                    // URB type: if already acked remove from URB window
                                    int lsn = URBlsn.get(p.getDestId()).get(p);
                                    if (windowURB.get(pid).alreadyAck(lsn)) {
                                        synchronized (lockAck) {
                                            toRecAckProcess.get(pid).remove(p);
                                            it.remove();
                                        }
                                    }
                                }
                            }
                            else {
                                int lsn = Integer.parseInt(p.getMessage().split(" ")[1]);
                                synchronized (lockFIFO) {
                                    // FIFO type: if already acked remove from FIFO window
                                    if (windowFIFO.get(pid).alreadyAck(lsn)) {
                                        synchronized (lockAck) {
                                            toRecAckProcess.get(pid).remove(p);
                                            it.remove();
                                        }
                                    }
                                }
                            }
                        }
                        // Check if any FIFO packet timed out
                        boolean timeoutWindow = toSendPid.stream()
                                .map(Packet::getType)
                                .anyMatch(packType -> packType == Packet.packType.FIFO);
                        if (timeoutWindow) {
                            // If firstTimeoutFIFO[pid-1]=false we don't reduce the window (a "second chance")
                            // Giving the second chance may help because this is not like classical TCP,
                            // a process may have nothing acked at this batch but it will have in the next one
                            if (!firstTimeoutFIFO[pid-1]) {
                                firstTimeoutFIFO[pid-1] = true;
                                continue;
                            }
                            // If it's the second time of timeout, reduce the window with timeout
                            // (i.e. put the window size to Constants.WINDOW_SIZE and thresh = windowSize/2)
                            firstTimeoutFIFO[pid-1] = false;
                            synchronized (lockFIFO) {
                                windowFIFO.get(pid).timeoutStart();
                            }
                        }
                        // Check if any URB packet timed out
                        boolean timeoutURB = toSendPid.stream()
                                .map(Packet::getType)
                                .anyMatch(packType -> packType == Packet.packType.URB);
                        if (timeoutURB) {
                            // Same reasoning as FIFO about second chance
                            if (!firstTimeoutURB[pid-1]) {
                                firstTimeoutURB[pid-1] = true;
                                continue;
                            }
                            firstTimeoutURB[pid-1] = false;
                            // Timeout if it's the second time
                            synchronized (lockURB) {
                                windowURB.get(pid).timeoutStart();
                            }
                        }
                        // Add all the packets to ack for this process to the global list
                        toAck.addAll(toSendPid);
                    }
                }
                // Add all the packets in toAck into the queue which the sender will read
                if (!toAck.isEmpty())
                    messageToSend.addAll(toAck);
            }
        }
    }

    /**
     * Start the ACKChecker
     */
    public void startAckCheck() {
        new ACKChecker().start();
    }

    /**
     * Send an ACK for the message received, indicating which retransmits we are ACKing
     * @param dpRec DatagramPacket received
     * @param sRec Message received
     * @param rNum Retransmission number to ACK
     */
    private void sendACK(DatagramPacket dpRec, String sRec, String rNum) {
        String sendString;
        byte[] sendBuf;
        InetAddress destIp = dpRec.getAddress(); // IP to send to
        int pid = Integer.parseInt(sRec.split(" ")[0]); // Pid to send to
        int destPort = portMap.get(pid); // Port to send to
        sendString = String.format("ACK %d %s:%s", id, rNum, sRec); // Message is "ACK pid retransmit:messageACKed"
        // Prepare the packet to send and send it on the socket
        sendBuf = sendString.getBytes();
        DatagramPacket dpSend =
                new DatagramPacket(sendBuf, sendBuf.length, destIp, destPort);
        sendOnSocket(dpSend);
    }

    /**
     * Class (i.e. thread) that will process packets received
     */
    private class Receive extends Thread {
        /**
         * Run the receiver thread
         */
        @Override
        public void run() {
            byte[] recBuf = new byte[1024];
            DatagramPacket dpRec;
            while (true) {
                // Create packet to receive on
                dpRec = new DatagramPacket(recBuf, recBuf.length);
                // Wait until receiving
                recOnSocket(dpRec);
                Long now = System.nanoTime();
                // Get message received
                String sRec = new String(trim(recBuf), StandardCharsets.UTF_8);
                if (!sRec.contains("ACK")) {
                    // If it's a normal message, note which one is the retransmit to ack and ACK it
                    // Note: Message is delivered only if it wasn't received before
                    String[] messAndResend = sRec.split(","); // Resend number after the ,
                    if (!recMessage.contains(messAndResend[0])) {
                        // Add to received
                        recMessage.add(messAndResend[0]);
                        try {
                            messageToDeliver.put(messAndResend[0]); // Deliver above
                        } catch (InterruptedException e) {
                            System.out.println("Exception trying to deliver a message in PF " + e.toString());
                        }
                    }
                    // Send ACK
                    sendACK(dpRec, messAndResend[0], messAndResend[1]); // Datagram, message, retransmit num
                }
                else{
                    // It's an ACK packet -> get the parameters from it
                    String[] ackedPack = sRec.split(":");
                    String[] packInfo = ackedPack[0].split(" "); // Pid and retransmit num
                    int pid = Integer.parseInt(packInfo[1]);
                    int rNum = Integer.parseInt(packInfo[2].replace("r", ""));
                    InetAddress address = dpRec.getAddress(); // IP address
                    int port = portMap.get(pid); // Port
                    Packet.packType type;
                    // Depending on length of the header message (2 or 3), determine the packet type (FIFO or URB)
                    if (ackedPack[1].split(" ").length == 2)
                        type = Packet.packType.FIFO;
                    else
                        type = Packet.packType.URB;
                    // Create a packet with the known info and put in the queue for the ACKChecker
                    Packet p = new Packet(ackedPack[1], address, port, pid, type);
                    try {
                        // Create a packetTimeRnum with packet, time received and retransmit number
                        recACKs.put(new PacketTimeRnum(p, now, rNum));
                    } catch (InterruptedException e) {
                        System.out.println("Exception trying to put an ACK in the queue " + e.toString());
                    }
                }
                recBuf = new byte[1024]; // Clean buffer
            }
        }
    }

    /**
     * Start receiver process
     */
    public void receiveAndDeliver(){
        new Receive().start();
    }

    /**
     * Send a packet on the dsSend socket
     * @param dpSend DatagramPacket to send
     */
    private void sendOnSocket(DatagramPacket dpSend) {
        try {
            dsSend.send(dpSend);
        } catch (IOException e) {
            System.out.println("Sending ACK error: " + e.toString());
        }
    }

    /**
     * Receive a packet from dpRec
     * @param dpRec DatagramPacket where loading the received packet
     */
    private void recOnSocket(DatagramPacket dpRec) {
        try {
            dsRec.receive(dpRec);
        } catch (IOException e) {
            System.out.println("Receiving error: " + e.toString());
        }
    }

    /**
     * Trim a byte array removing the trailing zero bytes
     * Found on https://stackoverflow.com/a/17004488
     * @param bytes array to trim
     * @return array without trailing zero bytes
     */
    private static byte[] trim(byte[] bytes)
    {
        int i = bytes.length - 1;
        while (i >= 0 && bytes[i] == 0)
            --i;
        return Arrays.copyOf(bytes, i + 1);
    }

    /**
     * Check equality
     * @param o Another object
     * @return true/false if objects are or not the same
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PerfectLink that = (PerfectLink) o;
        return id == that.id &&
                myPort == that.myPort &&
                Objects.equals(dsSend, that.dsSend) &&
                Objects.equals(dsRec, that.dsRec) &&
                Objects.equals(portMap, that.portMap) &&
                Objects.equals(messageToSend, that.messageToSend) &&
                Objects.equals(messageToDeliver, that.messageToDeliver) &&
                Objects.equals(recACKs, that.recACKs) &&
                Objects.equals(packetToSendFIFO, that.packetToSendFIFO) &&
                Objects.equals(packetToSendURB, that.packetToSendURB) &&
                Objects.equals(windowFIFO, that.windowFIFO) &&
                Objects.equals(windowURB, that.windowURB) &&
                Objects.equals(URBlsn, that.URBlsn) &&
                Objects.equals(URBlsnCount, that.URBlsnCount) &&
                Objects.equals(hosts, that.hosts) &&
                Objects.equals(toRecAckProcess, that.toRecAckProcess);
    }

    /**
     * @return hashcode
     */
    @Override
    public int hashCode() {
        return Objects.hash(id, myPort, dsSend, dsRec, portMap, messageToSend,
                messageToDeliver, recACKs, packetToSendFIFO, packetToSendURB,
                windowFIFO, windowURB, URBlsn, URBlsnCount, hosts, toRecAckProcess);
    }
}
