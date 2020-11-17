package cs451;

import java.net.InetAddress;
import java.util.Objects;

/**
 * Utility class to represent a packet to be sent to a specified process
 * Include also the packet type (FIFO, URB)
 */
public class Packet {
    private final MessagePacket messagePacket;
    private final InetAddress destIp;
    private final int destPort;
    private final int destId;
    private final packType type;
    protected enum packType {LCausal, URB}; // Packet type enum used in another parts of the code

    /**
     * @param messagePacket message to send + data
     * @param destIp destination ip of the receiver
     * @param destPort destination port of the receiver
     * @param destId destination id of the receiver
     * @param type packet type
     */
    public Packet(MessagePacket messagePacket, InetAddress destIp, int destPort, int destId, packType type) {
        this.messagePacket = messagePacket;
        this.destIp = destIp;
        this.destPort = destPort;
        this.destId = destId;
        this.type = type;
    }

    /**
     * @return destination ip
     */
    public InetAddress getDestIp() {
        return destIp;
    }

    /**
     * @return destination id
     */
    public int getDestId() {
        return destId;
    }

    /**
     * @return destination port
     */
    public int getDestPort() {
        return destPort;
    }

    /**
     * @return the message
     */
    public MessagePacket getMessagePacket() {
        return messagePacket;
    }

    /**
     * @return the packet type
     */
    public packType getType() {
        return type;
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
        Packet packet = (Packet) o;
        return destPort == packet.destPort &&
                destId == packet.destId &&
                messagePacket.equals(packet.messagePacket) &&
                destIp.equals(packet.destIp) &&
                type == packet.type;
    }

    /**
     * @return hashcode
     */
    @Override
    public int hashCode() {
        return Objects.hash(messagePacket, destIp, destPort, destId, type);
    }

    /**
     * @return packet string representation
     */
    @Override
    public String toString() {
        return "Packet{" +
                "message='" + messagePacket.getMessage() + '\'' +
                ", destId=" + destId +
                ", type=" + type +
                '}';
    }
}


