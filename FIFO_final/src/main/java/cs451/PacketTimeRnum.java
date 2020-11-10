package cs451;

import java.util.Objects;

/**
 * Utility class to bound a packet ACK with its retransmission number and the time it was received
 */
public class PacketTimeRnum {
    private final Packet packet;
    private final Long timeRec;
    private final int rNum;

    /**
     * @param p Packet
     * @param time time ACK received
     * @param rNum retransmission number
     */
    public PacketTimeRnum(Packet p, Long time, int rNum) {
        this.packet = p;
        this.timeRec = time;
        this.rNum = rNum;
    }

    /**
     * @return time received
     */
    public Long getTimeRec() {
        return timeRec;
    }

    /**
     * @return the packet
     */
    public Packet getPacket() {
        return packet;
    }

    /**
     * @return retransmission number
     */
    public int getrNum() {
        return rNum;
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
        PacketTimeRnum that = (PacketTimeRnum) o;
        return packet.equals(that.packet) &&
                timeRec.equals(that.timeRec);
    }

    /**
     * @return hashcode
     */
    @Override
    public int hashCode() {
        return Objects.hash(packet, timeRec);
    }

    /**
     * @return string representation
     */
    @Override
    public String toString() {
        return "PacketTimeRnum{" +
                "packet=" + packet +
                ", timeRec=" + timeRec +
                ", rNum=" + rNum +
                '}';
    }
}
