package cs451;

import java.util.Objects;

public class PacketTimeRnum {
    private Packet packet;
    private Long timeRec;
    private int rNum;

    public PacketTimeRnum(Packet p, Long time, int rNum) {
        this.packet = p;
        this.timeRec = time;
        this.rNum = rNum;
    }

    public Long getTimeRec() {
        return timeRec;
    }

    public Packet getPacket() {
        return packet;
    }

    public int getrNum() {
        return rNum;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PacketTimeRnum that = (PacketTimeRnum) o;
        return packet.equals(that.packet) &&
                timeRec.equals(that.timeRec);
    }

    @Override
    public int hashCode() {
        return Objects.hash(packet, timeRec);
    }

    @Override
    public String toString() {
        return "PacketTimeRnum{" +
                "packet=" + packet +
                ", timeRec=" + timeRec +
                ", rNum=" + rNum +
                '}';
    }
}
