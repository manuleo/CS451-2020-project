package cs451;

import java.net.InetAddress;
import java.util.Objects;

public class Packet {
    private String message;
    private InetAddress destIp;
    private int destPort;
    private int destId;
    private packType type;
    protected enum packType {FIFO, URB};

    public Packet(String message, InetAddress destIp, int destPort, int destId, packType type) {
        this.message = message;
        this.destIp = destIp;
        this.destPort = destPort;
        this.destId = destId;
        this.type = type;
    }

    public InetAddress getDestIp() {
        return destIp;
    }

    public int getDestId() {
        return destId;
    }

    public int getDestPort() {
        return destPort;
    }

    public String getMessage() {
        return message;
    }

    public packType getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Packet packet = (Packet) o;
        return destPort == packet.destPort &&
                destId == packet.destId &&
                message.equals(packet.message) &&
                destIp.equals(packet.destIp) &&
                type == packet.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, destIp, destPort, destId, type);
    }

    @Override
    public String toString() {
        return "Packet{" +
                "message='" + message + '\'' +
//                ", destIp=" + destIp +
//                ", destPort=" + destPort +
                ", destId=" + destId +
                ", type=" + type +
                '}';
    }
}


