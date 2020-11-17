package cs451;

import java.io.*;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

public class MessagePacket implements Serializable {
    private String message;
    private int[] W;

    public MessagePacket(String message, int[] w) {
        this.message = message;
        W = w;
    }

    public String getMessage() {
        return message;
    }

    public int[] getW() {
        return W;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    // From https://stackoverflow.com/a/3736247
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(this);
        return out.toByteArray();
    }

    // From https://stackoverflow.com/a/3736247
    public static MessagePacket deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return (MessagePacket) is.readObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessagePacket that = (MessagePacket) o;
        return message.equals(that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message);
    }

    @Override
    public String toString() {
        return "MessagePacket{" +
                "message='" + message + '\'' +
                ", W=" + Arrays.toString(W) +
                '}';
    }
}
