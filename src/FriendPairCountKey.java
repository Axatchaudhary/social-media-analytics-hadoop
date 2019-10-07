import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FriendPairCountKey implements WritableComparable<FriendPairCountKey> {

    int user_A;
    int user_B;
    int count;

    public FriendPairCountKey(int ua, int ub, int c) {
        this.user_A = ua;
        this.user_B = ub;
        this.count = c;
    }

    public FriendPairCountKey() { }

    public void set(int ua, int ub, int c) {
        this.user_A = ua;
        this.user_B = ub;
        this.count = c;
    }

    @Override
    public int hashCode() {
        return 0; // force all keys to go to same reducer
    }

    @Override
    public String toString() {
        return user_A + ", " + user_B + "\t"+ count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(user_A);
        dataOutput.writeInt(user_B);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.user_A = dataInput.readInt();
        this.user_B = dataInput.readInt();
        this.count = dataInput.readInt();
    }

    @Override
    public int compareTo(FriendPairCountKey o) {
        if (o == null) return -1;
        return o.count - this.count; // force sorting in descending order during sorting phase
    }
}
