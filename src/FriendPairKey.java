import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FriendPairKey implements WritableComparable<FriendPairKey> {

    int user_A;
    int user_B;

    public FriendPairKey(int ua, int ub) {
        this.user_A = ua;
        this.user_B = ub;
    }

    public FriendPairKey() {
    }

    public void set(int ua, int ub) {
        this.user_A = ua;
        this.user_B = ub;
    }

    private static int hash(int h) {
        // This function ensures that hashCodes that differ only by
        // constant multiples at each bit position have a bounded
        // number of collisions (approximately 8 at default load factor).
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + this.user_A;
        result = prime * result + this.user_B;
        return hash(result);
    }

        @Override
        public String toString() {
            return this.user_A + "," + this.user_B;
        }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.user_A);
        dataOutput.writeInt(this.user_B);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.user_A = dataInput.readInt();
        this.user_B = dataInput.readInt();
    }

    @Override
    public int compareTo(FriendPairKey o) {
        if (o == null) return 1;
        if(this.user_A == o.user_A) return this.user_B - o.user_B;
        return this.user_A - o.user_A;
    }
}
