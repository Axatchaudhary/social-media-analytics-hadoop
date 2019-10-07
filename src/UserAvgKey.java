import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserAvgKey implements WritableComparable<UserAvgKey> {

    int user_A;
    float avg;

    public UserAvgKey(int ua, float c) {
        this.user_A = ua;
        this.avg = c;
    }

    public UserAvgKey() {
    }

    public void set(int ua, float c) {
        this.user_A = ua;
        this.avg = c;
    }


    @Override
    public String toString() {
        return user_A + ", " + avg;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(user_A);
        dataOutput.writeFloat(avg);
    }

    @Override
    public int hashCode() {
        return 0; // want all keys to go to same reducer
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.user_A = dataInput.readInt();
        this.avg = dataInput.readFloat();
    }

    @Override
    public int compareTo(UserAvgKey o) {
        if (o == null) return -1;
        return (int) o.avg - (int) this.avg; // force sorting in descending order in sort phase
    }
}
