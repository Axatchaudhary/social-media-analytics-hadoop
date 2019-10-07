import org.apache.hadoop.mapreduce.Partitioner;

public class PairPartitioner extends Partitioner<FriendPairCountKey, FriendPairCountKey> {
    @Override
    public int getPartition(FriendPairCountKey friendPairCountKey, FriendPairCountKey friendPairCountKey2, int i) {
        return 0;
    }
}
