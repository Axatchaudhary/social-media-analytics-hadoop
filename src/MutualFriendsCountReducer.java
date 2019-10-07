import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MutualFriendsCountReducer extends Reducer<FriendPairCountKey, Text, FriendPairCountKey, Text> {

    int c = 0;

    @Override
    protected void reduce(FriendPairCountKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // get first 10 top count pairs
        for (Text mfs : values){
            if (c++ < 10){
                context.write(key, mfs);
            }
        }

    }
}
