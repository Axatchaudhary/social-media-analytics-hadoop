import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserTopAvgFriendsReducer extends Reducer<UserAvgKey, Text, IntWritable, Text> {

    int c = 0;

    @Override
    protected void reduce(UserAvgKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // get top 15 users with highest avg age of their friends
        for (Text addr : values){
            if (c++ < 15){
                int uid = key.user_A;
                String value = addr + ", " + key.avg;
                context.write(new IntWritable(uid), new Text(value));
            }
        }

    }
}
