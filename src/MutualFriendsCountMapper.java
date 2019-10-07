import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class MutualFriendsCountMapper extends Mapper<Object, Text, FriendPairCountKey, Text> {

    FriendPairCountKey fpc = new FriendPairCountKey();
    Text mfs = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());

        // get main content
        String friendPair = itr.nextToken();
        String mutualFriends = itr.nextToken();

        // get friend pair
        StringTokenizer itr2 = new StringTokenizer(friendPair, ",");
        int ua = Integer.parseInt(itr2.nextToken());
        int ub = Integer.parseInt(itr2.nextToken());

        //count friends of user
        int count = 1;
        for(int i = 0; i < mutualFriends.length(); i++){
            if (mutualFriends.charAt(i) == ',') count++;
        }

        fpc.set(ua, ub, count);
        mfs.set(mutualFriends);

        context.write(fpc, mfs);

    }
}
