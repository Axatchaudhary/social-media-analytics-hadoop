import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class MutualFriendsReducer extends Reducer<FriendPairKey, Text, FriendPairKey, Text> {
    HashSet<String> common;
    Text finalMutualFriendList;
    StringBuilder tempList;

    public void reduce(FriendPairKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        common = new HashSet<>(); // store common friends of user.
        tempList = new StringBuilder();
        StringTokenizer itr_friends;

        for (Text friends : values) {
            itr_friends = new StringTokenizer(friends.toString(), ",");

            while (itr_friends.hasMoreTokens()) {
                String mf = itr_friends.nextToken();
                if (common.contains(mf)) { // if this friend already exists that means it's a mutual friend
                    tempList.append(mf).append(',');
                } else {
                    common.add(mf); // mark friend for the first time.
                }
            }
        }

        if (tempList.length() > 1) { // more than zero mutual friends
            finalMutualFriendList = new Text(tempList.substring(0, tempList.length() - 1));
            context.write(key, finalMutualFriendList);
        }

    }
}
