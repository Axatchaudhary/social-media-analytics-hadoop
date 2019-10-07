import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class MutualFriendsMapper extends Mapper<Object, Text, FriendPairKey, Text> {

    FriendPairKey fp = new FriendPairKey();
    Text finalFriendsList = new Text(); // user's friends list in text format used as final value in mapper
    StringBuilder tempList;
    ArrayList<String> tempFriendList = new ArrayList<>(); // list of user's friends

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        tempList = new StringBuilder();

        // get main user
        StringTokenizer itr = new StringTokenizer(value.toString());
        int ua = Integer.parseInt(itr.nextToken());

        // get user's friends
        if (!itr.hasMoreTokens()) return; // this user has no friends
        String friends = itr.nextToken();
        StringTokenizer friends_itr = new StringTokenizer(friends, ",");
        tempFriendList.clear();

        while (friends_itr.hasMoreTokens()) {
            String nxt = friends_itr.nextToken();
            tempList.append(nxt).append(',');
            tempFriendList.add(nxt);
        }
        finalFriendsList.set(tempList.substring(0, tempList.length() - 1));

        for (String f : tempFriendList) {
            int ub = Integer.parseInt(f);
            if (ua <= ub) fp.set(ua, ub); // ordering because relationship exists both ways so both users go to same reducer
            else fp.set(ub, ua);
            context.write(fp, finalFriendsList);
        }

    }
}
