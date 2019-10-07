import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.logging.Logger;

public class MutualFriendsInMemoryJoinReducer extends Reducer<FriendPairKey, Text, FriendPairKey, Text> {
    HashSet<String> common = new HashSet<>();
    Text finalMutualFriendList;
    StringBuilder tempList;

    HashMap<Integer, User> users = new HashMap<>();
    final Logger LOG = Logger.getGlobal();


    private void readUser(String line){

        String[] fields = line.split(",");
        int uid = Integer.parseInt(fields[0]);
        String fn = fields[1];
        String ln = fields[2];
        String address = fields[3];
        String city = fields[4];
        String state = fields[5];
        String zip = fields[6];
        String country = fields[7];
        String username = fields[8];
        String dob = fields[9];
        users.put(uid, new User(uid, fn, state));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LOG.info("Attempting to connecting to HDFS");
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fin = fs.open(new Path("/input/userdata.txt"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fin));
        reader.lines().forEach(this::readUser);
        reader.close();
    }

    public void reduce(FriendPairKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        common.clear();
        tempList = new StringBuilder();
        StringTokenizer itr_friends;

        for (Text friends : values) {
            itr_friends = new StringTokenizer(friends.toString(), ",");

            while (itr_friends.hasMoreTokens()) {
                String mf = itr_friends.nextToken();
                if (common.contains(mf)) {
                    int uid_mf = Integer.parseInt(mf);
                    tempList.append(users.get(uid_mf)).append(',');
                } else {
                    common.add(mf);
                }
            }
        }

        if (tempList.length() > 1) {
            finalMutualFriendList = new Text("[" + tempList.substring(0, tempList.length() - 1) + "]");
            context.write(key, finalMutualFriendList);
        }

    }
}
