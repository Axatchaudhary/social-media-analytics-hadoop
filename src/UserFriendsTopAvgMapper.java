import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.StringTokenizer;

public class UserFriendsTopAvgMapper extends Mapper<Object, Text, UserAvgKey, Text> {

    UserAvgKey ua = new UserAvgKey();

    HashMap<Integer, User> users = new HashMap<>();

    private void readUser(String line) {
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
        users.put(uid, new User(uid, address, city, state, dob));
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("ATTEMPT: connecting to hdfs");
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fin = fs.open(new Path("/input/userdata.txt"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(fin));
        reader.lines().forEach(this::readUser);
        reader.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        int ageSum = 0;
        int friendCount = 0;
        int uid = Integer.parseInt(itr.nextToken());

        if (!itr.hasMoreTokens()) return;
        String friends = itr.nextToken();

        StringTokenizer itr2 = new StringTokenizer(friends, ",");


        while (itr2.hasMoreTokens()) {
            int uid2 = Integer.parseInt(itr2.nextToken());
            ageSum += users.get(uid2).getAge();
            friendCount++;
        }

        // In-memory join
        ua.set(uid, (float) ageSum / (float) friendCount);
        context.write(ua, new Text(users.get(uid).printAddr()));

    }
}
