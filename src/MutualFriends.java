import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MutualFriends {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mutual friends");

        // Main class
        job.setJarByClass(MutualFriends.class);

        // Mapper-Combiner-Reducer class
        job.setMapperClass(MutualFriendsMapper.class);
        job.setReducerClass(MutualFriendsReducer.class);

        // key class
        job.setMapOutputKeyClass(FriendPairKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(FriendPairKey.class); // Friend pair e.g. (user_A_id, user_B_id)
        job.setOutputValueClass(Text.class); // comma separated friends id list e.g. 1,2,3,4,5...

        System.out.println(args[1]);
        System.out.println(args[2]);

        // specify input output directory
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}