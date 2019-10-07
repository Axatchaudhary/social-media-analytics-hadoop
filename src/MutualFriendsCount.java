import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MutualFriendsCount {

    public static void main(String[] args) throws Exception {

        final String TMP = "TEMP_OUTPUT"; //used for storing temporary job output

        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>JOB 1

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "top mutual friends pair job 1");

        // Main class
        job1.setJarByClass(MutualFriendsCount.class);

        // Mapper-Combiner-Reducer class
        job1.setMapperClass(MutualFriendsMapper.class);
        job1.setReducerClass(MutualFriendsReducer.class);


        // key class
        job1.setMapOutputKeyClass(FriendPairKey.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(FriendPairKey.class); // Friend pair e.g. (user_A_id, user_B_id)
        job1.setOutputValueClass(Text.class); // comma separated friends id list e.g. 1,2,3,4,5...

        System.out.println(args[1]);
        System.out.println(TMP);

        // specify input output directory
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(TMP));

        job1.waitForCompletion(true);


        // >>>>>>>>>>>>>>>>>>>>>>>>>>>>JOB 2


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "top mutual friends pair job 2");

        // Main class
        job2.setJarByClass(MutualFriendsCount.class);

        // Mapper-Combiner-Reducer class
        job2.setMapperClass(MutualFriendsCountMapper.class);
        job2.setReducerClass(MutualFriendsCountReducer.class);
        job2.setPartitionerClass(PairPartitioner.class);

        // key class
        job2.setMapOutputKeyClass(FriendPairCountKey.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(FriendPairCountKey.class); // Friend pair e.g. (user_A_id, user_B_id)
        job2.setOutputValueClass(Text.class); // comma separated friends id list e.g. 1,2,3,4,5...

        System.out.println(TMP);
        System.out.println(args[2]);

        // specify input output directory
        FileInputFormat.addInputPath(job2, new Path(TMP));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);

    }

}