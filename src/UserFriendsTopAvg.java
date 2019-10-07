import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class UserFriendsTopAvg {


    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "top mutual friends pair job 1");

        // Main class
        job1.setJarByClass(UserFriendsTopAvg.class);

        // Mapper-Combiner-Reducer class
        job1.setMapperClass(UserFriendsTopAvgMapper.class);
        job1.setReducerClass(UserTopAvgFriendsReducer.class);


        // key class
        job1.setMapOutputKeyClass(UserAvgKey.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class); // Friend pair e.g. (user_A_id, user_B_id)
        job1.setOutputValueClass(Text.class); // comma separated friends id list e.g. 1,2,3,4,5...

        System.out.println(args[1]);
        System.out.println(args[2]);

        // specify input output directory
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        System.exit(job1.waitForCompletion(true) ? 0 : 1);

    }

}