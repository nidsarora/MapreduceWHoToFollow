  import java.io.IOException;
  import java.util.StringTokenizer;
  import org.apache.hadoop.conf.Configuration;
  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.io.IntWritable;
  import org.apache.hadoop.io.Text;
  import org.apache.hadoop.mapreduce.Job;
  import org.apache.hadoop.mapreduce.Mapper;
  import org.apache.hadoop.mapreduce.Reducer;
  import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
  import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  public class WhoToFollow {

   
    public static class InverterMapper extends Mapper<Object, Text, IntWritable, IntWritable>{
 
       public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
          StringTokenizer itr = new StringTokenizer(value.toString());
              if (itr.countTokens() >= 2) {
                   IntWritable person = new IntWritable(Integer.parseInt(itr.nextToken()));
                   while (itr.hasMoreTokens()) {
                      IntWritable follower = new IntWritable(Integer.parseInt(itr.nextToken()));
                      context.write(follower, person);
                       }
                 }
         }
     }


    // Main method
    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Who To Follow");
      job.setJarByClass(WhoToFollow.class);
      job.setMapperClass(InverterMapper.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



