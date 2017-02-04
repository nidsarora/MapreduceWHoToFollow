  import java.io.IOException;
  import java.util.StringTokenizer;
  import java.util.*;
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

   
    public static class InverterMapper extends Mapper<Object, Text, Text, Text>{
 
       public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
          StringTokenizer itr = new StringTokenizer(value.toString());
              if (itr.countTokens() >= 2) {
                   Text follower = new Text();
                   follower.set(itr.nextToken());
                   while (itr.hasMoreTokens()) {
                     String followed1=itr.nextToken();
                     Text followed=new Text(followed1);
                      String s_already_followed1="-";
                      String s_already_followed=s_already_followed1.concat(followed1);
                      Text already_followed=new Text(s_already_followed);
                      context.write(followed, follower);
                      context.write(follower,already_followed);
                      }
                 }
         }
     }

     

 public static class InvertReducer1 extends Reducer<Text, Text, Text, Text>{
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> persons_to_be_recommended = new HashSet<>();
            for(Text person: values){
                persons_to_be_recommended.add(person.toString());
            }
            result.set(convertSetToString(persons_to_be_recommended));
            context.write(key, result);
        }
        private static String convertSetToString(Set<String> persons_to_be_recommended){
            String result = "";
            Iterator<String> itr = persons_to_be_recommended.iterator();
            while(itr.hasNext()){
                result += itr.next();
                if(itr.hasNext()){
                    result += " ";
                }
            }
            return result;
        }
}




    // Main method
    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "Who To Follow");
      job.setJarByClass(WhoToFollow.class);
      job.setMapperClass(InverterMapper.class);
      job.setReducerClass(InvertReducer1.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
