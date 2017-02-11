 import java.io.IOException;
  import java.util.StringTokenizer;
  import java.util.*;
  import java.util.function.Predicate;
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

   /*
      This mapper just inverts the followed and the followers and tracks the people who are already being followed according to the input
      for eg  1   2 3 4
      is emitted as (2,1), (3,1) ,(4,1)//inverted emission
      and also tracking the people already followed by emiting (1,-3), (1,-2),(1,-4)
   */
    public static class InverterMapper extends Mapper<Object, Text, Text, Text>{
 
       public void map(Object key, Text value, Context context ) throws IOException, InterruptedException {
          StringTokenizer itr = new StringTokenizer(value.toString());
              if (itr.countTokens() >= 2) {//just to make sure that there are more than or equal to 2 tokens so that nextToken can be
                                            //found   easily
                   Text follower = new Text();
                   follower.set(itr.nextToken());//key of input set
                   while (itr.hasMoreTokens()) {
                       String followed1=itr.nextToken();
                       Text followed=new Text(followed1);
                       String s_already_followed1="-";
                       String s_already_followed=s_already_followed1.concat(followed1);
                       Text already_followed=new Text(s_already_followed);
                       context.write(followed, follower);//inverting the follower and followed ones
                       context.write(follower,already_followed);//maintaining the set of users who are already being followed according to the
                                                                // given input
                   }
               }
         }
     }

    
    /*
      This reducer just creates a hash set of all the emissions from the map for a particular person and finally converting to a string
      for eg emissions (1,2), (1,3)
      are combined as 1   2 3
    */
    public static class InvertReducer1 extends Reducer<Text, Text, Text, Text>{
        private Text result = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> persons_to_be_recommended = new HashSet<>();
            for(Text person: values){
                persons_to_be_recommended.add(person.toString());//for a particular key, values are added
            }
            result.set(convertSetToString(persons_to_be_recommended));
            context.write(key, result);
        }
        private static String convertSetToString(Set<String> persons_to_be_recommended){//for converting the Set into String
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
   /*
    This mapper ignores the key and maps every value to every other value and emits if the value is non-negative
   */
   public static class AllPairsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(values.toString());
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));
            ArrayList<Integer> friends = new ArrayList<>();
            IntWritable friend1 = new IntWritable();
            while (st.hasMoreTokens()) {
              
                Integer friend = Integer.parseInt(st.nextToken());
                if(friend>0)//maps every value to every other value and emits if the value is non-negative
                friends.add(friend);
                else
                {IntWritable existing_friend=new IntWritable();//if negative,emits to show an existing friend is already there for this person
                existing_friend.set(friend);
                context.write(user,existing_friend);
                }

            }
        
            ArrayList<Integer> seenFriends = new ArrayList<>();
            IntWritable friend2 = new IntWritable();
            for (Integer friend : friends) {
                friend1.set(friend);
                for (Integer seenFriend : seenFriends) {
                    friend2.set(seenFriend);
                    context.write(friend1, friend2);//emits all possible pair of friends
                    context.write(friend2, friend1);
                }
                seenFriends.add(friend1.get());
            }
        }
    }

    public static class RecommenderReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        private static class Recommendation {//includes functions used in the reducer

          
            private int friendId;
            private int nCommonFriends;

          
            public Recommendation(int friendId) {
                this.friendId = friendId;
                this.nCommonFriends = 1;
            }

       
            public int getFriendId() {
                return friendId;
            }

            public int getNCommonFriends() {
                return nCommonFriends;
            }

         
            public void addCommonFriend() {
                nCommonFriends++;
            }

             
            public String toString() {
                return friendId + "(" + nCommonFriends + ")";
            }
            public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
                for (Recommendation p : recommendations) {
                    if (p.getFriendId() == friendId) {
                        return p;
                    }
                }
                return null;
            }
        }
  
        /*
          THis reducer creates an array, counts the number of common friends, sorts and gives the people to follow after deleting  the ones
          which are aleady followed
        */ 
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable user = key;
            ArrayList<Integer> existingFriends = new ArrayList();
            ArrayList<Integer> recommendedUsers = new ArrayList<>();
            while (values.iterator().hasNext()) {
                int value = values.iterator().next().get();
                if (value > 0) {
                    recommendedUsers.add(value);
                } else {
                    existingFriends.add(value);
                }
            }
  
            for (Integer friend : existingFriends) {
                recommendedUsers.removeIf(new Predicate<Integer>() {
                    @Override
                    public boolean test(Integer t) {
                        return t.intValue() == -friend.intValue();
                    }
                });
            }
             ArrayList<Recommendation> recommendations = new ArrayList<>();
             for (Integer userId : recommendedUsers) {
                Recommendation p = Recommendation.find(userId, recommendations);
                if (p == null) {
                    recommendations.add(new Recommendation(userId));
                } else {
                    p.addCommonFriend();
                }
            }
            recommendations.sort(new Comparator<Recommendation>() {
                @Override
                public int compare(Recommendation t, Recommendation t1) {
                    return -Integer.compare(t.getNCommonFriends(), t1.getNCommonFriends());
                }
            });
            StringBuffer sb = new StringBuffer("");
            for (int i = 0; i < recommendations.size() && i < 10; i++) {
                Recommendation p = recommendations.get(i);
                sb.append(p.toString() + " ");
            }
            Text result = new Text(sb.toString());
            context.write(user, result);
        }
    }



    // Main method
    public static void main(String[] args) throws Exception {
     
      String inputDir = "/home/nids/WTFocomb/input.txt";
      String outputTempDir = "/home/nids/WTFocomb/outputTemp";
      String outputFinalDir = "/home/nids/WTFocomb/outputFinal";
      Configuration conf = new Configuration();
      Job job1 = Job.getInstance(conf, "JOB_1");
      job1.setJarByClass(WhoToFollow.class);
      job1.setMapperClass(InverterMapper.class);
      job1.setReducerClass(InvertReducer1.class);
      job1.setOutputKeyClass(Text.class);
      job1.setOutputValueClass(Text.class);
      FileInputFormat.addInputPath(job1, new Path(inputDir));
      FileOutputFormat.setOutputPath(job1, new Path(outputTempDir));

      boolean success = job1.waitForCompletion(true);
      if (success) {
      Job job2 = Job.getInstance(conf, "JOB_2");
      job2.setJarByClass(WhoToFollow.class);
      job2.setJarByClass(WhoToFollow.class);
      job2.setMapperClass(AllPairsMapper.class);
      job2.setReducerClass(RecommenderReducer.class);
      job2.setOutputKeyClass(IntWritable.class);
      job2.setOutputValueClass(IntWritable.class);
      FileInputFormat.addInputPath(job2, new Path(outputTempDir));
      FileOutputFormat.setOutputPath(job2, new Path(outputFinalDir));
      success = job2.waitForCompletion(true);
}

    }
}
