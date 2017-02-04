

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;
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

public class WhoToFollowM2R2 {

    public static class AllPairsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(values.toString());
            IntWritable user = new IntWritable(Integer.parseInt(st.nextToken()));
            ArrayList<Integer> friends = new ArrayList<>();
            IntWritable friend1 = new IntWritable();
            while (st.hasMoreTokens()) {
               
                Integer friend = Integer.parseInt(st.nextToken());
                if(friend>0)
                friends.add(friend); 
                else
                {IntWritable existing_friend=new IntWritable();
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
                    context.write(friend1, friend2);
                    context.write(friend2, friend1);
                }
                seenFriends.add(friend1.get());
            }
        }
    }

    public static class RecommenderReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        private static class Recommendation {

           
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

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Who To Follow Part2");
        job.setJarByClass(WhoToFollowM2R2.class);
        job.setMapperClass(AllPairsMapper.class);
        job.setReducerClass(RecommenderReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

