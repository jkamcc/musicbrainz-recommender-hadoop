import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author juancarrillo
 */
public class UserArtistDataCleaner {

    private static final int ARTIST_SHA = 1;

    private final static IntWritable number = new IntWritable(0);

     static class ArtistUserDictionaryMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            String[] data = line.toString().split("\t");

            try {
                context.write(new Text(data[ARTIST_SHA]), new IntWritable(0));
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("data[ARTIST_SHA] = " + data[ARTIST_SHA]);
            }
        }
    }

     static class ArtistUserDictionaryReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, new IntWritable());
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(UserArtistDataCleaner.class);
        job.setMapperClass(UserArtistDataCleaner.ArtistUserDictionaryMapper.class);
        job.setCombinerClass(UserArtistDataCleaner.ArtistUserDictionaryReducer.class);
        job.setReducerClass(UserArtistDataCleaner.ArtistUserDictionaryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(conf).delete(new Path(args[1]),true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
