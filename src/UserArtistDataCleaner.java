import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericsUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author juancarrillo
 */
public class UserArtistDataCleaner {

    private static final int ARTIST_ID = 0;
    private static final int ARTIST_SHA = 1;

    private final static IntWritable number = new IntWritable(0);

     static class ArtistUserDictionaryMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
         @Override
         protected void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
             String[] data = line.toString().split("\t");

            context.write(new IntWritable(NumberUtils.toInt(data[ARTIST_ID])), new Text(data[ARTIST_SHA]));
         }
    }

     static class ArtistUserDictionaryReducer extends Reducer<Text, IntWritable, LongWritable, Text> {

         @Override
         protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws InterruptedException {
             IntWritable value = values.iterator().next();
             try {
                 context.write(new LongWritable(value.get()), key);
             } catch (IOException e) {
                 log.error(e.getMessage());
             }
         }
     }

    public static void setArtistDictionary(Configuration conf, Path artistPath) throws IOException {
        Map<Integer, String> dictionary = new HashMap<>();

        FileSystem fs = FileSystem.get(artistPath.toUri(), conf);
        FileStatus[] outputFiles = fs.globStatus(new Path(artistPath, "file*"));
        for (FileStatus fileStatus : outputFiles) {
            SequenceFile.Reader.Option filePath = SequenceFile.Reader.file(fileStatus.getPath());
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, filePath);

            IntWritable key = new IntWritable();
            Text value = new Text();
            while (reader.next(key, value)) {
                dictionary.put(key.get(), value.toString());
            }
        }
        DefaultStringifier<Map<Integer, String>> mapStringifier = new DefaultStringifier<>(
                conf, GenericsUtil.getClass(dictionary));
        conf.set("dictionary", mapStringifier.toString(dictionary));
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        setArtistDictionary(conf, new Path("input/artist/"));

        Job job = Job.getInstance(conf);
        job.setJarByClass(UserArtistDataCleaner.class);
        job.setMapperClass(UserArtistDataCleaner.ArtistUserDictionaryMapper.class);
        //job.setReducerClass(UserArtistDataCleaner.ArtistUserDictionaryReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileSystem.get(conf).delete(new Path(args[1]),true);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static Configuration CreateNewConfiguration() {
        Configuration conf = new Configuration();

        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.output.compression.type", "BLOCK");
        conf.set("io.serializations",
                "org.apache.hadoop.io.serializer.JavaSerialization,"
                        + "org.apache.hadoop.io.serializer.WritableSerialization");
        return conf;
    }

    public static Log log = LogFactory.getLog(UserArtistDataCleaner.class);

}
