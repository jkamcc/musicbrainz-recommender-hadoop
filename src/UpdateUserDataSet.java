import com.google.common.base.Stopwatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericsUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author juancarrillo
 */
public class UpdateUserDataSet {

    public static void createDictionaryFile(Configuration conf, Path inputPath, Path outputPath) throws IOException {
        Map<String, Integer> dictionary = new HashMap<>();

        FileSystem fs = FileSystem.get(inputPath.toUri(), conf);
        FileStatus[] outputFiles = fs.globStatus(new Path(inputPath, "part-*"));
        for (FileStatus fileStatus : outputFiles) {
            SequenceFile.Reader.Option filePath = SequenceFile.Reader.file(fileStatus.getPath());
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, filePath);

            //writer
            SequenceFile.Writer.Option outputOption = SequenceFile.Writer.file(outputPath);
            SequenceFile.Writer writer = SequenceFile.createWriter(conf, outputOption,
                    SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class));

            IntWritable key = new IntWritable();
            Text value = new Text();
            while (reader.next(key, value)) {
                dictionary.put(value.toString(), key.get());
                writer.append(key, value);
            }
            writer.close();
        }
    }

    private static void loadDictionaryFile(Configuration conf, Path path, String name) throws IOException {
        Map<String, Integer> dictionary = new HashMap<>();

        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FileStatus[] outputFiles = fs.globStatus(new Path(path, name));
        for (FileStatus fileStatus : outputFiles) {
            SequenceFile.Reader.Option filePath = SequenceFile.Reader.file(fileStatus.getPath());
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, filePath);

            IntWritable key = new IntWritable();
            Text value = new Text();
            while (reader.next(key, value)) {
                dictionary.put(value.toString(), key.get());
            }
        }

        DefaultStringifier<Map<String, Integer>> mapStringifier = new DefaultStringifier<>(
                conf, GenericsUtil.getClass(dictionary));
        conf.set(name, mapStringifier.toString(dictionary));
    }

    public static void createDictionary(Configuration conf, Path inputPath, Path outputPath, Path dictionaryPath)
            throws Exception {

        Job job = Job.getInstance(conf);
        job.setJarByClass(ArtistDataCleanerMapReduce.class);
        job.setMapperClass(ArtistDataCleanerMapReduce.Mapper.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);

        createDictionaryFile(conf, outputPath, dictionaryPath);
    }

    public static void cleanUserData(Configuration conf, Path inputPath, Path outputPath,
            Path artistDictionaryPath, Path userDictionaryPath) throws Exception {

        loadDictionaryFile(conf, artistDictionaryPath, "artist-dict");
        loadDictionaryFile(conf, userDictionaryPath, "user-dict");

        Job job = Job.getInstance(conf);
        job.setJarByClass(UserLogsArtistCleanerMapReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(UserLogsArtistCleanerMapReducer.Mapper.class);
        job.setReducerClass(UserLogsArtistCleanerMapReducer.Reducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {

        Stopwatch timer = new Stopwatch();
        timer.start();

        Configuration conf = CreateNewConfiguration();
        /*
        UpdateUserDataSet.createDictionary(
                conf, new Path("input/artist_input"), new Path("output/"), new Path("input/artist/artist-dict"));
        UpdateUserDataSet.createDictionary(
                conf, new Path("input/user_input"), new Path("output/"), new Path("input/user/user-dict"));
        */
        String baseFolder = args[0];
        log.info("baseFolder = " + baseFolder);
        UpdateUserDataSet.loadDictionaryFile(conf, new Path(baseFolder+"input/artist/"), "artist-dict");
        UpdateUserDataSet.loadDictionaryFile(conf, new Path(baseFolder+"input/user/"), "user-dict");

        UpdateUserDataSet.cleanUserData(
                conf, new Path(baseFolder+"input/data/"),  new Path(baseFolder+"output/"), new Path(baseFolder+"input/artist/"), new Path(baseFolder+"input/user/"));

        log.info("Total running time: " + timer.stop());
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

    public static Log log = LogFactory.getLog(UpdateUserDataSet.class);

}
