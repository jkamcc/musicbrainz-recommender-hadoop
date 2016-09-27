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

    public static void createArtistDictionaryFile(Configuration conf, Path inputPath, Path outputPath) throws IOException {
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

    private static void loadArtistDictionaryFile(Configuration conf, Path path) throws IOException {
        Map<String, Integer> dictionary = new HashMap<>();

        FileSystem fs = FileSystem.get(path.toUri(), conf);
        FileStatus[] outputFiles = fs.globStatus(new Path(path, "artist-dict"));
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
        conf.set("dictionary", mapStringifier.toString(dictionary));
    }

    public static void createArtistDictionary(Configuration conf, Path inputPath, Path outputPath, Path dictionaryPath)
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

        createArtistDictionaryFile(conf, outputPath, dictionaryPath);
    }

    public static void cleanUserData(Configuration conf, Path inputPath, Path outputPath, Path artistDictionaryPath)
            throws Exception {

        loadArtistDictionaryFile(conf, artistDictionaryPath);

        Job job = Job.getInstance(conf);
        job.setJarByClass(UserLogsArtistCleanerMapReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(UserLogsArtistCleanerMapReducer.Mapper.class);
        job.setReducerClass(UserLogsArtistCleanerMapReducer.Reducer.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileSystem.get(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = CreateNewConfiguration();
        /*
        Path artistDictionaryPath = new Path("input/artist/artist-dict");
        UpdateUserDataSet.createArtistDictionary(
                conf, new Path("input/artist_input"), new Path("output/"), artistDictionaryPath);
        UpdateUserDataSet.loadArtistDictionaryFile(conf, new Path("input/artist/"));
        */

        UpdateUserDataSet.cleanUserData(conf, new Path("input/data/"),  new Path("output/"), new Path("input/artist/"));
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

}
