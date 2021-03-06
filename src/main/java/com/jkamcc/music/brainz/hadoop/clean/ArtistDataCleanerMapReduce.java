package com.jkamcc.music.brainz.hadoop.clean;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author juancarrillo
 */
public class ArtistDataCleanerMapReduce {

    private static final int ARTIST_ID = 0;
    private static final int ARTIST_SHA = 1;

     static class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text> {
         @Override
         protected void map(LongWritable key, Text line, Context context) throws InterruptedException {
             String[] data = line.toString().split("\t");

             try {
                 context.write(new IntWritable(NumberUtils.toInt(data[ARTIST_ID])), new Text(data[ARTIST_SHA]));
             } catch (IOException e) {
                 log.error(e.getMessage());
             }
         }
     }

    public static Log log = LogFactory.getLog(ArtistDataCleanerMapReduce.class);

}
