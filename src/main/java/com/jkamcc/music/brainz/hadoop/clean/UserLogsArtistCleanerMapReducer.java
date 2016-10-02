package com.jkamcc.music.brainz.hadoop.clean;

import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.GenericsUtil;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;

/**
 * @author juancarrillo
 */
public class UserLogsArtistCleanerMapReducer {

    static class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, Text> {

        private static final int USER_SHA = 0;
        private static final int ARTIST_SHA = 1;
        private static final int ARTIST_NAME = 2;
        private static final int PLAY_COUNT = 3;

        Map<String,Integer> artistDictionary = new HashMap<>();

        @Override
        protected void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            String[] data = line.toString().split("\t");
            String artist = data[ARTIST_SHA];
            try {
                int artistId = artistDictionary.get(artist);
                String newLine = artistId + "\t" + data[PLAY_COUNT];
                context.write(new Text(data[USER_SHA]), new Text(newLine));
            } catch (Exception e) {
                log.error(line);
                context.getCounter("Map", "InvalidLine").increment(1);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            DefaultStringifier<Map<String, Integer>> mapStringifier
                    = new DefaultStringifier<>(
                    conf, GenericsUtil.getClass(artistDictionary));
            artistDictionary = mapStringifier.fromString(conf.get("artist-dict"));
        }
    }

    static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, LongWritable, Text> {

        private static final int LIMIT = 10;
        private static final int MAX_SCORE = 10;
        private static final int ARTIST_ID = 0;
        private static final int PLAY_COUNT = 1;

        Map<String,Integer> userDictionary = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            DefaultStringifier<Map<String, Integer>> mapStringifier
                    = new DefaultStringifier<>(
                    conf, GenericsUtil.getClass(userDictionary));
            userDictionary = mapStringifier.fromString(conf.get("user-dict"));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int userId = 0;

            try {
                userId = userDictionary.get(key.toString());
                if (userId == 0) {
                    throw new Exception ();
                }
            } catch (Exception e) {
                context.getCounter("Map", "UsersNotFound").increment(1);
                log.error(key.toString());
            }

            Map<Integer, List<String>> map = new TreeMap<>(Collections.<Integer>reverseOrder());

            for (Text line : values) {
                String[] data = line.toString().split("\t");
                int playCount = NumberUtils.toInt(data[PLAY_COUNT]);
                if (playCount > 0) {
                    map.put(playCount, Arrays.asList(data[ARTIST_ID]));
                }
            }

            int i = 0;
            double maxValue = 0;
            DecimalFormat f = new DecimalFormat("##.00");

            for (Map.Entry<Integer, List<String>> entry : map.entrySet()) {
                double score = 0;
                double value = entry.getKey();
                // max value case
                if (i == 0) {
                    score = MAX_SCORE;
                    maxValue = value;
                } else {
                    score = value * 10 / maxValue;
                }

                List<String> artists = entry.getValue();
                for (String artist : artists) {
                    if (i < LIMIT) {
                        context.write(new LongWritable(userId), new Text(artist + "\t" + f.format(score)));
                    }
                    ++i;
                }

                if (i >= LIMIT) {
                    break;
                }
            }
        }


    }

    public static Log log = LogFactory.getLog(UserLogsArtistCleanerMapReducer.class);

}
