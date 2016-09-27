import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.GenericsUtil;

import java.io.IOException;
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

        Map<String,Integer> dictionary = new HashMap<>();

        @Override
        protected void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            String[] data = line.toString().split("\t");
            String artist = data[ARTIST_SHA];
            try {
                int artistId = dictionary.get(artist);
                String newLine = artistId + "\t" + data[PLAY_COUNT];
                context.write(new Text(data[USER_SHA]), new Text(newLine));
            } catch (Exception e) {
                log.error(line);
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            DefaultStringifier<Map<String, Integer>> mapStringifier
                    = new DefaultStringifier<>(
                    conf, GenericsUtil.getClass(dictionary));
            dictionary = mapStringifier.fromString(conf.get("dictionary"));
        }
    }

    static class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text> {

        private static final int LIMIT = 10;
        private static final int MAX_SCORE = 100;
        private static final int ARTIST_ID = 0;
        private static final int PLAY_COUNT = 1;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<Integer, List<String>> map = new TreeMap<>(Collections.<Integer>reverseOrder());

            for (Text line : values) {
                String[] data = line.toString().split("\t");
                map.put(NumberUtils.toInt(data[PLAY_COUNT]), Arrays.asList(data[ARTIST_ID]));
            }

            int i = 0;
            int maxValue = 0;

            for (Map.Entry<Integer, List<String>> entry : map.entrySet()) {
                int score = 0;
                int value = entry.getKey();
                // max value case
                if (i == 0) {
                    score = 100;
                    maxValue = value;
                } else {
                    score = value * 100 / maxValue;
                }

                List<String> artists = entry.getValue();
                for (String artist : artists) {
                    if (i < 10) {
                        context.write(key, new Text(artist + "\t" + score));
                    }
                    ++i;
                }

                if (i >= 10) {
                    break;
                }
            }
        }


    }

    public static Log log = LogFactory.getLog(UserLogsArtistCleanerMapReducer.class);

}
