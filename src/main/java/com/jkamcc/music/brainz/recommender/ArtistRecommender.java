package com.jkamcc.music.brainz.recommender;

import com.google.common.base.Stopwatch;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.common.LongPrimitiveIterator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.CachingUserSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author juancarrillo
 */
public class ArtistRecommender {

    private static DataModel model;
    private static Recommender recommender;

    public ArtistRecommender(DataModel model) throws TasteException {
        this.model = model;

        buildRecommender(0.20);
    }

    public void evaluatePrecisionAndRecall(RecommenderBuilder recommenderbuilder) throws TasteException {
        Stopwatch timer = new Stopwatch();
        timer.start();

        RecommenderIRStatsEvaluator statsEvaluator = new GenericRecommenderIRStatsEvaluator();
        IRStatistics stats = statsEvaluator.evaluate(
                recommenderbuilder, null, model, null, 2, GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 0.001);

        log.info("stats.getPrecision() = " + stats.getPrecision());
        log.info("stats.getRecall() = " + stats.getRecall());
        timer.stop();
        log.info("timer " + timer.elapsedTime(TimeUnit.MINUTES));
    }

    public void evaluateRMSE(RecommenderBuilder recommenderbuilder) throws TasteException {
        Stopwatch timer = new Stopwatch();
        timer.start();
        RecommenderEvaluator evaluator = new RMSRecommenderEvaluator();
        double error = evaluator.evaluate(recommenderbuilder, null, model, 0.7, 0.1);
        log.info("error = " + error);
        timer.stop();
        log.info("timer " + timer.elapsedTime(TimeUnit.MINUTES));
    }

    private void buildRecommender(final double threshold) throws TasteException {
        RecommenderBuilder userBasedRecommender = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                UserSimilarity similarity =
                        new CachingUserSimilarity(new TanimotoCoefficientSimilarity(dataModel), dataModel);
                UserNeighborhood neighborhood =
                        new ThresholdUserNeighborhood(threshold, similarity, dataModel);
                return new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
            }
        };
        recommender = userBasedRecommender.buildRecommender(model);

    }

    public void recommend() {

        List<RecommendedItem> recommendedItems = new ArrayList<>();

        try {
            System.out.println("ArtistRecommender.recommend");
            double threshold = 0.30;
            final int howMany = 2;
            while(recommendedItems.size() < howMany) {
                buildRecommender(threshold);
                List<RecommendedItem> single = recommender.recommend(3973050, howMany);
                log.info("threshold = " + threshold);
                log.info("single.size() = " + single.size());
                recommendedItems.addAll(single);
                threshold -= 0.10;
            }

            if (!recommendedItems.isEmpty()) {
                String query = "SELECT name FROM \"artist\" WHERE (\"artist\".\"id\" IN (";
                String[] ids = new String[recommendedItems.size()];
                int i = 0;
                for (RecommendedItem recommendation : recommendedItems) {
                    ids[i++] = recommendation.getItemID() + "";
                }
                query += StringUtils.join(ids, ",") + ")) ";
                query += "ORDER BY ";
                for (int j = 0; j < ids.length; j++) {
                    ids[j] = "id=" + ids[j] + " DESC";
                }
                query += StringUtils.join(ids, ",");
                query += " LIMIT 10";

                System.out.println(query);

            } else {

            }

        } catch (TasteException e) {
            e.printStackTrace();
        }
    }

    public void calculateUsersWithoutSuggestion() throws TasteException {
        LongPrimitiveIterator it = model.getUserIDs();
        double noRecommendation = 0;
        double i = 0;
        while (it.hasNext()) {
            List<RecommendedItem> items = recommender.recommend(it.nextLong(), 1);
            if (items.isEmpty()) {
                ++noRecommendation;
            }
            ++i;
            double percentage = noRecommendation/i;
            log.info(
                    noRecommendation +
                    " \\ " + i +
                    " \\ " + percentage);
        }
    }

    public static void main(String[] args) throws IOException, TasteException {
        String dataPath = "/Users/juancarrillo/Documents/Dissertation Project/hadoop/input/matrix/data.tsv";
        File data = new File(dataPath);
        DataModel model = new FileDataModel(data);
        ArtistRecommender artistRecommender = new ArtistRecommender(model);
        //artistRecommender.recommend();
        //artistRecommender.calculateUsersWithoutSuggestion();

    }

    private static Log log = LogFactory.getLog(ArtistRecommender.class);
}
