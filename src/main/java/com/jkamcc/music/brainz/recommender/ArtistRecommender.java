package com.jkamcc.music.brainz.recommender;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.AverageAbsoluteDifferenceRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;

/**
 * @author juancarrillo
 */
public class ArtistRecommender {

    private static DataModel model;
    private static RecommenderEvaluator evaluator;

    public static void main(String[] args) {
        Stopwatch timer = new Stopwatch();
        timer.start();
        log.info("Similarity\tEuclideanDistanceSimilarity");
        log.info("rows "+ 500000);
        try {
            ClassLoader classLoader = ArtistRecommender.class.getClassLoader();
            //String dataPath = IOUtils.toString(classLoader.getResourceAsStream("intro.csv"));
            String dataPath = "/Users/juancarrillo/Documents/Dissertation Project/hadoop/input/matrix/small_data.tsv";
            File data = new File(dataPath);
            log.debug("dataPath = " + dataPath);
            DataModel model = new FileDataModel(data);
            ArtistRecommender recommender = new ArtistRecommender(model);
            recommender.evaluateThresholdResults();
        } catch (Exception e) {
            log.error(e, e);
        }
        log.debug("Total running time: " + timer.stop());
    }

    public void evaluateThresholdResults() throws TasteException {
        double[] thresholds = getThresholds();
        final DecimalFormat f = new DecimalFormat("##.00");
        Stopwatch timer = new Stopwatch();
        for (double threshold : thresholds) {
            timer.reset();
            timer.start();
            Double score = this.evaluate(threshold);
            log.info(
                    "score\t" + (!score.isNaN()? f.format(score) : "NaN")
                    + "\tt=" + threshold
                    + "\t"+timer.stop()
            );
        }

    }

    private double[] getThresholds() {
        double[] thresholds = new double[5];
        thresholds[0] = 0.95;
        thresholds[1] = 0.90;
        thresholds[2] = 0.85;
        thresholds[3] = 0.80;
        thresholds[4] = 0.75;

        return thresholds;
    }

    public ArtistRecommender(DataModel model) throws IOException, TasteException {

        this.model = model;
        evaluator = new RMSRecommenderEvaluator();
    }

    public double evaluate(final double threshold) throws TasteException {

        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                UserSimilarity similarity = new EuclideanDistanceSimilarity(dataModel);
                UserNeighborhood neighborhood =
                        new ThresholdUserNeighborhood(threshold, similarity, dataModel);
                return new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
            }
        };

        // Use 70% of the data to train; test using the other 30%.
        return evaluator.evaluate(recommenderBuilder, null, model, 0.7, 1.0);
    }

    private static Log log = LogFactory.getLog(ArtistRecommender.class);
}
