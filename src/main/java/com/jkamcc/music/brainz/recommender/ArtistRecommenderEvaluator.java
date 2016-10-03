package com.jkamcc.music.brainz.recommender;

import com.google.common.base.Stopwatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.EuclideanDistanceSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.TanimotoCoefficientSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author juancarrillo
 */
public class ArtistRecommenderEvaluator {

    private static DataModel model;
    private static RecommenderEvaluator evaluator;

    public ArtistRecommenderEvaluator(DataModel model) throws IOException, TasteException {

        this.model = model;

        /* Root-Mean Square Error Cross-Validation */
        evaluator = new RMSRecommenderEvaluator();
    }

    public static void main(String[] args) {
        Stopwatch timer = new Stopwatch();
        timer.start();
        log.info("rows "+ 500000);
        try {
            String dataPath = "/Users/juancarrillo/Documents/Dissertation Project/hadoop/input/matrix/small_data.tsv";
            File data = new File(dataPath);
            log.debug("dataPath = " + dataPath);
            DataModel model = new FileDataModel(data);
            ArtistRecommenderEvaluator recommender = new ArtistRecommenderEvaluator(model);
            recommender.evaluateUserBasedNNeighbourhoodResults();
            recommender.evaluateUserBasedThresholdResults();
            recommender.evaluateItemBasedResults();
        } catch (Exception e) {
            log.error(e, e);
        }
        log.debug("Total running time: " + timer.stop());
    }

    public double evaluateData(RecommenderBuilder recommenderBuilder) throws TasteException {

        // Use 70% of the data to train; test using the other 30%.
        return evaluator.evaluate(recommenderBuilder, null, model, 0.7, 1.0);
    }

    public void evaluateUserBasedNNeighbourhoodResults() throws TasteException {
        int[] nsizes = getNeighbourhoodSizes();
        final DecimalFormat f = new DecimalFormat("##.00");
        Stopwatch timer = new Stopwatch();

        List<UserSimilarity> metrics = getUserSimilarityMetrics();

        timer.start();
        for (UserSimilarity similarity : metrics) {
            log.info("Similarity\t" + similarity.getClass().getName());
            for (int nsize : nsizes) {
                timer.reset();
                timer.start();

                Double score = this.evaluateData(
                        getUserBasedWithNNNeighboursEvaluator(similarity, nsize));

                timer.stop();

                log.info(
                        "score\t" + (!score.isNaN()? f.format(score) : "NaN")
                        + "\tn=" + nsize
                        + "\ttime="+timer.elapsedTime(TimeUnit.MINUTES) +" min"
                        +"\t" + similarity.getClass().getName()
                );
            }
        }
    }

    public void evaluateUserBasedThresholdResults() throws TasteException {
        double[] thresholds = getThresholds();
        final DecimalFormat f = new DecimalFormat("##.00");
        Stopwatch timer = new Stopwatch();

        List<UserSimilarity> metrics = getUserSimilarityMetrics();

        timer.start();
        for (UserSimilarity similarity : metrics) {
            log.info("Similarity\t" + similarity.getClass().getName());
            for (double threshold : thresholds) {
                timer.reset();
                timer.start();

                Double score = this.evaluateData(
                        getUserBasedWithThresholdEvaluator(similarity, threshold));

                timer.stop();
                log.info(
                        "score\t" + (!score.isNaN()? f.format(score) : "NaN")
                        + "\tt=" + threshold
                        + "\ttime="+timer.elapsedTime(TimeUnit.MINUTES) + " min"
                        +"\t" + similarity.getClass().getName()
                );
            }
        }
    }

    public void evaluateItemBasedResults() throws TasteException {
        final DecimalFormat f = new DecimalFormat("##.00");
        Stopwatch timer = new Stopwatch();

        List<ItemSimilarity> metrics = getItemSimilarityMetrics();

        timer.start();
        for (ItemSimilarity similarity : metrics) {
            log.info("Similarity\t" + similarity.getClass().getName());
            timer.reset();
            timer.start();

            Double score = this.evaluateData(
                    getItemBasedEvaluator(similarity));

            log.info(
                    "score\t" + (!score.isNaN()? f.format(score) : "NaN")
                    + "\ttime="+timer.stop()
                    +"\t" + similarity.getClass().getName()
            );
        }
    }

    private List<UserSimilarity> getUserSimilarityMetrics() throws TasteException {

        List<UserSimilarity> metrics = new ArrayList<>();

        metrics.add(new PearsonCorrelationSimilarity(model));
        metrics.add(new EuclideanDistanceSimilarity(model));
        metrics.add(new LogLikelihoodSimilarity(model));
        metrics.add(new TanimotoCoefficientSimilarity(model));

        return metrics;
    }

    private List<ItemSimilarity> getItemSimilarityMetrics() throws TasteException {

        List<ItemSimilarity> metrics = new ArrayList<>();

        metrics.add(new PearsonCorrelationSimilarity(model));
        metrics.add(new EuclideanDistanceSimilarity(model));
        metrics.add(new LogLikelihoodSimilarity(model));
        metrics.add(new TanimotoCoefficientSimilarity(model));

        return metrics;
    }

    private RecommenderBuilder getUserBasedWithNNNeighboursEvaluator(
            final UserSimilarity similarity, final int n) throws TasteException {

        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                UserNeighborhood neighborhood =
                        new NearestNUserNeighborhood(n, similarity, dataModel);
                return new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
            }
        };

        return recommenderBuilder;
    }

    private RecommenderBuilder getUserBasedWithThresholdEvaluator(
            final UserSimilarity similarity, final double threshold) throws TasteException {

        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {
                UserNeighborhood neighborhood =
                        new ThresholdUserNeighborhood(threshold, similarity, dataModel);
                return new GenericUserBasedRecommender(dataModel, neighborhood, similarity);
            }
        };

        return recommenderBuilder;
    }

    private RecommenderBuilder getItemBasedEvaluator(final ItemSimilarity similarity) {
        RecommenderBuilder recommenderBuilder = new RecommenderBuilder() {
            @Override
            public Recommender buildRecommender(DataModel dataModel) throws TasteException {

                return new GenericItemBasedRecommender(dataModel, similarity);
            }
        };

        return recommenderBuilder;
    }

    private int[] getNeighbourhoodSizes() {

        int[] nsize = new int[6];

        nsize[0] = 4;
        nsize[1] = 8;
        nsize[2] = 16;
        nsize[3] = 32;
        nsize[4] = 64;
        nsize[5] = 128;

        return nsize;
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

    private static Log log = LogFactory.getLog(ArtistRecommenderEvaluator.class);
}
