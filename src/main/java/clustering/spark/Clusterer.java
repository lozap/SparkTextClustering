package clustering.spark;

import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by Semen on 30.01.2018.
 */
public class Clusterer {
    private KMeans kMeans;


    public Clusterer(int k, int seed, String featuresColumn, String predictedColumn) {
        this.kMeans = new KMeans()
                                .setK(k)
                                .setSeed(seed)
                                .setMaxIter(1000)
                                .setFeaturesCol(featuresColumn)
                                .setPredictionCol(predictedColumn);
    }

    public KMeansModel fitting(Dataset<Row> dataset){
        return this.kMeans.fit(dataset);
    }

    public double calculatingErrors(Dataset<Row> dataset) {
        return fitting(dataset).computeCost(dataset);
    }
}
