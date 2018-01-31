package clustering.spark;

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;


public class TFIDF {
   private HashingTF hashingTF;

    public TFIDF(String inputColumn, String outputColumn, int numFeatures){
        this.hashingTF = new HashingTF()
                                        .setInputCol(inputColumn)
                                        .setOutputCol(outputColumn)
                                        .setNumFeatures(numFeatures);
    }

    public Dataset<Row> getFeaturesData(Dataset<Row> wordsData) {
       return this.hashingTF.transform(wordsData);
    }
}
