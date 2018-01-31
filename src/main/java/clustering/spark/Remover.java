package clustering.spark;

import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by Semen on 30.01.2018.
 */
public class Remover {
    StopWordsRemover remover;

    public Remover(String inputColumn, String outputColumn) {
        this.remover = new StopWordsRemover()
                                            .setInputCol(inputColumn)
                                            .setOutputCol(outputColumn);
    }

    public Dataset<Row> transformingDf(Dataset<Row> df) {
        return this.remover.transform(df);
    }
}
