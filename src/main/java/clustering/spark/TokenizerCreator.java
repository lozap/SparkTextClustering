package clustering.spark;

import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by Semen on 30.01.2018.
 */
public class TokenizerCreator {
   Tokenizer tokenizer;

    public  TokenizerCreator(String inputColumn, String outputColumn) {
        this.tokenizer = new Tokenizer()
                                        .setInputCol(inputColumn)
                                        .setOutputCol(outputColumn);
    }


    public Dataset<Row> dataFrameTokenization(Dataset<Row> sentenceDataFrame ) {
        return tokenizer.transform(sentenceDataFrame);
    }

//    public String[] getTokens(Dataset<Row> sentenceDataFrame ){
//        return dataFrameTokenization(sentenceDataFrame ).columns();
//    }
}
