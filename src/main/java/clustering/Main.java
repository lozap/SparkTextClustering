package clustering; /**
 * Created by Semen on 30.01.2018.
 */
import clustering.spark.*;
import org.apache.spark.sql.*;

public class Main {
    private static final String fileDir = "C:\\Users\\Semen\\IdeaProjects\\ClusterCreater\\files";
    public static void main(String[] args) {
     Dataset<Row> df = DataFrameCreator.createDataFrame(fileDir);
     Dataset<Row> dfTokenized = new TokenizerCreator("texts", "tokens").dataFrameTokenization(df);
     Dataset<Row> dfWithoutStopWords = new Remover("tokens", "nonStopTokens").transformingDf(dfTokenized);
     Dataset<Row> dfVectorized = new TFIDF("nonStopTokens", "freqTerms", 200).getFeaturesData(dfWithoutStopWords);
     Clusterer clusterer = new Clusterer(3, 123, "freqTerms", "prediction");
     clusterer.fitting(dfVectorized);


    }
}
