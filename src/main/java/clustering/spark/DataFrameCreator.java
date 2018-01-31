package clustering.spark;

import clustering.lemmatization.Lemmatizer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Semen on 30.01.2018.
 */
public class DataFrameCreator {

    private static SparkSession startSession() {
        return SparkSession.builder().getOrCreate();
    }
    private static StructType buildStructure() {
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("text", DataTypes.StringType, false, Metadata.empty())
        });

        return schema;
    }
    private static List<Row> getTexts(String path) {
        Lemmatizer lemmatizer = new Lemmatizer();
        final File folder = new File(path);
        int i = 0;
        List<Row> data = new ArrayList<>();
        for (final File fileEntry : folder.listFiles()) {

            ArrayList<String> words = lemmatizer.getWords(fileEntry.getAbsolutePath());
            data.add(RowFactory.create(i, words));
            i++;
        }
        return  data;
    }

    public static Dataset<Row> createDataFrame(String fromPath) {
        return startSession().createDataFrame(getTexts(fromPath), buildStructure());
    }

    public static Column select(String name, String fromePath) {
        return createDataFrame(fromePath).col(name);
    }
}
