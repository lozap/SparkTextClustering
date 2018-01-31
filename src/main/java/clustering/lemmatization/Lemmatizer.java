package clustering.lemmatization;

import opennlp.tools.lemmatizer.SimpleLemmatizer;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.util.MarkableFileInputStreamFactory;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import org.apache.http.client.entity.InputStreamFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Semen on 30.01.2018.
 */
public class Lemmatizer {
    private static POSTaggerME tagger;


    private void init() {

        try {

            InputStream is = getClass().getResourceAsStream("/models/en-pos-maxent.bin");
            tagger = new POSTaggerME(new POSModel(is));
            is.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    private static SimpleLemmatizer lemmatizer;


    private String lemmatize(String word, String postag) throws IOException {

        if (lemmatizer == null) {
            InputStream is = getClass().getResourceAsStream("/models/en-lemmatizer.dict");
            lemmatizer = new SimpleLemmatizer(is);
            is.close();
        }

        String lemma = lemmatizer.lemmatize(word, postag);
        return lemma;

    }

    public ArrayList<String> getWords(String fileName) {

        if (tagger == null) {
            init();
        }


        ArrayList<String> result = new ArrayList<>();

        try {
            MarkableFileInputStreamFactory isf = new MarkableFileInputStreamFactory(new File(fileName));
            ObjectStream<String> lineStream = new PlainTextByLineStream(isf, "UTF-8");
            String line;
            while ((line = lineStream.read()) != null) {

                String[] tokens = SimpleTokenizer.INSTANCE.tokenize(line);//get tokens
                String[] tags = tagger.tag(tokens);//get part of speech

                POSSample sample = new POSSample(tokens, tags);
                for (int i = 0; i < sample.getSentence().length; i++) {
                    String word = sample.getSentence()[i].toLowerCase();
                    String tag = sample.getTags()[i];
                    word = lemmatize(word, tag);
                    result.add(word);

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return result;

    }
}
