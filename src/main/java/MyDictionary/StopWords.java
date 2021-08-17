package MyDictionary;

import java.io.*;
import java.net.URISyntaxException;

public class StopWords {
    private String[] stopWords;

    public StopWords() throws FileNotFoundException {
        int numWords = 1;
        BufferedReader reader;
        try {
            InputStream in = getClass().getResourceAsStream("/stopWords.txt");
            reader = new BufferedReader(new InputStreamReader(in));
            //reader = new BufferedReader(new FileReader("stopWords.txt"));
            String line = reader.readLine();
            while (line != null) {
                // read next line
                line = reader.readLine();
                numWords++;
            }
            stopWords = new String[numWords];
            in = getClass().getResourceAsStream("/stopWords.txt");
            reader = new BufferedReader(new InputStreamReader(in));
            //reader = new BufferedReader(new FileReader("stopWords.txt"));
            line = reader.readLine();
            for(int i = 0; i<numWords; i++) {
                // read next line and put in string array
                line = reader.readLine();
                stopWords[i] = line;
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean isStop(String word){
        for(int i=0; i<stopWords.length;i++){
            if(word.equals(stopWords[i]))
                return true;
        }
        return false;
    }
}
