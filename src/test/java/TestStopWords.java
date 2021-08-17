import MyDictionary.StopWords;

import java.io.FileNotFoundException;

public class TestStopWords {

    public static void main(String[] args) throws FileNotFoundException {
        StopWords testStopWords = new StopWords();
        System.out.println(testStopWords.isStop("בין")); //works
        String s = "***";
        System.out.println(s);
        System.out.println(s.compareTo("*א"));

    }
}
