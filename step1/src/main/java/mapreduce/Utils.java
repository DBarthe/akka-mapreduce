package mapreduce;

public class Utils {
    public static int hashWord(String word, int reducerCount) {
        return Math.floorMod(word.hashCode(), reducerCount);
    }
}
