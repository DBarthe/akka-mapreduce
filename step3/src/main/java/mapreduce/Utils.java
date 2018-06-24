package mapreduce;

import com.sun.istack.internal.Nullable;

import java.util.Collection;
import java.util.Iterator;

public class Utils {
    public static int hashWord(String word, int reducerCount) {
        return Math.floorMod(word.hashCode(), reducerCount);
    }
    
    /**
     * Remove and return the first element from any collection
     */
    @Nullable
    public static <U> U removeFirst(Collection<? extends U> c) {
        Iterator<? extends U> it = c.iterator();
        if (!it.hasNext()) { return null; }
        U removed = it.next();
        it.remove();
        return removed;
    }
}
