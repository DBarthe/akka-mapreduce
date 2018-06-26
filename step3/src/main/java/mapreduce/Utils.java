package mapreduce;


import java.util.Collection;
import java.util.Iterator;

public class Utils {
    /**
     * Remove and return the first element from any collection
     */
    public static <U> U removeFirst(Collection<? extends U> c) {
        Iterator<? extends U> it = c.iterator();
        if (!it.hasNext()) { return null; }
        U removed = it.next();
        it.remove();
        return removed;
    }
}
