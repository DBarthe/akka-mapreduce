package mapreduce;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Partitioner is a class that map 64-bit hashes to a partition indexes.
 * When created, we specify a number of partitions. It then split the 64-bit space into N uniform ranges.
 */
public class Partitionner implements Serializable {
    
    /**
     * The number of partitions
     */
    private int n;
    
    /**
     * The lower bounds of the token rings
     */
    private List<Long> tokensBounds;
    
    /**
     * Create the partitioner
     * @param n the number of partitions
     */
    public Partitionner(int n) {
        this.n = n;
        tokensBounds = new ArrayList<>(n);
        generate();
    }
    
    /**
     * Generate the token ring (all the inner bounds)
     */
    private void generate() {
        for (int i = 0; i < n; i++) {
            long bound = ((Long.MAX_VALUE / (long)n) * (long)i) + Long.MIN_VALUE;
            tokensBounds.set(i, bound);
        }
    }
    
    /**
     * Giving a token, return the index of the partition it belongs to
     * @param token a 64-bit integer obtained after hashing something
     * @return the partition index
     */
    public int getPartitionIndex(long token) {
        int index;
        for (index = 0; index < n - 1; index++) {
            long lowerBound = tokensBounds.get(index);
            long upperBound = tokensBounds.get(index + 1);
            if (token >= lowerBound && token < upperBound) {
                return index;
            }
        }
        return index;
    }
}
