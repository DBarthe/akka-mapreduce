package mapreduce;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * Partitioner is a class that map 64-bit hashes to a partition indexes.
 * When created, we specify a number of partitions. It then split the 64-bit space into N uniform ranges.
 */
public class Partitioner implements Serializable {
    
    /**
     * The number of partitions
     */
    private int n;
    
    /**
     * The lower bounds of the token rings
     */
    List<Long> tokensBounds;
    
    /**
     * Create the partitioner
     * @param n the number of partitions
     */
    public Partitioner(int n) {
        this.n = n;
        tokensBounds = new ArrayList<>(n);
        generate();
    }
    
    /**
     * Generate the token ring (all the inner bounds)
     */
    private void generate() {
        
        BigInteger up = BigInteger.valueOf(2).pow(64);
        BigInteger low = BigInteger.valueOf(Long.MIN_VALUE);
        
        for (int i = 0; i < n; i++) {
            BigInteger bound = up.divide(BigInteger.valueOf(n)).multiply(BigInteger.valueOf(i)).add(low);
            tokensBounds.add(bound.longValue());
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
