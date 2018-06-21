package mapreduce;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TokenRing is class that maps partition indexes to reducers references
 */
public class TokenRing<T> implements Serializable {
    
    /**
     * The number of partitions
     */
    private int n;
    
    /**
     * Map a partition index to a reducer
     */
    private List<T> ring;
    
    /**
     * Index the partitions index by reducer
     */
    private Map<T, Set<Integer>> index = new HashMap<>();
    
    /**
     * Create the token ring
     *
     * @param n the number of partitions
     */
    public TokenRing(int n) {
        this.n = n;
        this.ring = new ArrayList<>(n);
    }
    
    /**
     * Initialize the token ring with a set of reducers
     *
     * @param reducerList the list of reducers's actor ref
     */
    public void initialize(List<T> reducerList) {
        int nReducers = reducerList.size();
        for (int i = 0; i < n; i++) {
            affect(i, reducerList.get(i % nReducers));
        }
    }
    
    /**
     * Affect a partition to a reducer, updating the ring as well as the index.
     * It the partition was already affected to some reducer, it will remove the affectation first
     *
     * @param partitionIndex the index of the partition
     * @param reducer        the reducers's actor ref
     */
    private void affect(int partitionIndex, T reducer) {
        T donor = ring.get(partitionIndex);
        if (donor != null) {
            index.get(donor).remove(partitionIndex);
        }
        ring.set(partitionIndex, reducer);
        Set<Integer> partitions = index.computeIfAbsent(reducer, x -> new HashSet<>());
        partitions.add(partitionIndex);
        index.put(reducer, partitions);
    }
    
    /**
     * Change is a subclass holding information about a movement in the token ring, that means a partition being
     * removed from a reducer to be given to another reducer
     *
     * @param <T>
     */
    public static class Change<T> {
        
        private int partitionIndex;
        private T donor;
        private T receiver;
        
        private Change(int partitionIndex, T donor) {
            this.partitionIndex = partitionIndex;
            this.donor = donor;
        }
        
        private void complete(T receiver) {
            this.receiver = receiver;
        }
        
        public int getPartitionIndex() {
            return partitionIndex;
        }
        
        public T getDonor() {
            return donor;
        }
        
        public T getReceiver() {
            return receiver;
        }
    }
    
    
    /**
     * Re-balance the partition distribution across the reducers
     *
     * @return the summary of changes
     */
    public List<Change<T>> rebalance() {
        // Compute the perfect distribution
        int nReducers = index.size();
        int optimum = n / nReducers;
        int remain = n % nReducers;
        
        // gather the list of partition to be redistributed
        Queue<Change<T>> changeQueue = index.entrySet().stream()
                .filter(entry -> entry.getValue().size() > optimum)
                .flatMap(entry -> entry.getValue().stream().limit(entry.getValue().size() - optimum))
                .skip(remain)
                .map(partitionIndex -> new Change<T>(partitionIndex, ring.get(partitionIndex)))
                .collect(Collectors.toCollection(LinkedList::new));
        
        
        // gather the list of potential receiver
        Stream<T> receiverStream = index.entrySet().stream()
                .filter(entry -> entry.getValue().size() < optimum)
                .map(Map.Entry::getKey);
        
        // attribute receiver to partitions
        Stream<Change<T>> changeStream = receiverStream.flatMap(receiver -> {
            List<Change<T>> localChangeList = new ArrayList<>();
            for (int i = 0; i < optimum - index.get(receiver).size() && changeQueue.size() > 0; i++) {
                Change<T> change = changeQueue.poll();
                change.complete(receiver);
                localChangeList.add(change);
            }
            return localChangeList.stream();
        });
        
        // re-affect the partition
        changeStream.forEach(change -> affect(change.getPartitionIndex(), change.getReceiver()));
        
        // return the list of changes
        return changeStream.collect(Collectors.toList());
    }
    
    /**
     * Add a reducer to the ring and re-balance
     *
     * @param reducer the reducer's actor ref
     */
    public List<Change<T>> add(T reducer) {
        index.computeIfAbsent(reducer, x -> new HashSet<>());
        return rebalance();
    }
    
    /**
     * Remove a reducer from the ring and re-balance
     *
     * @param reducer the reducer's actor ref
     */
    public void remove(T reducer) {
        //TODO
    }
}
