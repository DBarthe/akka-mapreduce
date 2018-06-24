package mapreduce;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * TokenRing is class that maps partition indexes to nodes
 */
public class TokenRing<T extends Serializable> implements Serializable {
    
    /**
     * The number of partitions
     */
    private int n;
    
    /**
     * Map a partition index to a node
     */
    private List<T> ring;
    
    /**
     * Index the partitions by nodes
     */
    private Map<T, Set<Integer>> index = new HashMap<>();
    
    /**
     * Change is a subclass holding information about a movement in the token ring, that means a partition being
     * removed from a node to be given to another node
     *
     * @param <T>
     */
    public static class Change<T extends Serializable> implements Serializable {
        
        private int partitionIndex;
        private T donor;
        private T receiver;
        
        public Change(int partitionIndex, T donor, T receiver) {
            this.partitionIndex = partitionIndex;
            this.donor = donor;
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
     * Create the token ring
     *
     * @param n the number of partitions
     */
    public TokenRing(int n) {
        this.n = n;
        this.ring = Stream.generate(() -> (T)null).limit(n).collect(Collectors.toList());
    }
    
    public int getN() {
        return n;
    }
    
    List<T> getRing() {
        return ring;
    }
    
    Map<T, Set<Integer>> getIndex() {
        return index;
    }
    
    /**
     * Initialize the token ring with a set of nodes
     *
     * @param nodeList the list of nodes's actor ref
     */
    public void initialize(List<T> nodeList) {
        int nReducers = nodeList.size();
        for (int i = 0; i < n; i++) {
            affect(i, nodeList.get(i % nReducers));
        }
    }
    
    /**
     * Initialize the token ring with one node only
     * @param node
     */
    public void initialize(T node) {
        initialize(Collections.singletonList(node));
    }
    
    /**
     * @return true if the ring has been initialized
     */
    public boolean initialized() {
        return index.size() > 0;
    }
    
    /**
     * Affect a partition to a node, updating the ring as well as the index.
     * It the partition was already affected to some node, it will remove the affectation first
     *
     * @param partitionIndex the index of the partition
     * @param node        the nodes's actor ref
     */
    private void affect(int partitionIndex, T node) {
        T donor = ring.get(partitionIndex);
        if (donor != null) {
            index.get(donor).remove(partitionIndex);
        }
        ring.set(partitionIndex, node);
        Set<Integer> partitions = index.computeIfAbsent(node, x -> new HashSet<>());
        partitions.add(partitionIndex);
        index.put(node, partitions);
    }
    
    /**
     * Sort the index by number of partitions
     * @return the navigable set of the sorted index entries
     */
    private PriorityQueue<Map.Entry<T, Set<Integer>>> indexSort(boolean asc) {
        PriorityQueue<Map.Entry<T, Set<Integer>>> sortedIndex = new PriorityQueue<>(index.size(),
                Comparator.comparingInt(x -> (asc ? 1 : -1) * x.getValue().size()));
        sortedIndex.addAll(index.entrySet());
        return sortedIndex;
    }
    /**
     * Add a node to the ring and re-balance
     *
     * @param node the node to add to the ring
     * @return the changelog
     */
    public List<Change<T>> add(T node) {
        
        // create the changelog to return
        List<Change<T>> changelog = new ArrayList<>();
    
        if (index.size() == 0) {
            initialize(node);
            return changelog;
        }
    
        // stop if the token ring already contains this node
        if (index.containsKey(node)) {
            return changelog;
        }
        
        // Compute the perfect distribution
        int nReducers = index.size() + 1;
        int optimum = n / nReducers;
        
        // sort the index on number of partitions
        PriorityQueue<Map.Entry<T, Set<Integer>>> sortedIndex = indexSort(false);
        for (int i = 0; i < optimum; i++) {
            
            // poll the most occupied node from the sorted index
            assert sortedIndex.size() > 0;
            Map.Entry<T, Set<Integer>> entry = sortedIndex.poll();
            T donor = entry.getKey();
            Set<Integer> partitionSet = entry.getValue();
            
            // Move one partition (just create a Change object)
            assert partitionSet.size() > 0;
            Integer partitionIndex = Utils.removeFirst(partitionSet);
            changelog.add(new Change<>(partitionIndex, donor, node));

            // put back the node in the sorted set
            if (partitionSet.size() > 0) {
                entry.setValue(partitionSet);
                sortedIndex.add(entry);
            }
        }
        
        // Apply the changes in the data-structure
        index.put(node, new HashSet<>());
        changelog.forEach(c -> affect(c.getPartitionIndex(), node));
        
        // return the change log
        return changelog;
        
    }
    
    
    public static void main(String[] args) {
        TokenRing<Integer> ring = new TokenRing<>(10);
        List<Change<Integer>> changes;
        
        List<Integer> initList = new ArrayList<>();
        initList.add(1);
        initList.add(2);
        initList.add(3);
        
        ring.initialize(initList);
        
        changes = ring.add(4);
        changes = ring.add(5);
        changes = ring.add(6);
        
//        changes = ring.remove(6);
//        changes = ring.remove(2);
        System.out.println("toto");
        
        return ;
    }
}
