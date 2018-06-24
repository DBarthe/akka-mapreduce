package mapreduce;

import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import mapreduce.Messages.PartitionTransferExecution;
import mapreduce.Messages.PartitionTransferOrder;
import mapreduce.Messages.RegisterReducer;
import mapreduce.Messages.WordCount;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ReducerActor extends WorkerActor {

//  public static class GetCountMessage implements Serializable {
//    String word;
//
//    public GetCountMessage(String word) {
//      this.word = word;
//    }
//  }

  private Map<String, Integer> wordCountMap = new HashMap<>();
  private Map<Integer, Set<String>> indexByPartition = new HashMap<>();

    @Override
    protected ReceiveBuilder completeReceive(ReceiveBuilder builder) {
        return builder
                .match(PartitionTransferOrder.class, this::processTransferOrder)
                .match(PartitionTransferExecution.class, this::processTransferExecution)
                .match(WordCount.class, this::processWordCount);
    }
    
    @Override
    protected Messages.Register getRegisterMessage() {
        return new RegisterReducer();
    }
    
    /**
     * Process a word count message
     */
    private void processWordCount(WordCount wordCount) {
        long token = MurmurHash.hash64(wordCount.getWord());
        int partition = getPartitioner().getPartitionIndex(token);
        ActorRef destReducer = getTokenRing().getRing().get(partition);
        
        // check if we are the correct destination
        if (self() != destReducer) {
            // if not forward the message
            log.info("forwarding word count ({}, {})", wordCount.getWord(), wordCount.getCount());
            destReducer.forward(wordCount, context());
        }
        else {
            // otherwise process the word count here
            log.info("processing new word count ({}, {})", wordCount.getWord(), wordCount.getCount());
            wordCountMap.put(wordCount.getWord(), wordCountMap.getOrDefault(wordCount.getWord(), 0) + wordCount.getCount());
            if (indexByPartition.containsKey(partition)) {
                indexByPartition.get(partition).add(wordCount.getWord());
            }
            else {
                Set<String> set = new HashSet<>();
                set.add(wordCount.getWord());
                indexByPartition.put(partition, set);
            }
        }
    }
    
    /**
     * Transfer the partition to the reducer and free-up local memory
     */
    private void processTransferOrder(PartitionTransferOrder m) {
        log.info("received partition transfer : {}", m.getChange());
        int partition = m.getChange().getPartitionIndex();
        if (indexByPartition.containsKey(partition)) {
            Set<String> partitionSet = indexByPartition.get(partition);
            Map<String, Integer> partitionWordCount = wordCountMap.entrySet().stream()
                    .filter(entry -> partitionSet.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            
            m.getChange().getReceiver().tell(new PartitionTransferExecution(partition, partitionWordCount), self());
            partitionSet.forEach(word -> wordCountMap.remove(word));
            indexByPartition.remove(partition);
        }
    }
    
    /**
     * Received the partition from another reducer
     */
    private void processTransferExecution(PartitionTransferExecution m) {
        log.info("received partition {} from reducer {} ({} word counts)", m.getPartitionIndex(), sender(), m.getWordCount().size());
        m.getWordCount().forEach((word, count) -> {
            wordCountMap.put(word, wordCountMap.getOrDefault(word, 0) + count);
        });
        Set<String> partitionSet = indexByPartition.getOrDefault(m.getPartitionIndex(), new HashSet<>());
        partitionSet.addAll(m.getWordCount().keySet());
        indexByPartition.put(m.getPartitionIndex(), partitionSet);
    }
    
//  private void processGetCountMessage(GetCountMessage m) {
//    int count = wordCountMap.getOrDefault(m.word, 0);
//    System.out.println(String.format("#%s Total for word %s is %d", getSelf().path(), m.word, count));
//  }

}
