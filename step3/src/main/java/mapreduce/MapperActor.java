package mapreduce;

import akka.actor.ActorRef;
import akka.cluster.Member;
import akka.japi.pf.ReceiveBuilder;
import mapreduce.Messages.Line;
import mapreduce.Messages.RegisterMapper;
import mapreduce.Messages.WordCount;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MapperActor extends WorkerActor {

    @Override
    protected ReceiveBuilder completeReceive(ReceiveBuilder builder) {
        return builder
                .match(Line.class, this::processLine);
    }
    
    @Override
    protected Messages.Register getRegisterMessage() {
        return new RegisterMapper();
    }
    
    /**
     * Process a line and send word count to reducers
     */
    private void processLine(Line line) {
        System.out.println(String.format("received line #%d : %s", line.getNum(), line.getContent()));

        countWords(line.getContent()).forEach((word, count) -> {
            word = word.trim();
            if (word.length() == 0) {
                return ;
            }
            
            long token = MurmurHash.hash64(word);
            int partition = getPartitioner().getPartitionIndex(token);
            ActorRef reducer = getTokenRing().getRing().get(partition);
            if (reducer == null) {
                log.warning("no reducer for word {} (token: {}, partition: {}), dropping", word, partition, token);
                return ;
            }
            log.info("sending word {} to reducer {} (token: {}, partition: {})", word, reducer, token, partition);
            reducer.tell(new WordCount(word, Math.toIntExact(count)), getSelf());
        });
    }
    
    private Map<String, Long> countWords(String line) {
        return Arrays.stream(line.split("\\W+"))
                .collect(Collectors.groupingBy(Function.identity(), TreeMap::new, Collectors.counting()));
    }
}

