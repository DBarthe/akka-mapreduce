package mapreduce;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;

public class Mapper extends AbstractActor {
    
    public static class LineMessage implements Serializable {
        int num;
        String content;
        
        public LineMessage(int num, String content) {
            this.num = num;
            this.content = content;
        }
    }
    
    /**
     * All the reducers
     */
    private List<ActorRef> reducers;
    
    public Mapper(List<ActorRef> reducers) {
        this.reducers = reducers;
    }
    
    @Override
    public Receive createReceive() {
        System.out.println(String.format("#%s: initialization", getSelf().path()));
        return receiveBuilder()
                .match(LineMessage.class, this::processLine)
                .build();
    }
    
    private void processLine(LineMessage m) {
        System.out.println(String.format("#%s received line #%d : %s", getSelf().path(), m.num, m.content));
        
        countWords(m.content).forEach((word, count) -> {
            int hash = hashWord(word);
            ActorRef reducer = reducers.get(hash);
            Reducer.WordCountMessage message = new Reducer.WordCountMessage(word, Math.toIntExact(count));
            reducer.tell(message, getSelf());
        });
    }
    
    private int hashWord(String word) {
        return Utils.hashWord(word, reducers.size());
    }
    
    private Map<String, Long> countWords(String line) {
        return Arrays.stream(line.split("\\W+"))
                .collect(Collectors.groupingBy(Function.<String>identity(), TreeMap::new, counting()));
    }
}

