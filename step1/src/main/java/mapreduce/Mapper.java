package mapreduce;

import akka.actor.AbstractActor;
import akka.actor.ActorSelection;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.counting;

public class Mapper extends AbstractActor {
    
    public static class LineMessage {
        int num;
        String content;
        
        public LineMessage(int num, String content) {
            this.num = num;
            this.content = content;
        }
    }
    
    /**
     * The number of reducers
     */
    private int reducerCount;
    
    public Mapper(int reducerCount) {
        this.reducerCount = reducerCount;
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(LineMessage.class, this::processLine)
                .build();
    }
    
    private void processLine(LineMessage m) {
        System.out.println(String.format("#%s received line #%d : %s", getSelf().path(), m.num, m.content));
        
        countWords(m.content).forEach((word, count) -> {
            int hash = hashWord(word);
            ActorSelection reducer =  getContext().actorSelection(String.format("../reducer-%d", hash));
            Reducer.WordCountMessage message = new Reducer.WordCountMessage(word, Math.toIntExact(count));
            reducer.tell(message, getSelf());
            getSender().tell(true, getSelf());
        });
    }
    
    private int hashWord(String word) {
        return Utils.hashWord(word, reducerCount);
    }
    
    private Map<String, Long> countWords(String line) {
        return Arrays.stream(line.split("\\W+"))
                .collect(Collectors.groupingBy(Function.<String>identity(), TreeMap::new, counting()));
    }
}

