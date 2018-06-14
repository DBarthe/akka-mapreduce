package mapreduce;

import akka.actor.AbstractActor;

import java.util.HashMap;
import java.util.Map;

public class Reducer extends AbstractActor {
  
  public static class WordCountMessage {
    String word;
    int count;
  
    public WordCountMessage(String word, int count) {
      this.word = word;
      this.count = count;
    }
  }
  
  public static class GetCountMessage {
    String word;
  
    public GetCountMessage(String word) {
      this.word = word;
    }
  }

  private Map<String, Integer> wordCountMap = new HashMap<>();
  
  @Override
  public Receive createReceive() {
    return receiveBuilder()
      .match(WordCountMessage.class, this::processWordCountMessage)
      .match(GetCountMessage.class, this::processGetCountMessage)
      .build();
  }
  
  private void processGetCountMessage(GetCountMessage m) {
    int count = wordCountMap.getOrDefault(m.word, 0);
    System.out.println(String.format("#%s Total for word %s is %d", getSelf().path(), m.word, count));
  }
  
  private void processWordCountMessage(WordCountMessage m) {
    System.out.println(String.format("#%s received line #%s : %d", getSelf().path(), m.word, m.count));
    wordCountMap.put(m.word, wordCountMap.getOrDefault(m.word, 0) + m.count);
  }
}
