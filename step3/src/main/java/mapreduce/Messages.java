package mapreduce;

import akka.actor.ActorRef;
import mapreduce.TokenRing.Change;

import java.io.Serializable;
import java.util.Map;

public class Messages {
    
    /**
     * Three way handshake between workers and master.
     *
     * With the Hello message, the worker announce its existence
     */
    public static class Hello implements Serializable {}
    
    
    /**
     * During the Welcome message, the master send the current TokenRing to the worker
     */
    public static class Welcome implements Serializable {
        private TokenRing<ActorRef> tokenRing;
    
        public Welcome(TokenRing<ActorRef> tokenRing) {
            this.tokenRing = tokenRing;
        }
    
        public TokenRing<ActorRef> getTokenRing() {
            return tokenRing;
        }
    }
    
    /**
     * During the Register massage, the worker explicits which kind of worker it becomes : mapper or reducer or reader
     */
    public static abstract class Register implements Serializable {}
    public static class RegisterMapper extends Register {}
    public static class RegisterReducer extends Register {}
    public static class RegisterReader extends Register {}
    
    /**
     * When the TokenRing change, a TokenRingState is broadcasted to all mappers and reducers
     */
    public static class TokenRingState implements Serializable {
        private TokenRing<ActorRef> tokenRing;
    
        public TokenRingState(TokenRing<ActorRef> tokenRing) {
            this.tokenRing = tokenRing;
        }
    
        public TokenRing<ActorRef> getTokenRing() {
            return tokenRing;
        }
    }
    
    /**
     * When a reducer join or leave, the TokenRing is modified and some partition need to be moved between nodes.
     * This message tells to a reducer it has to send part of its data to another reducer.
     */
    public static class PartitionTransferOrder implements Serializable {
        private Change<ActorRef> change;
    
        public PartitionTransferOrder(Change<ActorRef> change) {
            this.change = change;
        }
    
        public Change<ActorRef> getChange() {
            return change;
        }
    }
    
    /**
     * The data of the partition transferred
     */
    public static class PartitionTransferExecution implements Serializable {
        private int partitionIndex;
        private Map<String, Integer> wordCount;
    
        public PartitionTransferExecution(int partitionIndex, Map<String, Integer> wordCount) {
            this.partitionIndex = partitionIndex;
            this.wordCount = wordCount;
        }
    
        public int getPartitionIndex() {
            return partitionIndex;
        }
    
        public Map<String, Integer> getWordCount() {
            return wordCount;
        }
    }
    
    /**
     * A line to proceed
     */
    public static class Line implements Serializable {
        private int num;
        private String content;
        
        public Line(int num, String content) {
            this.num = num;
            this.content = content;
        }
    
        public int getNum() {
            return num;
        }
    
        public void setNum(int num) {
            this.num = num;
        }
    
        public String getContent() {
            return content;
        }
    
        public void setContent(String content) {
            this.content = content;
        }
    }
    
    /**
     * A word to count
     */
    public static class WordCount implements Serializable {
        private String word;
        private int count;
        
        public WordCount(String word, int count) {
            this.word = word;
            this.count = count;
        }
    
        public String getWord() {
            return word;
        }
    
        public int getCount() {
            return count;
        }
    }
    
    /**
     * Message to ask a word count
     */
    public static class ReadWordCount implements Serializable {
        private String word;
    
        public ReadWordCount(String word) {
            this.word = word;
        }
    
        public String getWord() {
            return word;
        }
    }
}
