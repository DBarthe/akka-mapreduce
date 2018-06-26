package mapreduce;

import akka.actor.ActorRef;
import akka.japi.pf.ReceiveBuilder;
import mapreduce.Messages.*;

public abstract class ReaderActor extends WorkerActor {
    
    @Override
    protected ReceiveBuilder completeReceive(ReceiveBuilder builder) {
        return builder
                .match(ReadWordCount.class, this::readWordCount)
                .match(WordCount.class, this::writeWordCount);
    }
    
    @Override
    protected Register getRegisterMessage() {
        return new RegisterReader();
    }
    
    private void readWordCount(ReadWordCount m) {
        String word = m.getWord();
        if (word.length() == 0) {
            return ;
        }
    
        long token = MurmurHash.hash64(word);
        int partition = getPartitioner().getPartitionIndex(token);
        ActorRef reducer = getTokenRing().getRing().get(partition);
        reducer.tell(m, self());
    }
    
    protected abstract void writeWordCount(WordCount m);
    
}

