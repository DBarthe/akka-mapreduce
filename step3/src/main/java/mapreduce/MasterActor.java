package mapreduce;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import mapreduce.Messages.PartitionTransferOrder;
import mapreduce.Messages.TokenRingState;
import mapreduce.TokenRing.Change;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static mapreduce.Messages.*;

public class MasterActor extends AbstractActor {
    
    LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    
    private List<ActorRef> mappers = new ArrayList<>();
    private List<ActorRef> reducers = new ArrayList<>();
    private List<ActorRef> readers = new ArrayList<>();
    
    private TokenRing<ActorRef> tokenRing;
    
    public MasterActor(int nPartitions) {
        this.tokenRing = new TokenRing<>(nPartitions);
    }
    
    @Override
    public void preStart() throws IOException {
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Hello.class, m -> {
                    welcome(sender());
                })
                .match(RegisterMapper.class, m -> {
                    registerMapper(sender());
                })
                .match(RegisterReducer.class, m -> {
                    registerReducer(sender());
                })
                .match(Terminated.class, terminated -> {
                    if (mappers.contains(terminated.getActor())) {
                        unregisterMapper(terminated.getActor());
                    }
                    else if (reducers.contains(terminated.getActor())) {
                        unregisterReducer(terminated.getActor());
                    }
                    else if (readers.contains(terminated.getActor())) {
                        readers.remove(terminated.getActor());
                    }
                })
                .match(Line.class, this::forwardLine)
                .build();
    }
    
    /**
     * Send a welcome message with the TokenRing to any new worker sending Hello.
     */
    private void welcome(ActorRef sender) {
        log.info("Received hello from {}, sending welcome", sender);
        sender.tell(new Welcome(tokenRing), self());
    }
    
    /**
     * Register the worker as a mapper, watch it in case it goes down
     */
    private void registerMapper(ActorRef mapper) {
        if (mappers.contains(mapper)) {
            log.info("mapper already registered : {}", mapper);
            return ;
        }
    
        log.info("registering mapper : {}", mapper);
        mappers.add(mapper);
        getContext().watch(mapper);
    }
    
    /**
     * Unregister a mapper after it goes down
     */
    private void unregisterMapper(ActorRef mapper) {
        log.info("mapper un-registration : {}", mapper);
        mappers.remove(mapper);
    }
    
    /**
     * Register the worker as a reducer, watch it in case it goes down
     */
    private void registerReducer(ActorRef reducer) {
        if (reducers.contains(reducer)) {
            log.info("reducer already registered : {}", reducer);
            return ;
        }
    
        log.info("reducer registration : {}", reducer);
        getContext().watch(reducer);
        reducers.add(reducer);
        
        List<Change<ActorRef>> changelog = tokenRing.add(reducer);
        propagateTokenRing();
        propagateChanges(changelog);
    }
    
    /**
     * Propagate the new token ring to every workers
     */
    private void propagateTokenRing() {
        mappers.forEach(m -> {
            m.tell(new TokenRingState(tokenRing), self());
        });
        reducers.forEach(m -> {
            m.tell(new TokenRingState(tokenRing), self());
        });
        readers.forEach(m -> {
            m.tell(new TokenRingState(tokenRing), self());
        });
    }
    
    /**
     * After tokenRing changed, send the changes to concerned reducers so they can exchange their data.
     */
    private void propagateChanges(List<Change<ActorRef>> changelog) {
        changelog.forEach(c -> c.getDonor().tell(new PartitionTransferOrder(c), self()));
    }
    
    /**
     * Unregister a reducer after it goes down
     */
    private void unregisterReducer(ActorRef reducer) {
        log.info("reducer un-registration : {}", reducer);
        reducers.remove(reducer);
        tokenRing.remove(reducer);
        propagateTokenRing();
    }
    
    /**
     * Forward a line of text to a mapper
     */
    private void forwardLine(Line line) {
        if (mappers.size() == 0) {
            log.warning("no mapper available, droping line : {}", line.getContent());
        }
        else {
            log.info("forwarding line : {}", line.getContent());
            mappers.get(line.getNum() % mappers.size()).forward(line, getContext());
        }
    }
}
