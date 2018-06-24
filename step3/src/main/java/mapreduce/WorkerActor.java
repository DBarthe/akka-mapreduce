package mapreduce;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.Terminated;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.pf.ReceiveBuilder;
import mapreduce.Messages.Register;
import mapreduce.Messages.TokenRingState;
import mapreduce.Messages.Welcome;

import java.io.IOException;

import static akka.cluster.ClusterEvent.initialStateAsEvents;

/**
 * Base class for Mapper and Reducer actors.
 * Manage the clustering membership and registration to the master actor.
 *
 * Two methods completeReceive and register must be implemented
 */
public abstract class WorkerActor extends AbstractActor {
    
    protected LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    private Cluster cluster = Cluster.get(getContext().getSystem());
    
    public enum Status {
        ORPHAN, JOINING, JOINED, BOOTSTRAPPED, LEAVING, LEAVED
    }
    
    private TokenRing<ActorRef> tokenRing = null;
    private Partitioner partitioner = null;
    private ActorRef master = null;
    private Status status = Status.ORPHAN;
    
    public TokenRing<ActorRef> getTokenRing() {
        return tokenRing;
    }
    
    public ActorRef getMaster() {
        return master;
    }
    
    public Status getStatus() {
        return status;
    }
    
    public Partitioner getPartitioner() {
        return partitioner;
    }
    
    @Override
    public void preStart() throws IOException {
        cluster.subscribe(getSelf(), initialStateAsEvents(), ClusterEvent.MemberEvent.class, ClusterEvent.UnreachableMember.class);
    }
    
    //re-subscribe when restart
    @Override
    public void postStop() {
        cluster.unsubscribe(getSelf());
    }
    
    @Override
    public Receive createReceive() {
        System.out.println(String.format("#%s: initialization", getSelf().path()));
        ReceiveBuilder builder = receiveBuilder()
                .match(ClusterEvent.MemberUp.class, mUp -> {
                    log.info("Member is Up: {} with roles : {}", mUp.member(), mUp.member().roles());
                    if (mUp.member().hasRole("master")) {
                        hello(mUp.member());
                    }
                })
                .match(Welcome.class, m -> {
                    register(m, sender());
                })
                .match(ClusterEvent.UnreachableMember.class, mUnreachable -> {
                    log.info("Member detected as unreachable: {}", mUnreachable.member());
                })
                .match(ClusterEvent.MemberRemoved.class, mRemoved -> {
                    log.info("Member is Removed: {}", mRemoved.member());
                })
                .match(ClusterEvent.MemberEvent.class, message -> {
                    // ignore
                })
                .match(TokenRingState.class, m -> {
                    tokenRing = m.getTokenRing();
                })
                .match(Terminated.class, terminated -> {
                   if (terminated.getActor() == master) {
                       unregister();
                   }
                });
        
        return completeReceive(builder).build();
    }
    
    private void hello(Member master) {
        log.info("sending hello to master : {}", master);
        getContext().actorSelection(master.address() + "/user/master").tell(new Messages.Hello(), self());
        status = Status.JOINING;
    }
    
    private void register(Welcome welcome, ActorRef master) {
        log.info("received welcome from : {}, registering", master);
        master.tell(getRegisterMessage(), self());
        this.master = master;
        tokenRing = welcome.getTokenRing();
        partitioner = new Partitioner(tokenRing.getN());
        status = Status.JOINED;
        getContext().watch(master);
    }
    
    private void unregister() {
        log.info("unregistering master");
        this.master = null;
        this.status = Status.ORPHAN;
    }
    
    /**
     * Implement this method to append matches to the ReceiveBuilder
     */
    protected abstract ReceiveBuilder completeReceive(ReceiveBuilder builder);
    
    /**
     * This method must return a Register message to send to the master during the 3-way handshake
     */
    protected abstract Register getRegisterMessage();
}
