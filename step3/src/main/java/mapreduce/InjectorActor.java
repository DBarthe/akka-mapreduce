package mapreduce;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class InjectorActor extends AbstractActor {
    
    
    private String namedPipe;
    
    public InjectorActor(String namedPipe) {
        this.namedPipe = namedPipe;
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }
    
    @Override
    public void preStart() throws IOException {
        processNamedPipe();
    }
    
    private void processNamedPipe() throws IOException {
        while (true) {
            processFile(namedPipe);
        }
    }
    
    private void processFile(String fileName) throws IOException {
        ActorSelection master = getContext().actorSelection("/user/master");
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        Stream<String> stream = in.lines();
        final AtomicInteger lineNum = new AtomicInteger(0);
        stream.forEach(lineContent -> {
            master.tell(new Messages.Line(lineNum.getAndIncrement(), lineContent), getSelf());
        });
        in.close();
    }
}
