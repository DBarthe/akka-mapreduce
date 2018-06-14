package mapreduce;

import akka.actor.*;
import akka.remote.RemoteScope;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Master extends AbstractActor {
    
    private String namedPipe;
    private List<Address> remoteAddresses;
    private int mapperCount;
    private int reducerCount;
    private ArrayList<ActorRef> mappers = new ArrayList<>();
    private ArrayList<ActorRef> reducers = new ArrayList<>();
    
    public Master(String namedPipe, List<Address> remoteAddresses, int mapperCount, int reducerCount) {
        this.namedPipe = namedPipe;
        this.remoteAddresses = remoteAddresses;
        this.mapperCount = mapperCount;
        this.reducerCount = reducerCount;
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }
    
    @Override
    public void preStart() throws IOException {
        initializeReducers();
        initializeMappers();
        processNamedPipe();
    }
    
    /**
     * Initialize all the reducers spread on all remote systems
     */
    private void initializeReducers() {
        reducers = new ArrayList<>(reducerCount);
        IntStream.range(0, reducerCount).forEach(n -> {
            Address address = remoteAddresses.get(n % remoteAddresses.size());
            String name = String.format("reducer-%d", n);
            ActorRef ref = getContext().actorOf(Props.create(Reducer.class)
                    .withDeploy(new Deploy(new RemoteScope(address))), name);
            reducers.add(ref);
        });
    }
    
    /**
     * Initialize all the mappers spread on all remote systems
     */
    private void initializeMappers() {
        mappers = new ArrayList<>(mapperCount);
        IntStream.range(0, mapperCount).forEach(n -> {
            Address address = remoteAddresses.get(n % remoteAddresses.size());
            String name = String.format("mapper-%d", n);
            ActorRef ref = getContext().actorOf(Props.create(Mapper.class, reducers)
                    .withDeploy(new Deploy(new RemoteScope(address))), name);
            mappers.add(ref);
        });
    }
    
    private void processNamedPipe() throws IOException {
        while (true) {
            processFile(namedPipe);
        }
    }
    
    private void processFile(String fileName) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(fileName));
        Stream<String> stream = in.lines();
        final AtomicInteger lineNum = new AtomicInteger(0);
        stream.forEach(lineContent -> {
            int mapperIndex = lineNum.get() % mapperCount;
            mappers.get(mapperIndex).tell(new Mapper.LineMessage(lineNum.getAndIncrement(), lineContent), getSelf());
        });
        in.close();
    }
}
