package mapreduce;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.io.*;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Master extends AbstractActor {
    
    private String namedPipe;
    private int mapperCount;
    private int reducerCount;
    private ArrayList<ActorRef> mappers;
    private ArrayList<ActorRef> reducers;
    
    public Master(String namedPipe, int mapperCount, int reducerCount) {
        this.namedPipe = namedPipe;
        this.mapperCount = mapperCount;
        this.reducerCount = reducerCount;
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }
    
    @Override
    public void preStart() throws IOException {
        initializeMappers();
        initializeReducers();
        processNamedPipe();
    }
    
    private void initializeMappers() {
        mappers = new ArrayList<>(mapperCount);
        IntStream.range(0, mapperCount).forEach(n -> {
            mappers.add(n, getContext().actorOf(Props.create(Mapper.class, reducerCount), String.format("mapper-%d", n)));
        });
    }
    
    private void initializeReducers() {
        reducers = new ArrayList<>(reducerCount);
        IntStream.range(0, reducerCount).forEach(n -> {
            reducers.add(n, getContext().actorOf(Props.create(Reducer.class), String.format("reducer-%d", n)));
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
