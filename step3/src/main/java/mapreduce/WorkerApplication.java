package mapreduce;

import akka.actor.*;
import akka.remote.RemoteScope;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class WorkerApplication {
    
    public static void main(String[] args) {
        
        String port = args[0];
        int nMappers = Integer.parseInt(args[1]);
        int nReducers = Integer.parseInt(args[2]);
        
        // override port configuration
        Properties properties = new Properties();
        properties.setProperty("akka.remote.netty.tcp.port", port);
        Config overrides = ConfigFactory.parseProperties(properties);
        Config config = overrides.withFallback(ConfigFactory.load("worker"));
        
        // create the worker system
        ActorSystem system = ActorSystem.create("MapReduceSystem", config);
        
        IntStream.range(0, nMappers).forEach(n -> {
            system.actorOf(Props.create(MapperActor.class));
        });
        
        
        IntStream.range(0, nReducers).forEach(n -> {
            system.actorOf(Props.create(ReducerActor.class));
        });
        
    }
}
