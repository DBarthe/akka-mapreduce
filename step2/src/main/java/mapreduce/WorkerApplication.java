package mapreduce;

import akka.actor.*;
import akka.remote.RemoteScope;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.Properties;
import java.util.stream.IntStream;

public class WorkerApplication {

  public static void main(String[] args) {
  
    // override port configuration
    String port = args[0];
    Properties properties = new Properties();
    properties.setProperty("akka.remote.netty.tcp.port", port);
    Config overrides = ConfigFactory.parseProperties(properties);
    Config config = overrides.withFallback(ConfigFactory.load("worker"));
  
    // create the worker system
    ActorSystem system = ActorSystem.create("WorkerSystem", config);
  }
}
