package mapreduce;

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import mapreduce.Messages.ReadWordCount;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.stream.IntStream;

public class ReaderApplication {

  public static void main(String[] args) {
  
    String port = args[0];
  
    // override port configuration
    Properties properties = new Properties();
    properties.setProperty("akka.remote.netty.tcp.port", port);
    Config overrides = ConfigFactory.parseProperties(properties);
    Config config = overrides.withFallback(ConfigFactory.load("worker"));
  
    // create the worker system
    ActorSystem system = ActorSystem.create("MapReduceSystem", config);
  
    system.actorOf(Props.create(SysoutReaderActor.class), "reader");
    

    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Please enter words on stdin...");
    in.lines().forEach(line -> {
      String word = line.trim();
      System.out.println(String.format("processing word '%s'", word));
      if (word.length() > 0) {
        system.actorSelection("/user/reader").tell(new ReadWordCount(word), null);
      }
    });
  }
}
