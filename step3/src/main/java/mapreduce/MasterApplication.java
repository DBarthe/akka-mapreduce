package mapreduce;

import akka.actor.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MasterApplication {

  public static void main(String[] args) {

    int nPartitions = Integer.parseInt(args[0]);
    String namedPipe = args[1];
    
    ActorSystem system = ActorSystem.create("MapReduceSystem", ConfigFactory.load(("master")));
    system.actorOf(Props.create(MasterActor.class, nPartitions), "master");
    system.actorOf(Props.create(InjectorActor.class, namedPipe), "injector");
  }
}
