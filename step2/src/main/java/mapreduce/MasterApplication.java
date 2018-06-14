package mapreduce;

import akka.actor.*;
import com.typesafe.config.ConfigFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

// TODO: serialization of messages

public class MasterApplication {

  public static void main(String[] args) {
  
    if (args.length < 3) {
      System.out.println(String.format("run: mkfifo myNamedPipe\nand: ./program myNamedPipe n-mappers n-reducer\nthen: cat /dev/urandom> myNamedPipe (few seconds !!!)"));
      System.exit(1);
    }
  
    String namedPipe = args[0];
    int nMappers = Integer.parseInt(args[1]);
    int nReducer = Integer.parseInt(args[2]);
    
    List<Address> remoteAddresses = Arrays.asList(
            AddressFromURIString.parse("akka.tcp://WorkerSystem@127.0.0.1:4001"),
            AddressFromURIString.parse("akka.tcp://WorkerSystem@127.0.0.1:4002"),
            AddressFromURIString.parse("akka.tcp://WorkerSystem@127.0.0.1:4003"),
            AddressFromURIString.parse("akka.tcp://WorkerSystem@127.0.0.1:4004")
    );
    
    ActorSystem system = ActorSystem.create("MasterSystem", ConfigFactory.load(("master")));
    ActorRef a = system.actorOf(Props.create(Master.class, namedPipe, remoteAddresses, nMappers, nReducer), "master");
  
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Please enter words on stdin...");
    in.lines().forEach(line -> {
      String word = line.trim();
      if (word.length() > 0) {
        int hash = Utils.hashWord(word, nReducer);
        system.actorSelection(a.path().child(String.format("reducer-%d", hash))).tell(new Reducer.GetCountMessage(word), null);
      }
    });
  }
}
