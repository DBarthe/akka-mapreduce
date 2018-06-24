package mapreduce;

import akka.actor.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class MasterApplication {

  public static void main(String[] args) {

    String namedPipe = args[0];
    
    ActorSystem system = ActorSystem.create("MapReduceSystem", ConfigFactory.load(("master")));
    system.actorOf(Props.create(MasterActor.class), "master");
    system.actorOf(Props.create(InjectorActor.class, namedPipe), "injector");

    
    //
//    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
//    System.out.println("Please enter words on stdin...");
//    in.lines().forEach(line -> {
////      String word = line.trim();
////      if (word.length() > 0) {
////        int hash = Utils.hashWord(word, nReducer);
////        system.actorSelection(a.path().child(String.format("reducer-%d", hash))).tell(new ReducerActor.GetCountMessage(word), null);
////      }
//    });
  }
}
