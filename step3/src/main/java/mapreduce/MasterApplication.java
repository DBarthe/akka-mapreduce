package mapreduce;

import akka.actor.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class MasterApplication {

  public static void main(String[] args) {
  
    if (args.length < 1) {
      System.out.println("run: mkfifo myNamedPipe\nand: ./program myNamedPipe\nthen: cat /dev/urandom> myNamedPipe (few seconds !!!)");
      System.exit(1);
    }
  
    String namedPipe = args[0];
    
    ActorSystem system = ActorSystem.create("MasterSystem", ConfigFactory.load(("master")));

    ActorRef a = system.actorOf(Props.create(Master.class, namedPipe), "master");
  
    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
    System.out.println("Please enter words on stdin...");
    in.lines().forEach(line -> {
//      String word = line.trim();
//      if (word.length() > 0) {
//        int hash = Utils.hashWord(word, nReducer);
//        system.actorSelection(a.path().child(String.format("reducer-%d", hash))).tell(new Reducer.GetCountMessage(word), null);
//      }
    });
  }
}
