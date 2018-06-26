# Map Reduce

Step 3 - Clustering


## Architecture

Cette partie du TP utilise la notion de TokenRing empruntée à la base de données distribuée Cassandra.

L'objectif étant de pouvoir ajouter ou supprimer des reducers à la volée tout en concervant une cohérence et un équilibrage des données.

Pour cela les mots sont hashés sur 64 bits par la classe `MurmurHash`.

L'essemble des hash possibles est divisé en N intervals, c'est à dire N partitions, par la classe `Partitionner`.

Ensuite, la classe `TokenRing` gère quel reducer a la charge de quelles partitions.

L'acteur maître (`MasterActor`) maintient la cohérence de cet objet, et le diffuse à tous les mappers et reducers via le message `TokenRingState`.

Le premier reducer qui rejoint le cluster obtient la charge de toutes les partitions.
Lorsqu'un reducer rejoint le cluster, il va décharger les reducers existants de quelques partitions de sorte à garder un équilibre optimal.
Le noeud master coordine les transfères de partitions via les messages `PartitionTransferOrder`.
Les reducers concernés gère eux mêmes l'execution de ce transfère via le message `PartitionTransferExecution`.


L'arrivée d'un nouvel acteur dans le cluster est géré par un handshake en 3 étapes :
* worker -> `Hello` -> master
* master -> `welcome` -> worker, en transférant l'état du TokenRing et le nombre de partitions
* worker -> `Register` -> master, en précisant si il devient un mapper ou un reducer

Si le worker devient un reducer, le handshake est suivi d'une redistribution de partitions et une modification du TokenRing.

Lorsqu'un reducer quitte le cluster, ses partitions sont redistribuées aux reducers restant de la même manière.

Au démarrage du master, on peut décider le nombre de partitions :
* un grand nombre permet un meilleur équilibrage des reducers
* un petit nombre induit un overhead cpu et io plus petit


## Build et tests

```
mvn package
```

## Execution

(Il va falloir ouvrir plusieurs terminaux)

Initialiser le cluster and lancer un acteur `master` et un acteur `injector` dans la même JVM, avec 30 partitions :
```
mkfifo fifo
mvn compile exec:java -Dexec.mainClass="mapreduce.MasterApplication" -Dexec.args="30 fifo"
```

Lancer un noeud `worker` contenant un seul acteur `mapper`, écoutant sur un port aléatoire :
```
mvn compile exec:java -Dexec.mainClass="mapreduce.WorkerApplication" -Dexec.args="0 1 0"
```

Lancer un noeud `worker` contenant un seul acteur `reducer`, écoutant sur un port aléatoire :
```
mvn compile exec:java -Dexec.mainClass="mapreduce.WorkerApplication" -Dexec.args="0 0 1"
```

On peut aussi lancer des noeuds `worker` contenant plusieurs `mappers` et plusieurs `reducers` dans la même JVM, par exemple 4 et 6 :
```
mvn compile exec:java -Dexec.mainClass="mapreduce.WorkerApplication" -Dexec.args="0 4 6"
```

Envoyer du texte à traiter :
```
cat README.md > fifo
```
... et observer les traces des noeuds.

Lancer un noeud `reader` pour intérroger les résultats :
```
mvn compile exec:java -Dexec.mainClass="mapreduce.ReaderApplication" -Dexec.args="0"
```
Une fois démarré, entrez des mots sur l'entrée standard.
