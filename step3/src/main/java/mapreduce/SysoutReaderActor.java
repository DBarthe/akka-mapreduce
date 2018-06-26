package mapreduce;

public class SysoutReaderActor extends ReaderActor {
    @Override
    protected void writeWordCount(Messages.WordCount m) {
        log.info("word = {} count = {}", m.getWord(), m.getCount());
    }
}
