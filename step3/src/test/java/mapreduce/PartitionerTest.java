package mapreduce;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PartitionerTest {
    
    @Test
    public void testExact() {
        
        int n = 6;
        
        Partitioner partitioner = new Partitioner(n);
        
        assertEquals(-9223372036854775808L, (long)partitioner.tokensBounds.get(0));
        assertEquals(-6148914691236517206L, (long)partitioner.tokensBounds.get(1));
        assertEquals(-3074457345618258604L, (long)partitioner.tokensBounds.get(2));
        assertEquals(-2L, (long)partitioner.tokensBounds.get(3));
        assertEquals(3074457345618258600L, (long)partitioner.tokensBounds.get(4));
        assertEquals(6148914691236517202L, (long)partitioner.tokensBounds.get(5));
    }

    @Test
    public void testRange() {
        
        int n = 20;
        
        Partitioner partitioner = new Partitioner(n);

        Random random = new Random();
    
        for (int i = 0; i < 1000; i++) {
            int partition = partitioner.getPartitionIndex(random.nextLong());
            assertTrue(partition >= 0);
            assertTrue(partition < n);
        }
    }
    
    @Test
    public void testGet() {
        
        int n = 6;
        
        Partitioner partitioner = new Partitioner(n);
        
        
        assertEquals(0, partitioner.getPartitionIndex(-9223372036854775808L));
        assertEquals(0, partitioner.getPartitionIndex(-9223372036854775807L));
        assertEquals(0, partitioner.getPartitionIndex(-6148914691236517207L));
        assertEquals(1, partitioner.getPartitionIndex(-6148914691236517206L));
        assertEquals(3, partitioner.getPartitionIndex(0));
        assertEquals(4, partitioner.getPartitionIndex(6148914691236517201L));
        assertEquals(5, partitioner.getPartitionIndex(6148914691236517208L));
    }
    
    
}