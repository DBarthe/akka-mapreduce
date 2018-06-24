package mapreduce;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TokenRingTest {
    
    public TokenRing<Integer> createTokenRing() {
        TokenRing<Integer> tokenRing = new TokenRing<>(10);
        List<Integer> initial = new ArrayList<>();
        initial.add(1);
        initial.add(2);
        initial.add(3);
        tokenRing.initialize(initial);
        return tokenRing;
    }
    
    private <T extends Serializable> void checkIntegrity(TokenRing<T> tokenRing) {
        tokenRing.getIndex().forEach((x, set) -> {
            set.forEach(p -> {
                assertEquals(tokenRing.getRing().get(p), x);
            });
        });
        
        tokenRing.getRing().forEach(Assert::assertNotNull);
    }
    
    private <T extends Serializable> void checkBalance(TokenRing<T> tokenRing) {
        int optimum = tokenRing.getN() / tokenRing.getIndex().size();
        tokenRing.getIndex().forEach((x, set) -> assertTrue(set.size() >= optimum && set.size() <= optimum + 1));
    }
    
    private <T extends Serializable>  void checkAll(TokenRing<T> tokenRing, int expectedSize) {
        assertEquals(expectedSize, tokenRing.getIndex().size());
        checkIntegrity(tokenRing);
        checkBalance(tokenRing);
    }
    
    @Test
    public void initializeList() {
        TokenRing<Integer> tokenRing = new TokenRing<>(10);
        List<Integer> initial = new ArrayList<>();
        initial.add(1);
        initial.add(2);
        initial.add(3);
        
        tokenRing.initialize(initial);
    
        checkAll(tokenRing, 3);
    }
    
    @Test
    public void initializeSingleton() {
        TokenRing<Integer> tokenRing = new TokenRing<>(10);
        
        tokenRing.initialize(1);
    
        checkAll(tokenRing, 1);
    }
    
    @Test
    public void add() {
        TokenRing<Integer> tokenRing = createTokenRing();
    
        for (int i = 0; i < 100; i++) {
            tokenRing.add(4 + i);
            checkAll(tokenRing, 4 + i);
        }
    }
    
    
}