package jessezhang;

import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class InstrumentHighPriceProcessorTest {
    private static InstrumentHighPriceProcessor processor = new InstrumentHighPriceProcessor();
    
    private static final double UNCERTAINTY_DELTA = 0.00000001;

    @Test
    public void testProcessUpdateHighestPrice() throws Exception {
        
        Instrument originalInstrument = new Instrument("TEST1", 100.0, 100.0, 99.0, 98.0, 100.0);
        Instrument transformedInstrument = processor.process(originalInstrument);
        
        Assert.assertEquals(100.0, transformedInstrument.getHighestPrice(), UNCERTAINTY_DELTA);
        Assert.assertEquals(99.0, transformedInstrument.getSecondHighestPrice(), UNCERTAINTY_DELTA); 
    }
    
    @Test
    public void testProcessUpdateSecondHighestPrice() throws Exception {
        
        Instrument originalInstrument = new Instrument("TEST2", 100.0, 100.0, 101.0, 98.0, 100.0);
        Instrument transformedInstrument = processor.process(originalInstrument);
        
        Assert.assertEquals(101.0, transformedInstrument.getHighestPrice(), UNCERTAINTY_DELTA); 
        Assert.assertEquals(100.0, transformedInstrument.getSecondHighestPrice(), UNCERTAINTY_DELTA); 
    }
    
    @Test
    public void testProcessSameHighestPrice() throws Exception {
        
        Instrument originalInstrument = new Instrument("TEST3", 100.0, 101.0, 101.0, 99.0, 102.0);
        Instrument transformedInstrument = processor.process(originalInstrument);
        
        Assert.assertEquals(101.0, transformedInstrument.getHighestPrice(), UNCERTAINTY_DELTA); 
        Assert.assertEquals(99.0, transformedInstrument.getSecondHighestPrice(), UNCERTAINTY_DELTA); 
    }
    
    @Test
    public void testProcessSameSecondHighestPrice() throws Exception {
        
        Instrument originalInstrument = new Instrument("TEST4", 100.0, 99.0, 101.0, 99.0, 102.0);
        Instrument transformedInstrument = processor.process(originalInstrument);
        
        Assert.assertEquals(101.0, transformedInstrument.getHighestPrice(), UNCERTAINTY_DELTA); 
        Assert.assertEquals(99.0, transformedInstrument.getSecondHighestPrice(), UNCERTAINTY_DELTA); 
    }
    
    @Test
    public void testProcessNoUpdate() throws Exception {
        
        Instrument originalInstrument = new Instrument("TEST5", 100.0, 100.0, 102.0, 101.0, 100.0);
        Instrument transformedInstrument = processor.process(originalInstrument);
        
        Assert.assertEquals(102.0, transformedInstrument.getHighestPrice(), UNCERTAINTY_DELTA); 
        Assert.assertEquals(101.0, transformedInstrument.getSecondHighestPrice(), UNCERTAINTY_DELTA); 
    }

}
