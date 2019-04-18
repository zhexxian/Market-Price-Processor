package jessezhang;

import org.junit.Test;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class InstrumentAveragePriceProcessorTest {
    private static int count = 4; //TODO: extract as variable

    private static InstrumentAveragePriceProcessor processor = new InstrumentAveragePriceProcessor(count); 

    @Test
    public void testProcess() throws Exception {
        
        Instrument originalInstrument = new Instrument("TEST1", 100.0, 100.0, 100.0, 100.0, 80.0);
        Instrument transformedInstrument = processor.process(originalInstrument);
        
        double newAveragePrice = (1.0/count)*((count-1)*80.0+100.0);
        
        Assert.assertEquals(newAveragePrice, transformedInstrument.getAveragePrice(), 0.1); //TODO: update delta precision
    }

}
