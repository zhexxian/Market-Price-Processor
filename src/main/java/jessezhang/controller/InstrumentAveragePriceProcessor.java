package jessezhang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;

public class InstrumentAveragePriceProcessor implements ItemProcessor<Instrument, Instrument> {

    private static final Logger log = LoggerFactory.getLogger(InstrumentAveragePriceProcessor.class);
    private int count;
    
    public InstrumentAveragePriceProcessor(int count) {
        this.count = count;
    }
    @Override
    public Instrument process(final Instrument instrument) throws Exception {
        final String name = instrument.getName();
        final double currentPrice = instrument.getCurrentPrice();
        final double persistedPrice = instrument.getPersistedPrice();
        final double highestPrice = instrument.getHighestPrice();
        final double secondHighestPrice  = instrument.getSecondHighestPrice();
        final double averagePrice;
        double currentAveragePrice = instrument.getAveragePrice();
        
        if (currentAveragePrice == 0) {
            currentAveragePrice = persistedPrice;
        }
        
        averagePrice = (1.0/count)*((count-1)*currentAveragePrice+persistedPrice);
        
        final Instrument transformedInstrument = new Instrument(name, currentPrice, persistedPrice, highestPrice, secondHighestPrice, averagePrice);

        log.info("Converting (" + instrument + ") into (" + transformedInstrument + ")");
        return transformedInstrument;
    }

}
