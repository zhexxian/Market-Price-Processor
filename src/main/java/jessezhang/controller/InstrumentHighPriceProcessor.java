package jessezhang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;

public class InstrumentHighPriceProcessor implements ItemProcessor<Instrument, Instrument> {

    private static final Logger log = LoggerFactory.getLogger(InstrumentHighPriceProcessor.class);

    @Override
    public Instrument process(final Instrument instrument) throws Exception {
        final String name = instrument.getName();
        final double currentPrice = instrument.getCurrentPrice();
        final double persistedPrice = instrument.getPersistedPrice();
        final double averagePrice = instrument.getAveragePrice();
        final double highestPrice;
        final double secondHighestPrice;
        double currentHighestPrice = instrument.getHighestPrice();
        double currentSecondHighestPrice = instrument.getSecondHighestPrice();
        
        if (persistedPrice > currentHighestPrice) {
            highestPrice = persistedPrice;
            secondHighestPrice = currentHighestPrice;
        }
        else if (persistedPrice == currentHighestPrice) {
            highestPrice = persistedPrice;
            secondHighestPrice = currentSecondHighestPrice;
        }
        else if (persistedPrice > currentSecondHighestPrice) {
            highestPrice = currentHighestPrice;
            secondHighestPrice = persistedPrice;
        }
        else {
            highestPrice = currentHighestPrice;
            secondHighestPrice = currentSecondHighestPrice;
        }

        final Instrument transformedInstrument = new Instrument(name, currentPrice, persistedPrice, highestPrice, secondHighestPrice, averagePrice);

        log.info("Converting (" + instrument + ") into (" + transformedInstrument + ")");
        return transformedInstrument;
    }

}
