package jessezhang;

public class Instrument {

    private String name;
    private double currentPrice;
    private double persistedPrice;
    private double highestPrice;
    private double secondHighestPrice;
    private double averagePrice;
    
    public Instrument() {
    }

    public Instrument(String name, double currentPrice, double persistedPrice, double highestPrice, double secondHighestPrice, double averagePrice) {
        this.name = name;
        this.currentPrice = currentPrice;
        this.persistedPrice = persistedPrice;
        this.highestPrice = highestPrice;
        this.secondHighestPrice = secondHighestPrice;
        this.averagePrice = averagePrice;
    }


    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    public double getCurrentPrice() {
        return currentPrice;
    }
    
    public void setCurrentPrice(double currentPrice) {
        this.currentPrice = currentPrice;
    }
    
    public double getPersistedPrice() {
        return persistedPrice;
    }
    
    public void setPersistedPrice(double persistedPrice) {
        this.persistedPrice = persistedPrice;
    }
    
    public double getHighestPrice() {
        return highestPrice;
    }

    public void setHighestPrice(double highestPrice) {
        this.highestPrice = highestPrice;
    }
    
    public double getSecondHighestPrice() {
        return secondHighestPrice;
    }

    public void setSecondHighestPrice(double secondHighestPrice) {
        this.secondHighestPrice = secondHighestPrice;
    }
    
    public double getAveragePrice() {
        return averagePrice;
    }

    public void setAveragePrice(double averagePrice) {
        this.averagePrice = averagePrice;
    }

    @Override
    public String toString() {
        return String.format("%s: %f - %f - %f -%f - %f", name, currentPrice, persistedPrice, highestPrice, secondHighestPrice, averagePrice);
    }

}
