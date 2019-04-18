# Market Price Processor

*Market Price Processor* is a market data service that handles a file input of prices associated with instruments. It persists the pricing data in an in-memory database, and produces a report after a specified period (e.g. 30 seconds) that analyses:

* The 2nd highest persisted price (to reduce the impact of high price outlier) over the entire period;
* The average persisted price over the last 10 seconds. 

The 4 sample instruments are split into 2 groups:
1. Tne high update frequency group (`GOOG` and `BP.L`): price persists every 3 seconds;
2. The low update frequency group(`BT.L` and `VOD.L`): price persists every 5 seconds.


## Implementation

### Libraries

*Market Price Processor* is written in Java programming language that implements the Spring framework. It integrates with HSQLDB and Logback, and uses Maven for dependency management.

### Input & Output
The instrument market data is mocked in a text file (refer to `/src/main/resources/sample-data.txt`) that is read continuously in a loop. 
The data update output is on the command line, while the analysis of 2nd highest and averge price is generated in a csv file (located in `/output`). The logging output is saved too (located in `/logs`).

### Architecture

#### Model
The model is defined in the `Instrument` class, and a database schema  `schema-all.sql` stores the instrument details.

#### Reader and Writer
A writer class defined in `InstrumentMarketPriceBatchScheduler.java` populates the service periodically with dummy data from the price source. Running in parallel is a reader class defined in `InstrumentPersistedPriceBatchScheduler` that gets data from the service for storage and analysis.

#### Batch Processor
Spring batch processing is implemented to process the group of instruments in one go.

#### Task Scheduler
Task scheduler is implemented for price processing at defined fix frequency.

#### Multi-Threading
Spring Boot's asynchronous method call is enabled with to make the market price update tasks run on different threads.


## Assumptions

### Precision
All pricing information is stored in `double` data structure, and the uncertainty delta used in unit tests is 1e-8.

### Real Time
Although we defines pricing update time assumping it reflects real time, there is no real-time guarantee in a Java SE JVM, without involving a Real Time Java implementation running on a real time operating system. One of the reason is that Java's Garbage Collector may run at any time, and when it runs, all Java processes pause, include any form of timer implemented in the code base.

### Calculate Average
The "average price" is calculated by incremental averaging. Instead of keeping all previous terms, it keeps only the number of terms (count) so far and the current average (the running mean).

Formula:
```
newAverage = (1/count) * ((count-1) * currenAverage+currentValue)
```

### Decide 2nd highest price
The 2nd highest price will not be the same as the highest price. If the same highest price is observed twice, it is still only the highest price; the 2nd highest will not be the same as highest value (unless both the highest and the 2nd highest price are zero).


## Getting Started

### Prerequisities

You'll need to install:
* JDK 1.8 or later
* Maven 3.2+


### Installing
Run maven package to compile Java code, run tests, and finish by packaging the code up in a JAR file within the target directory.
```bash
mvn package
```

Or you could run the script that takes care of building the project:
```bash
$ ./scripts/run.sh
```

### Usage

Simply run the packaged jar file:
```bash
java -jar target/market-price-processor-0.1.0.jar
```

### Tests

We use jUnit for unit tests that runs everytime when the project is built.



