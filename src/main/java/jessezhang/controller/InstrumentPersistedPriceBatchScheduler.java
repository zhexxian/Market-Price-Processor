package jessezhang;

import javax.sql.DataSource;

import java.util.Map;
import java.util.IdentityHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.ScheduledMethodRunnable;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

@Configuration
public class InstrumentPersistedPriceBatchScheduler {

    private static final Logger log = LoggerFactory.getLogger(InstrumentPersistedPriceBatchScheduler.class);
    
    private final Map<Object, ScheduledFuture<?>> scheduledTasks = new IdentityHashMap<>();
    
    private static final String QUERY_FIND_INSTRUMENTS_HIGH_UPDATE_FREQUENCY =
            "SELECT " +
                "name, " +
                "current_price, " +
                "persisted_price, " +
                "highest_price, " +
                "second_highest_price, " +
                "average_price " +
            "FROM instruments " +
            "WHERE " +
                "name = 'BP.L' OR name = 'GOOG'";

    private static final String QUERY_FIND_INSTRUMENTS_LOW_UPDATE_FREQUENCY =
            "SELECT " +
                "name, " +
                "current_price, " +
                "persisted_price, " +
                "highest_price, " +
                "second_highest_price, " +
                "average_price " +
            "FROM instruments " +
            "WHERE " +
                "name = 'BT.L' OR name = 'VOD.L'";
    
    private static final int PERSISTENT_PRICE_HIGH_FREQUENCY_UPDATE_INTERVAL_IN_MILISECONDS = 3000;
    
    private static final int PERSISTENT_PRICE_LOW_FREQUENCY_UPDATE_INTERVAL_IN_MILISECONDS = 5000;
    
    private static final int AVERAGE_PRICE_HIGH_FREQUENCY_INITIAL_DELAY_IN_MILISECONDS = 18000;
    
    private static final int AVERAGE_PRICE_LOW_FREQUENCY_INITIAL_DELAY_IN_MILISECONDS = 20000;
    
    private static final int
    HIGH_FREQUENCY_INSTRUMENT_UPDATE_AVERAGE_BASE = 4;

    private static final int
    LOW_FREQUENCY_INSTRUMENT_UPDATE_AVERAGE_BASE = 2;

    private AtomicInteger timer = new AtomicInteger(0);
    private AtomicInteger timeOfLowFrequencyInstrumentUpdateAverageBase = new AtomicInteger(0);

    @Value("${number.of.high.update.frequency.instruments}")
    private int numberOfHighUpdateFrequencyInstruments;
        
    @Value("${number.of.low.update.frequency.instruments}")
    private int numberOfLowUpdateFrequencyInstruments;

    @Autowired
    private ApplicationContext context;

    @Autowired
    public DataSource dataSource;
        
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    @Autowired
    Job reportInstrumentPriceJob;
    
    @Autowired
    public UpdateHighFrequencyPersistedPriceJobCompletionNotificationListener updateHighFrequencyPersistedPriceJobCompletionNotificationListener;
    
    @Autowired
    public UpdateLowFrequencyPersistedPriceJobCompletionNotificationListener updateLowFrequencyPersistedPriceJobCompletionNotificationListener;

    @Autowired
    public UpdateAveragePriceJobCompletionNotificationListener updateAveragePriceJobCompletionNotificationListener;
    
    @Bean
    public JobLauncher updatePersistedPriceJobLauncher() throws Exception {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(updatePersistedPriceJobRepository());
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }
    
    @Bean
    public JobRepository updatePersistedPriceJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(new ResourcelessTransactionManager());
        return (JobRepository) factory.getObject();
    }
     
    //================================================================================
    // Reader, Writer, Processor
    //================================================================================
  
    @Bean
    ItemReader<Instrument> databaseHighUpdateFrequencyInstrumentItemReader(DataSource dataSource) {
        JdbcCursorItemReader<Instrument> databaseReader = new JdbcCursorItemReader<>();
        databaseReader.setDataSource(dataSource);
        databaseReader.setSql(QUERY_FIND_INSTRUMENTS_HIGH_UPDATE_FREQUENCY);
        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(Instrument.class));
        return databaseReader;
    }
    
    @Bean
    ItemReader<Instrument> databaseLowUpdateFrequencyInstrumentItemReader(DataSource dataSource) {
        JdbcCursorItemReader<Instrument> databaseReader = new JdbcCursorItemReader<>();
        databaseReader.setDataSource(dataSource);
        databaseReader.setSql(QUERY_FIND_INSTRUMENTS_LOW_UPDATE_FREQUENCY);
        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(Instrument.class));
        return databaseReader;
    }
    
    @Bean
    public InstrumentHighPriceProcessor instrumentHighPriceProcessor() {
        return new InstrumentHighPriceProcessor();
    }
    
    @Bean
    public InstrumentAveragePriceProcessor instrumentHighUpdateFrequencyAveragePriceProcessor() {
        return new InstrumentAveragePriceProcessor(HIGH_FREQUENCY_INSTRUMENT_UPDATE_AVERAGE_BASE);
    }
    
    @Bean
    public InstrumentAveragePriceProcessor instrumentLowUpdateFrequencyAveragePriceProcessor() {
        return new InstrumentAveragePriceProcessor(LOW_FREQUENCY_INSTRUMENT_UPDATE_AVERAGE_BASE);
    }
    
    @Bean
    public JdbcBatchItemWriter<Instrument> instrumentPersistedPriceWriter() {
        return new JdbcBatchItemWriterBuilder<Instrument>()
            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql("UPDATE " +
                            "instruments " + 
                    "SET " + 
                            "persisted_price = :currentPrice, " + 
                            "highest_price = :highestPrice, " + 
                            "second_highest_price = :secondHighestPrice, " + 
                            "average_price = :averagePrice " + 
                    "WHERE name=:name")
            .dataSource(dataSource)
            .build();
    }
    
    //================================================================================
    // Job, Step
    //================================================================================

    public Job highFrequencyUpdatePersistedPriceJob() {
        return jobBuilderFactory.get("highFrequencyUpdatePersistedPriceJob")
            .incrementer(new RunIdIncrementer())
            .listener(updateHighFrequencyPersistedPriceJobCompletionNotificationListener)
            .flow(highFrequencyUpdatePersistedPriceStep())
            .end()
            .build();
    }
    
    public Step highFrequencyUpdatePersistedPriceStep() {
        return stepBuilderFactory.get("highFrequencyUpdatePersistedPriceStep")
            .<Instrument, Instrument> chunk(numberOfHighUpdateFrequencyInstruments)
            .reader(databaseHighUpdateFrequencyInstrumentItemReader(dataSource))
            .processor(instrumentHighPriceProcessor())
            .writer(instrumentPersistedPriceWriter())
            .build();
    }

    public Job highFrequencyUpdateAveragePriceJob() {
        return jobBuilderFactory.get("highFrequencyUpdateAveragePriceJob")
            .incrementer(new RunIdIncrementer())
            .listener(updateAveragePriceJobCompletionNotificationListener)
            .flow(highFrequencyUpdateAveragePriceStep())
            .end()
            .build();
    }

    public Step highFrequencyUpdateAveragePriceStep() {
        return stepBuilderFactory.get("highFrequencyUpdateAveragePriceStep")
            .<Instrument, Instrument> chunk(numberOfHighUpdateFrequencyInstruments)
            .reader(databaseHighUpdateFrequencyInstrumentItemReader(dataSource))
            .processor(instrumentHighUpdateFrequencyAveragePriceProcessor())
            .writer(instrumentPersistedPriceWriter())
            .build();
    }
    
    public Job lowFrequencyUpdatePersistedPriceJob() {
        return jobBuilderFactory.get("lowFrequencyUpdatePersistedPriceJob")
            .incrementer(new RunIdIncrementer())
            .listener(updateLowFrequencyPersistedPriceJobCompletionNotificationListener)
            .flow(lowFrequencyUpdatePersistedPriceStep())
            .end()
            .build();
    }

    public Step lowFrequencyUpdatePersistedPriceStep() {
        return stepBuilderFactory.get("lowFrequencyUpdatePersistedPriceStep")
            .<Instrument, Instrument> chunk(numberOfHighUpdateFrequencyInstruments)
            .reader(databaseLowUpdateFrequencyInstrumentItemReader(dataSource))
            .processor(instrumentHighPriceProcessor())
            .writer(instrumentPersistedPriceWriter())
            .build();
    }

    public Job lowFrequencyUpdateAveragePriceJob() {
        return jobBuilderFactory.get("lowFrequencyUpdateAveragePriceJob")
            .incrementer(new RunIdIncrementer())
            .listener(updateAveragePriceJobCompletionNotificationListener)
            .flow(lowFrequencyUpdateAveragePriceStep())
            .end()
            .build();
    }

    public Step lowFrequencyUpdateAveragePriceStep() {
        return stepBuilderFactory.get("lowFrequencyUpdateAveragePriceStep")
            .<Instrument, Instrument> chunk(numberOfHighUpdateFrequencyInstruments)
            .reader(databaseLowUpdateFrequencyInstrumentItemReader(dataSource))
            .processor(instrumentLowUpdateFrequencyAveragePriceProcessor())
            .writer(instrumentPersistedPriceWriter())
            .build();
    }

    //================================================================================
    // Scheduled Jobs
    //================================================================================
    
   @Scheduled(fixedRate = PERSISTENT_PRICE_HIGH_FREQUENCY_UPDATE_INTERVAL_IN_MILISECONDS)
    public void highFrequencyUpdatePersistedPrice() throws Exception {
        JobParameters param = new JobParametersBuilder().addString("JobID-highFrequencyUpdatePersistedPrice-", String.valueOf(System.currentTimeMillis())).toJobParameters();
        JobExecution execution = updatePersistedPriceJobLauncher().run( highFrequencyUpdatePersistedPriceJob(), param);
    }
    
    @Scheduled(fixedRate = PERSISTENT_PRICE_HIGH_FREQUENCY_UPDATE_INTERVAL_IN_MILISECONDS, initialDelay = AVERAGE_PRICE_HIGH_FREQUENCY_INITIAL_DELAY_IN_MILISECONDS)
    public void highFrequencyUpdateAveragePrice() throws Exception {
        JobParameters param = new JobParametersBuilder().addString("Job-highFrequencyUpdateAveragePrice-", String.valueOf(System.currentTimeMillis())).toJobParameters();
        JobExecution execution = updatePersistedPriceJobLauncher().run( highFrequencyUpdateAveragePriceJob(), param);
    }

    @Scheduled(fixedRate = PERSISTENT_PRICE_LOW_FREQUENCY_UPDATE_INTERVAL_IN_MILISECONDS)
    public void lowFrequencyUpdatePersistedPrice() throws Exception {
        JobParameters param = new JobParametersBuilder().addString("Job-lowFrequencyUpdatePersistedPrice-", String.valueOf(System.currentTimeMillis())).toJobParameters();
        JobExecution execution = updatePersistedPriceJobLauncher().run( lowFrequencyUpdatePersistedPriceJob(), param);
    }
    

    @Scheduled(fixedRate = PERSISTENT_PRICE_LOW_FREQUENCY_UPDATE_INTERVAL_IN_MILISECONDS, initialDelay = AVERAGE_PRICE_LOW_FREQUENCY_INITIAL_DELAY_IN_MILISECONDS)
    public void lowFrequencyUpdateAveragePrice() throws Exception {
        JobParameters param = new JobParametersBuilder().addString("Job-lowFrequencyUpdateAveragePrice-", String.valueOf(System.currentTimeMillis())).toJobParameters();
        JobExecution execution = updatePersistedPriceJobLauncher().run( lowFrequencyUpdateAveragePriceJob(), param);
        timeOfLowFrequencyInstrumentUpdateAverageBase.getAndIncrement();
        if (timeOfLowFrequencyInstrumentUpdateAverageBase.getAcquire() > LOW_FREQUENCY_INSTRUMENT_UPDATE_AVERAGE_BASE) {
            reportInstrumentPrice();
            log.info("All price processing jobs are completed, and the report is generated in the /output folder. Please press ctrl+c to exit the program.");
            stopJobSchedulerWhenSchedulerDestroyed();
        }
    }
    
    //================================================================================
    // Scheduler
    //================================================================================

    @Bean
	public TaskScheduler poolScheduler() {
    	return new CustomTaskScheduler();
	}
	
	public void reportInstrumentPrice() throws Exception {
	    JobParameters param = new JobParametersBuilder().addString("Job-reportInstrumentPrice-", String.valueOf(System.currentTimeMillis())).toJobParameters();
        JobExecution execution = updatePersistedPriceJobLauncher().run(reportInstrumentPriceJob, param);
	}
	
	public void stopJobSchedulerWhenSchedulerDestroyed() throws Exception {
        ScheduledAnnotationBeanPostProcessor bean = context
            .getBean(ScheduledAnnotationBeanPostProcessor.class);
        TaskScheduler schedulerBean = context
            .getBean(TaskScheduler.class);
        bean.postProcessBeforeDestruction(
            schedulerBean, "TaskScheduler"); 
        bean.destroy();
    }
	
	class CustomTaskScheduler extends ThreadPoolTaskScheduler {

    	@Override
    	public ScheduledFuture<?> scheduleAtFixedRate(Runnable task, long period) {
        	ScheduledFuture<?> future = super.scheduleAtFixedRate(task, period);
        	ScheduledMethodRunnable runnable = (ScheduledMethodRunnable) task;
        	scheduledTasks.put(runnable.getTarget(), future);
        	return future;
    	}
    	
    }
        
}
