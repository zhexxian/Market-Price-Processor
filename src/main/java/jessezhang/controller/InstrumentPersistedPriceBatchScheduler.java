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

    @Autowired
    private ApplicationContext context;

    @Autowired
    public DataSource dataSource;
        
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    @Autowired
    public UpdatePersistedPriceJobCompletionNotificationListener updatePersistedPriceJobCompletionNotificationListener;
    
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
       
    private AtomicInteger timeInSeconds = new AtomicInteger(0); //TODO: rename
    final private int numberOfInstruments = 4; //TODO: extract this variable
     
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
        return new InstrumentAveragePriceProcessor(4); //TODO: extract as variable
    }
    
    @Bean
    public InstrumentAveragePriceProcessor instrumentLowUpdateFrequencyAveragePriceProcessor() {
        return new InstrumentAveragePriceProcessor(2); //TODO: extract as variable
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
            .listener(updatePersistedPriceJobCompletionNotificationListener)
            .flow(highFrequencyUpdatePersistedPriceStep())
            .end()
            .build();
    }
    
    public Step highFrequencyUpdatePersistedPriceStep() {
        return stepBuilderFactory.get("highFrequencyUpdatePersistedPriceStep")
            .<Instrument, Instrument> chunk(2) //TODO: extract as variable
            .reader(databaseHighUpdateFrequencyInstrumentItemReader(dataSource))
            .processor(instrumentHighPriceProcessor())
            .writer(instrumentPersistedPriceWriter())
            .build();
    }

    public Job highFrequencyUpdateAveragePriceJob() {
        return jobBuilderFactory.get("highFrequencyUpdateAveragePriceJob")
            .incrementer(new RunIdIncrementer())
            .listener(updatePersistedPriceJobCompletionNotificationListener)
            .flow(highFrequencyUpdateAveragePriceStep())
            .end()
            .build();
    }

    public Step highFrequencyUpdateAveragePriceStep() {
        return stepBuilderFactory.get("highFrequencyUpdateAveragePriceStep")
            .<Instrument, Instrument> chunk(2) //TODO: extract as variable
            .reader(databaseHighUpdateFrequencyInstrumentItemReader(dataSource))
            .processor(instrumentHighUpdateFrequencyAveragePriceProcessor())
            .writer(instrumentPersistedPriceWriter())
            .build();
    }
    
    public Job lowFrequencyUpdatePersistedPriceJob() {
        return jobBuilderFactory.get("lowFrequencyUpdatePersistedPriceJob")
            .incrementer(new RunIdIncrementer())
            .listener(updatePersistedPriceJobCompletionNotificationListener)
            .flow(lowFrequencyUpdatePersistedPriceStep())
            .end()
            .build();
    }

    public Step lowFrequencyUpdatePersistedPriceStep() {
        return stepBuilderFactory.get("lowFrequencyUpdatePersistedPriceStep")
            .<Instrument, Instrument> chunk(4) //TODO: extract as variable
            .reader(databaseLowUpdateFrequencyInstrumentItemReader(dataSource))
            .processor(instrumentHighPriceProcessor())
            .writer(instrumentPersistedPriceWriter())
            .build();
    }

    public Job lowFrequencyUpdateAveragePriceJob() {
        return jobBuilderFactory.get("lowFrequencyUpdateAveragePriceJob")
            .incrementer(new RunIdIncrementer())
            .listener(updatePersistedPriceJobCompletionNotificationListener)
            .flow(lowFrequencyUpdateAveragePriceStep())
            .end()
            .build();
    }

    public Step lowFrequencyUpdateAveragePriceStep() {
        return stepBuilderFactory.get("lowFrequencyUpdateAveragePriceStep")
            .<Instrument, Instrument> chunk(4) //TODO: extract as variable
            .reader(databaseLowUpdateFrequencyInstrumentItemReader(dataSource))
            .processor(instrumentLowUpdateFrequencyAveragePriceProcessor())
            .writer(instrumentPersistedPriceWriter())
            .build();
    }

    //================================================================================
    // Scheduled Jobs
    //================================================================================
    
   @Scheduled(fixedRate = 3000) //TODO: extract as variable
    public void highFrequencyUpdatePersistedPrice() throws Exception {
        JobParameters param = new JobParametersBuilder().addString("JobID-highFrequencyUpdatePersistedPrice-", String.valueOf(System.currentTimeMillis())).toJobParameters();
        JobExecution execution = updatePersistedPriceJobLauncher().run( highFrequencyUpdatePersistedPriceJob(), param);
    }
    
    @Scheduled(fixedRate = 3000, initialDelay = 18000) //TODO: extract as variable
    public void highFrequencyUpdateAveragePrice() throws Exception {
        JobParameters param = new JobParametersBuilder().addString("Job-highFrequencyUpdateAveragePrice-", String.valueOf(System.currentTimeMillis())).toJobParameters();
        JobExecution execution = updatePersistedPriceJobLauncher().run( highFrequencyUpdateAveragePriceJob(), param);
    }

    @Scheduled(fixedRate = 5000) //TODO: extract as variable
    public void lowFrequencyUpdatePersistedPrice() throws Exception {
        JobParameters param = new JobParametersBuilder().addString("Job-lowFrequencyUpdatePersistedPrice-", String.valueOf(System.currentTimeMillis())).toJobParameters();
        JobExecution execution = updatePersistedPriceJobLauncher().run( lowFrequencyUpdatePersistedPriceJob(), param);
    }
    

    @Scheduled(fixedRate = 5000, initialDelay = 20000) //TODO: extract as variable
    public void lowFrequencyUpdateAveragePrice() throws Exception {
        log.info(String.valueOf(timeInSeconds.getAcquire()));
        JobParameters param = new JobParametersBuilder().addString("Job-lowFrequencyUpdateAveragePrice-", String.valueOf(System.currentTimeMillis())).toJobParameters();
        JobExecution execution = updatePersistedPriceJobLauncher().run( lowFrequencyUpdateAveragePriceJob(), param);
        timeInSeconds.getAndIncrement();
        if (timeInSeconds.getAcquire() >2) { //TODO: extract as variable
            log.info("All price processing jobs are completed. Please press ctrl+c to exit the program.");
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
