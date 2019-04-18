package jessezhang;

import javax.sql.DataSource;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.ScheduledMethodRunnable;
import org.springframework.core.io.ClassPathResource;

@Configuration
@EnableBatchProcessing
public class InstrumentMarketPriceBatchScheduler {

    private static final Logger log = LoggerFactory.getLogger(InstrumentMarketPriceBatchScheduler.class);

    @Autowired
    public DataSource dataSource;
    
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;
    
    @Autowired
    public ImportMarketPriceJobCompletionNotificationListener importMarketPriceJobCompletionNotificationListener;
        
    private AtomicInteger timeInSeconds = new AtomicInteger(0);
    final private int numberOfInstruments = 4; //TODO: extract as variable
    int fileLengthInSeconds = 10; //TODO: extract as variable

    //================================================================================
    // Reader, Writer, Processor
    //================================================================================
    
    @Bean
    public ArrayList<FlatFileItemReader<Instrument>> readers() {
        ArrayList<FlatFileItemReader<Instrument>> readerArray = new ArrayList<FlatFileItemReader<Instrument>>();
        for (int i=0; i<fileLengthInSeconds; i++) {
            readerArray.add(i,new FlatFileItemReaderBuilder<Instrument>()
            .name("instrumentItemReader"+String.valueOf(i))
            .resource(new ClassPathResource("sample-data.txt"))
            .delimited()
            .delimiter(":")
            .names(new String[]{"name", "currentPrice"})
            .fieldSetMapper(new BeanWrapperFieldSetMapper<Instrument>() {{
                setTargetType(Instrument.class);
            }})
            .currentItemCount(i*numberOfInstruments)
            .maxItemCount((i+1)*numberOfInstruments) //TODO: create count variable; max item = batch size + currentItemCount
            .build());
        }
        return readerArray;
    }
        
    @Bean
    public JdbcBatchItemWriter<Instrument> instrumentMarketPriceWriter() {
        return new JdbcBatchItemWriterBuilder<Instrument>()
            .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
            .sql("UPDATE " +
                            "instruments " + 
                    "SET " + 
                            "current_price = :currentPrice " + 
                    "WHERE name=:name")
            .dataSource(dataSource)
            .build();
    }

    //================================================================================
    // Job, Step
    //================================================================================

    public Job importMarketPriceJob() {
        return jobBuilderFactory.get("importMarketPriceJob")
            .incrementer(new RunIdIncrementer())
            .listener(importMarketPriceJobCompletionNotificationListener)
            .flow(importMarketPriceStep())
            .end()
            .build();
    }

    public Step importMarketPriceStep() {
        return stepBuilderFactory.get("importMarketPriceStep")
            .<Instrument, Instrument> chunk(4) //TODO: extract value "4" to properties file
            .reader(readers().get(timeInSeconds.getAcquire() % 10)) //TODO: make the time variable loop ceil(30/10); logs
            .writer(instrumentMarketPriceWriter())
            .build();
    }
    
    //================================================================================
    // Scheduled Jobs
    //================================================================================
   
    @Scheduled(fixedRate = 1000) //TODO: extract this variable
    public void importMarketPrice() throws Exception {
        log.info(String.valueOf(timeInSeconds.getAcquire()));
        JobParameters param = new JobParametersBuilder().addString("JobID-importMarketPrice-", String.valueOf(System.currentTimeMillis())).toJobParameters();
        
        JobExecution execution = importMarketPriceJobLauncher().run( importMarketPriceJob(), param);
        timeInSeconds.getAndIncrement();
    }

    @Bean
    public JobLauncher importMarketPriceJobLauncher() throws Exception {
        SimpleJobLauncher importMarketPriceJobLauncher = new SimpleJobLauncher();
        importMarketPriceJobLauncher.setJobRepository(importMarketPriceJobRepository());
        importMarketPriceJobLauncher.afterPropertiesSet();
        return importMarketPriceJobLauncher;
    }
    
    @Bean
    public JobRepository importMarketPriceJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(dataSource);
        factory.setTransactionManager(new ResourcelessTransactionManager());
        return (JobRepository) factory.getObject();
    }
    
}
