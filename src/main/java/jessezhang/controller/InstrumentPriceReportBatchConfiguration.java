package jessezhang;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

@Configuration
public class InstrumentPriceReportBatchConfiguration {

    private static final Logger log = LoggerFactory.getLogger(InstrumentPriceReportBatchConfiguration.class);
    
    private Resource outputResource = new FileSystemResource("output/instrument_price_report.csv");

    private static final String PRICE_REPORT_HEADER = "Instrument Name, Second Highest Persisted Price, Average Persisted Price";

    private static final String QUERY_FIND_INSTRUMENTS =
            "SELECT " +
                "name, " +
                "second_highest_price, " +
                "average_price " +
            "FROM instruments";
            
    @Value("${number.of.instruments}")
    private int numberOfInstruments;

    @Autowired
    public DataSource dataSource;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;
 
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
 
    //================================================================================
    // Reader, Writer
    //================================================================================
    
    @Bean
    ItemReader<Instrument> databaseInstrumentPriceItemReader(DataSource dataSource) {
        JdbcCursorItemReader<Instrument> databaseReader = new JdbcCursorItemReader<>();
        databaseReader.setDataSource(dataSource);
        databaseReader.setSql(QUERY_FIND_INSTRUMENTS);
        databaseReader.setRowMapper(new BeanPropertyRowMapper<>(Instrument.class));
        return databaseReader;
    }

    @Bean
    public FlatFileItemWriter<Instrument> instrumentPriceReportWriter() {
        FlatFileItemWriter<Instrument> writer = new FlatFileItemWriter<>();
        String priceReportFileHeader = PRICE_REPORT_HEADER;
        StringHeaderWriter headerWriter = new StringHeaderWriter(priceReportFileHeader);
        writer.setHeaderCallback(headerWriter);
        writer.setResource(outputResource);
        writer.setAppendAllowed(false);
        writer.setLineAggregator(new DelimitedLineAggregator<Instrument>() {{
                setDelimiter(",");
                setFieldExtractor(new BeanWrapperFieldExtractor<Instrument>() {{
                    setNames(new String[] { "name", "secondHighestPrice", "averagePrice" });
            }});
        }});
        return writer;
    }
    
    //================================================================================
    // Job, Step
    //================================================================================

    @Bean
    public Job reportInstrumentPriceJob() {
        return jobBuilderFactory
            .get("reportInstrumentPriceJob")
            .incrementer(new RunIdIncrementer())
            .start(reportInstrumentPriceStep())
            .build();
    }

    @Bean
    public Step reportInstrumentPriceStep() {
    return stepBuilderFactory.get("reportInstrumentPriceStep").<Instrument, Instrument>chunk(numberOfInstruments)
            .reader(databaseInstrumentPriceItemReader(dataSource))
            .writer(instrumentPriceReportWriter())
            .build();
    }

}