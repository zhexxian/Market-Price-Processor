package jessezhang;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.listener.JobExecutionListenerSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class UpdateLowFrequencyPersistedPriceJobCompletionNotificationListener extends JobExecutionListenerSupport {

	private static final Logger log = LoggerFactory.getLogger(UpdateLowFrequencyPersistedPriceJobCompletionNotificationListener.class);

	private final JdbcTemplate jdbcTemplate;

	@Autowired
	public UpdateLowFrequencyPersistedPriceJobCompletionNotificationListener(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		if (jobExecution.getStatus() == BatchStatus.COMPLETED) {
			log.debug("Low frequency persisted price update job finished, time to verify the results:");
			
			jdbcTemplate.query(
				"SELECT " + 
					"name, " + 
					"current_price, " +
					"persisted_price, " +
					"highest_price, " + 
					"second_highest_price, " + 
					"average_price " + 
				"FROM instruments",
				(resultSet, row) -> new Instrument(
					resultSet.getString("name"),
					resultSet.getDouble("current_price"),
					resultSet.getDouble("persisted_price"),
					resultSet.getDouble("highest_price"),
					resultSet.getDouble("second_highest_price"),
					resultSet.getDouble("average_price"))
				).forEach(instrument -> log.debug("Found <" + instrument + "> in the database."));
			
			jdbcTemplate.query(
				"SELECT " + 
					"name, " + 
					"current_price, " +
					"persisted_price, " +
					"highest_price, " + 
					"second_highest_price, " + 
					"average_price " + 
				"FROM instruments " + 
				"WHERE " +
					"name = 'BT.L' OR name = 'VOD.L'",
				(resultSet, row) -> new Instrument(
					resultSet.getString("name"),
					resultSet.getDouble("current_price"),
					resultSet.getDouble("persisted_price"),
					resultSet.getDouble("highest_price"),
					resultSet.getDouble("second_highest_price"),
					resultSet.getDouble("average_price"))
				).forEach(instrument -> log.info(instrument.getName() + " updated"));
		}
	}
}
