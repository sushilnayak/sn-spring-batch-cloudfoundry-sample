package com.nayak.simplepartitionexample;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.batch.partition.DeployerPartitionHandler;
import org.springframework.cloud.task.batch.partition.DeployerStepExecutionHandler;
import org.springframework.cloud.task.batch.partition.PassThroughCommandLineArgsProvider;
import org.springframework.cloud.task.batch.partition.SimpleEnvironmentVariablesProvider;
import org.springframework.cloud.task.configuration.EnableTask;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@EnableBatchProcessing
@EnableTask
@SpringBootApplication
public class SimplePartitionExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(SimplePartitionExampleApplication.class, args);
	}
}

@Configuration
class JobConfiguration {

	private static final int GRID_SIZE = 4;
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	@Autowired
	public DataSource dataSource;
	@Autowired
	public JobRepository jobRepository;

	@Autowired
	private ConfigurableApplicationContext context;
	@Autowired
	private DelegatingResourceLoader resourceLoader;
	@Autowired
	private Environment environment;

	@Bean
	public JobExplorerFactoryBean jobExplorer() {
		JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();

		jobExplorerFactoryBean.setDataSource(this.dataSource);

		return jobExplorerFactoryBean;
	}

	@Bean
	public PartitionHandler partitionHandler(TaskLauncher taskLauncher, JobExplorer jobExplorer) throws Exception {

		Resource resource = resourceLoader.getResource("file:///C:/Users/HP/.m2/repository/com/nayak/simple-partition-example/0.0.1-SNAPSHOT/simple-partition-example-0.0.1-SNAPSHOT.jar");

		DeployerPartitionHandler partitionHandler = new DeployerPartitionHandler( taskLauncher, jobExplorer, resource, "minionStep");

		partitionHandler.setGridSize(4);
		// partitionHandler.setPollInterval(60);

		List<String> commandLineArgs = new ArrayList<>(3);
		commandLineArgs.add("--spring.profiles.active=minion");
		commandLineArgs.add("--spring.cloud.task.initialize.enable=false");
		commandLineArgs.add("--spring.batch.initializer.enabled=false");
		partitionHandler.setCommandLineArgsProvider(
				new PassThroughCommandLineArgsProvider(commandLineArgs));
		partitionHandler.setEnvironmentVariablesProvider(
				new SimpleEnvironmentVariablesProvider(this.environment));
		partitionHandler.setMaxWorkers(1);

		partitionHandler.setApplicationName("test-job-sn-" + System.currentTimeMillis());

		return partitionHandler;
	}

	@Bean
	public Partitioner partitioner() {
		return new Partitioner() {
			@Override
			public Map<String, ExecutionContext> partition(int gridSize) {

				Map<String, ExecutionContext> partitions = new HashMap<>(gridSize);

				for (int i = 0; i < GRID_SIZE; i++) {
					ExecutionContext context1 = new ExecutionContext();
					context1.put("partitionNumber", i);

					partitions.put("partition" + i, context1);
				}

				return partitions;
			}
		};
	}

	@Bean
	@Profile("minion")
	public DeployerStepExecutionHandler stepExecutionHandler(JobExplorer jobExplorer) {
		return new DeployerStepExecutionHandler(this.context, jobExplorer, this.jobRepository);
	}

	@Bean
	@StepScope
	public Tasklet minionTasklet(final @Value("#{stepExecutionContext['partitionNumber']}") Integer partitionNumber) {

		return (contribution, chunkContext) -> {
			System.out.println("################################################");
			System.out.println("This tasklet ran partition: " + partitionNumber);
			System.out.println("################################################");

			return RepeatStatus.FINISHED;
		};
	}

	@Bean
	public Step step1(PartitionHandler partitionHandler) throws Exception {
		return stepBuilderFactory.get("step1")
				.partitioner(minionStep().getName(), partitioner()).step(minionStep())
				.partitionHandler(partitionHandler)
				.taskExecutor(new SimpleAsyncTaskExecutor()).build();
	}

	@Bean
	public Step minionStep() {

		return stepBuilderFactory.get("minionStep").tasklet(minionTasklet(null)).build();
	}

	@Bean
	@Profile("!minion")
	public Job partitionedJob(PartitionHandler partitionHandler) throws Exception {
		Random random = new Random();
		return jobBuilderFactory
				.get("partitionedJob" + random.nextInt())
				.start(step1(partitionHandler))
				.build();
	}
}
