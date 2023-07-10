package org.springbatch;

import java.util.concurrent.Future;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestTemplate;

@Configuration
public class BatchConfig {
    private JobRepository jobRepository;
    private PlatformTransactionManager transactionManager;
    private RestTemplate restTemplate;

    public BatchConfig(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.restTemplate = new RestTemplate();
    }

    @Bean
    public Job importClientsJob(JobRepository jobRepository, Step importClientsStep) {
        return new JobBuilder("importClientsJob", jobRepository)
                .start(importClientsStep)
                .build();
    }

    @Bean
    public Step importClientsStep(ItemReader<Person> reader, ItemProcessor<Person, Future<Person>> processor,
                                  ItemWriter<Future<Person>> writer) {
        return new StepBuilder("importClientsStep", jobRepository)
                .<Person, Future<Person>>chunk(1000, transactionManager)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    @Bean
    public ItemReader<Person> reader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("personFileReader")
                .resource(new FileSystemResource("files/person.csv"))
                .delimited()
                .names("name", "email", "birthDay", "age", "id")
                .addComment("--")
                .fieldSetMapper((FieldSet fieldSet) -> {
                    return new Person(fieldSet.readLong("id"),
                            fieldSet.readString("name"), fieldSet.readString("email"),
                            fieldSet.readString("birthDay"), fieldSet.readInt("age"), null);
                })
                .build();
    }

    @Bean
    public ItemProcessor<Person, Future<Person>> asyncProcessor(ItemProcessor<Person, Person> itemProcessor,
                                                                TaskExecutor taskExecutor) {
        var asyncProcessor = new AsyncItemProcessor<Person, Person>();
        asyncProcessor.setTaskExecutor(taskExecutor);
        asyncProcessor.setDelegate(itemProcessor);
        return asyncProcessor;
    }

    @Bean
    public ItemProcessor<Person, Person> processor() {
        return person -> {
            var uri = "https://jsonplaceholder.typicode.com/photos/" + person.id();
            var photo = restTemplate.getForObject(uri, Photo.class);
            var newPessoa = new Person(person.id(), person.name(), person.email(), person.birthDay(), person.age(),
                    photo.thumbnailUrl());
            return newPessoa;
        };
    }

    @Bean
    public ItemWriter<Future<Person>> asyncWriter(ItemWriter<Person> writer) {
        var asyncWriter = new AsyncItemWriter<Person>();
        asyncWriter.setDelegate(writer);
        return asyncWriter;
    }

    @Bean
    public ItemWriter<Person> writer() {
        return System.out::println;
    }

}

record Person(Long id, String name, String email, String birthDay, Integer age, String thumbnail) {
}

record Photo(Long id, String thumbnailUrl) {
}