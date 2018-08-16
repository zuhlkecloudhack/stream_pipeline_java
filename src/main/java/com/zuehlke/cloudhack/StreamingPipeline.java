package com.zuehlke.cloudhack;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.joda.time.Duration;

import java.util.Collections;
import java.util.Map;

public class StreamingPipeline {

    private static final Logger LOG = LogManager.getLogger(StreamingPipeline.class.getName());
    private static final String INPUT_TOPIC = "projects/zuhlkecloudhack/topics/test123";
    private static final String OUTPUT_TOPIC = "projects/zuhlkecloudhack/topics/test456";
    private static final String OUTPUT_DATASET = "cloudhack.flight_messages_java_pipeline";
    private static final String OUTPUT_GCS_PATH = "gs://flight_messages/java_pipeline/message";
    private static final String FILE_WRITTEN_RESPONSE = "file written to GCS";

    public static void main(String[] args) {
        LOG.info("Starting Streaming Pipeline");

        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);
        StreamingPipeline sp = new StreamingPipeline();

        // read from pub-sub topic
        PCollection<String> messages = p.apply("read event from pub-sub", PubsubIO.readStrings().fromTopic(INPUT_TOPIC));

        // write messages to GCS
        messages.apply("grouping messages to windows", assignMessageWindowFn())
                .apply("write message to GCS", writeToGCSFn())
                .getPerDestinationOutputFilenames()

                // write notification to pub-sub
                .apply("map to response message", createPubsubResponseFn())
                .apply("write response to pub/sub", PubsubIO.writeMessages().to(OUTPUT_TOPIC));

        // write messages to BQ
        messages
                .apply("convert messages to table rows", ParDo.of(new MessageToTableRowConverter()))
                .apply("insert into BigQuery", sp.writeToBigQuery());


        PipelineResult result = p.run();
        result.waitUntilFinish();
    }

    private static MapElements<KV<Object, String>, PubsubMessage> createPubsubResponseFn() {
        return MapElements.into(TypeDescriptor.of(PubsubMessage.class)).via(x -> createMessageFor(x.getValue()));
    }

    private static TextIO.TypedWrite<String, Object> writeToGCSFn() {
        return TextIO.write().to(OUTPUT_GCS_PATH).withOutputFilenames().withSuffix(".json").withWindowedWrites().withNumShards(1);
    }

    private static PubsubMessage createMessageFor(String filename) {
        Map<String, String> attrs = Collections.singletonMap("filename", filename);
        return new PubsubMessage(FILE_WRITTEN_RESPONSE.getBytes(), attrs);
    }

    private static Window<String> assignMessageWindowFn() {
        return Window.<String>into(FixedWindows.of(Duration.standardSeconds(5)))
                .withAllowedLateness(Duration.standardDays(1))
                .triggering(AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(AfterPane.elementCountAtLeast(1))
                        .withLateFirings(AfterFirst.of(
                                AfterPane.elementCountAtLeast(1),
                                AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(Duration.standardSeconds(1))
                        )))
                .discardingFiredPanes();
    }

    private BigQueryIO.Write<TableRow> writeToBigQuery() {
        return BigQueryIO.writeTableRows()
                .to(OUTPUT_DATASET)
                .withSchema(FlightMessage.getTableSchema())
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND);
    }
}