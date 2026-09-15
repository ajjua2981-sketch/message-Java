package com.dams.messageparsing.batch;

import com.dams.messageparsing.consumer.KafkaMessageConsumer;
import com.dams.messageparsing.processor.MessageProcessor;
import com.dams.messageparsing.processor.PermanentMessageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Entry point for the batch run.
 *
 * <p>Implements {@link CommandLineRunner} so Spring Boot calls {@link #run} once
 * on startup and exits when it returns — making this suitable for cron scheduling
 * without keeping a long-running process alive.</p>
 *
 * <p>Offset-commit strategy (mirrors the Python implementation):
 * <ul>
 *   <li>Success: commit after Oracle update — message fully processed.</li>
 *   <li>Permanent failure (bad XML / missing path): commit to skip poison pill.</li>
 *   <li>Transient failure (DB down, etc.): do NOT commit — exit the loop so the
 *       message is retried on the next cron run from the last committed offset.</li>
 * </ul></p>
 */
@Component
public class BatchRunner implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(BatchRunner.class);

    @Value("${app.max-messages-per-run:10000}")
    private int maxMessagesPerRun;

    @Value("${app.poll-timeout-seconds:5}")
    private long pollTimeoutSeconds;

    @Value("${app.empty-polls-before-exit:3}")
    private int emptyPollsBeforeExit;

    private final KafkaMessageConsumer kafkaConsumer;
    private final MessageProcessor processor;

    public BatchRunner(KafkaMessageConsumer kafkaConsumer, MessageProcessor processor) {
        this.kafkaConsumer = kafkaConsumer;
        this.processor = processor;
    }

    @Override
    public void run(String... args) {
        int processed = 0;
        int failed    = 0;
        int emptyPolls = 0;

        try {
            kafkaConsumer.start();
            logger.info("Batch started — max messages per run: {}", maxMessagesPerRun);

            while (processed + failed < maxMessagesPerRun) {
                String xmlMessage = kafkaConsumer.poll(Duration.ofSeconds(pollTimeoutSeconds));

                if (xmlMessage == null) {
                    if (++emptyPolls >= emptyPollsBeforeExit) {
                        logger.info("No more messages in topic — ending batch");
                        break;
                    }
                    continue;
                }

                emptyPolls = 0;

                try {
                    processor.processMessage(xmlMessage);
                    kafkaConsumer.commit();
                    processed++;

                } catch (PermanentMessageException e) {
                    // Malformed XML or missing reference-ID path — retrying will not help.
                    // Commit to advance past this poison pill.
                    logger.warn("Skipping unprocessable message (permanent failure): {}", e.getMessage());
                    kafkaConsumer.commit();
                    failed++;

                } catch (Exception e) {
                    // Likely a transient error (e.g. DB unreachable).
                    // Do NOT commit — message will be retried on the next cron run.
                    logger.error("Aborting batch due to unexpected error: {}", e.getMessage(), e);
                    failed++;
                    break;
                }
            }

        } finally {
            kafkaConsumer.stop();
            logger.info("Batch complete — processed: {}  failed: {}", processed, failed);
        }

        if (failed > 0) {
            System.exit(1);
        }
    }
}
