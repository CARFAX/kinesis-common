package com.carfax.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeIntervalBasedKinesisRecordProcessor implements IRecordProcessor, IShutdownNotificationAware {

    private ShutdownNotificationAwareRecordProcessor delegateProcessor;
    private long checkpointIntervalMillis = 60000L;
    private long nextCheckpointTimeInMillis;
    private int maxRetries = 3;
    private long backoffTimeInMillis = 3000L;

    private static final Logger logger = LoggerFactory.getLogger(TimeIntervalBasedKinesisRecordProcessor.class);

    @Override
    public void initialize(InitializationInput initializationInput) {
        delegateProcessor.initialize(initializationInput);
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        try {
            delegateProcessor.processRecords(processRecordsInput);

            // Checkpoint once every checkpoint interval.
            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(processRecordsInput.getCheckpointer());
                nextCheckpointTimeInMillis = System.currentTimeMillis() + checkpointIntervalMillis;
            }
        } catch (Exception e) {
            delegateProcessor.handleException(e);
        }
    }

    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        delegateProcessor.shutdown(shutdownInput);

        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
            checkpoint(shutdownInput.getCheckpointer());
        }
    }

    @Override
    public void shutdownRequested(IRecordProcessorCheckpointer checkpointer) {
        delegateProcessor.shutdownRequested(checkpointer);
        checkpoint(checkpointer);
    }

    void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        for (int i = 1; i <= maxRetries; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (failover).
                break;
            } catch (ThrottlingException e) {
                // Back off and re-attempt checkpoint upon transient failures
                if (i >= (maxRetries - 1)) {
                    logger.info("@ Checkpoint failed after $i attempts." + e);
                    break;
                } else
                    logger.info("@ Transient issue when checkpointing - attempt" + i + "of" + maxRetries);
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                logger.info("@ Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library." + e);
                break;
            }

            sleepOnException();
        }
    }

    void sleepOnException() {
        try {
            Thread.sleep(backoffTimeInMillis);
        } catch (InterruptedException e) {
            logger.info("@ Interrupted sleep" + e);
        }
    }

    public ShutdownNotificationAwareRecordProcessor getDelegateProcessor() {
        return delegateProcessor;
    }

    public void setDelegateProcessor(ShutdownNotificationAwareRecordProcessor delegateProcessor) {
        this.delegateProcessor = delegateProcessor;
    }

    public long getCheckpointIntervalMillis() {
        return checkpointIntervalMillis;
    }

    public void setCheckpointIntervalMillis(long checkpointIntervalMillis) {
        this.checkpointIntervalMillis = checkpointIntervalMillis;
    }

    public long getNextCheckpointTimeInMillis() {
        return nextCheckpointTimeInMillis;
    }

    public void setNextCheckpointTimeInMillis(long nextCheckpointTimeInMillis) {
        this.nextCheckpointTimeInMillis = nextCheckpointTimeInMillis;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public long getBackoffTimeInMillis() {
        return backoffTimeInMillis;
    }

    public void setBackoffTimeInMillis(long backoffTimeInMillis) {
        this.backoffTimeInMillis = backoffTimeInMillis;
    }
}
