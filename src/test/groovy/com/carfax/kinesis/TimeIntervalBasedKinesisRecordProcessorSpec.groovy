package com.carfax.kinesis

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput
import spock.lang.Specification

class TimeIntervalBasedKinesisRecordProcessorSpec extends Specification {

    private TimeIntervalBasedKinesisRecordProcessor timeIntervalBasedKinesisRecordProcessor

    void setup() {
        timeIntervalBasedKinesisRecordProcessor = Spy(TimeIntervalBasedKinesisRecordProcessor)
        timeIntervalBasedKinesisRecordProcessor.setDelegateProcessor(Mock(ShutdownNotificationAwareRecordProcessor))
    }

    void "initialize"() {
        given:
        InitializationInput initializationInput = new InitializationInput()

        when:
        timeIntervalBasedKinesisRecordProcessor.initialize(initializationInput)

        then:
        1 * timeIntervalBasedKinesisRecordProcessor.getDelegateProcessor().initialize(initializationInput)
    }

    void 'processRecords'() {
        given:
        timeIntervalBasedKinesisRecordProcessor.checkpointIntervalMillis = 1

        IRecordProcessorCheckpointer checkpointer = Mock(IRecordProcessorCheckpointer)
        ProcessRecordsInput processRecordsInput = new ProcessRecordsInput().withCheckpointer(checkpointer)

        when: 'Next checkpoint time is null (first time)'
        timeIntervalBasedKinesisRecordProcessor.processRecords(processRecordsInput)

        then: 'Process and then checkpoint'
        1 * timeIntervalBasedKinesisRecordProcessor.delegateProcessor.processRecords(processRecordsInput)
        1 * timeIntervalBasedKinesisRecordProcessor.checkpoint(checkpointer) >> null

        when: 'Next checkpoint time is has passed.  (Also setting the checkpoint interval really high for the next test)'
        sleep(2)
        timeIntervalBasedKinesisRecordProcessor.checkpointIntervalMillis = 60000
        timeIntervalBasedKinesisRecordProcessor.processRecords(processRecordsInput)

        then: 'Process and then checkpoint'
        1 * timeIntervalBasedKinesisRecordProcessor.delegateProcessor.processRecords(processRecordsInput)
        1 * timeIntervalBasedKinesisRecordProcessor.checkpoint(checkpointer) >> null

        when: 'We have not reached the next checkpoint time'
        timeIntervalBasedKinesisRecordProcessor.processRecords(processRecordsInput)

        then: 'Process, but do not checkpoint'
        1 * timeIntervalBasedKinesisRecordProcessor.delegateProcessor.processRecords(processRecordsInput)
        0 * timeIntervalBasedKinesisRecordProcessor.checkpoint(*_) >> null
    }

    void 'shutdown - ZOMBIE'() {
        given:
        ShutdownInput shutdownInput = new ShutdownInput().withShutdownReason(ShutdownReason.ZOMBIE)

        when:
        timeIntervalBasedKinesisRecordProcessor.shutdown(shutdownInput)

        then:
        0 * timeIntervalBasedKinesisRecordProcessor.checkpoint(*_)
    }

    void 'shutdown - TERMINATE'() {
        given:
        IRecordProcessorCheckpointer checkpointer = Mock(IRecordProcessorCheckpointer)
        ShutdownInput shutdownInput = new ShutdownInput().withShutdownReason(ShutdownReason.TERMINATE).withCheckpointer(checkpointer)

        when:
        timeIntervalBasedKinesisRecordProcessor.shutdown(shutdownInput)

        then:
        1 * timeIntervalBasedKinesisRecordProcessor.checkpoint(checkpointer) >> null
    }

    void 'shutdownRequested'() {
        given:
        IRecordProcessorCheckpointer checkpointer = Mock(IRecordProcessorCheckpointer)

        when:
        timeIntervalBasedKinesisRecordProcessor.shutdownRequested(checkpointer)

        then:
        1 * timeIntervalBasedKinesisRecordProcessor.checkpoint(checkpointer) >> null
    }

    void 'checkpointer gets called once on success'(){
        given:
        IRecordProcessorCheckpointer checkpointer = Mock(IRecordProcessorCheckpointer)

        when:
        timeIntervalBasedKinesisRecordProcessor.checkpoint(checkpointer)

        then:
        1 * checkpointer.checkpoint()
    }
}
