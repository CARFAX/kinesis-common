package com.carfax.kinesis

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput
import spock.lang.Specification

class TimeIntervalBasedKinesisRecordProcessorSpec extends Specification {

    private TimeIntervalBasedKinesisRecordProcessor timeIntervalBasedKinesisRecordProcessor

    void setup() {
        timeIntervalBasedKinesisRecordProcessor = new TimeIntervalBasedKinesisRecordProcessor()
        timeIntervalBasedKinesisRecordProcessor.setDelegateProcessor(Mock(ShutdownNotificationAwareRecordProcessor))
    }

    def "initialize"() {
        given:
        InitializationInput initializationInput = new InitializationInput()

        when:
        timeIntervalBasedKinesisRecordProcessor.initialize(initializationInput)

        then:
        1 * timeIntervalBasedKinesisRecordProcessor.getDelegateProcessor().initialize(initializationInput)
    }
}
