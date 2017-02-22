package com.carfax.kinesis;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IShutdownNotificationAware;

public interface ShutdownNotificationAwareRecordProcessor extends IRecordProcessor, IShutdownNotificationAware {

}
