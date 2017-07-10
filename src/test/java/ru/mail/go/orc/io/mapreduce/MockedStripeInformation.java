package ru.mail.go.orc.io.mapreduce;

import org.apache.commons.lang.NotImplementedException;
import org.apache.orc.StripeInformation;

class MockedStripeInformation implements StripeInformation {
    final private long offset;
    final private long lenght;

    MockedStripeInformation(long offset, long length) {
        this.offset = offset;
        this.lenght = length;
    }

    @Override
    public long getOffset() {
        return offset;
    }

    @Override
    public long getLength() {
        return lenght;
    }

    @Override
    public long getIndexLength() throws RuntimeException {
        throw new NotImplementedException("Not implemented in mock");
    }

    @Override
    public long getDataLength() throws RuntimeException {
        throw new NotImplementedException("Not implemented in mock");
    }

    @Override
    public long getFooterLength() throws RuntimeException {
        throw new NotImplementedException("Not implemented in mock");
    }

    @Override
    public long getNumberOfRows() {
        throw new NotImplementedException("Not implemented in mock");
    }
}
