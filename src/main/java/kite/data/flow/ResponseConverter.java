package kite.data.flow;

import kite.datasource.Response;

public interface ResponseConverter {
    Object convert(Object response);
}
