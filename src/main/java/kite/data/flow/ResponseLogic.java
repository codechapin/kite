package kite.data.flow;

public interface ResponseLogic<M, R> {
    ResponseLogicResult<R> apply(M model);
}
