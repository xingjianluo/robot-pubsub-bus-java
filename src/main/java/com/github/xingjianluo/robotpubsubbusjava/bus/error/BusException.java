package com.github.xingjianluo.robotpubsubbusjava.bus.error;

/** 统一失败语义：所有错误均带可区分错误码、上下文信息与可选原因。 */
public class BusException extends RuntimeException {

    private final BusErrorCode code;

    public BusException(BusErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public BusException(BusErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public BusErrorCode errorCode() {
        return code;
    }
}
