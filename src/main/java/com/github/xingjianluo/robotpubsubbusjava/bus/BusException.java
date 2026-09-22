package com.github.xingjianluo.robotpubsubbusjava.bus;

/**
 * 总线操作失败时抛出的异常，携带可区分的 {@link ErrorCode}。
 */
public class BusException extends RuntimeException {

    private final ErrorCode code;

    public BusException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
