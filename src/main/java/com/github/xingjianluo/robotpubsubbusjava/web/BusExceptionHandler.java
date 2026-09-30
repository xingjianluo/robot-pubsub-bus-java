package com.github.xingjianluo.robotpubsubbusjava.web;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 统一失败语义映射：BusException 的错误码映射为可区分的 HTTP 状态码，
 * 响应体始终携带 {@code code} 与 {@code message}，客户端可据此区分
 * 未认证、越权、未知主题、断线拦截等不同失败。
 */
@RestControllerAdvice
public class BusExceptionHandler {

    @ExceptionHandler(BusException.class)
    public ResponseEntity<BusApiDtos.ErrorResponse> handle(BusException exception) {
        HttpStatus status = switch (exception.code()) {
            case UNAUTHENTICATED, AUTHENTICATION_FAILED -> HttpStatus.UNAUTHORIZED;
            case UNAUTHORIZED_PUBLISH, UNAUTHORIZED_SUBSCRIBE -> HttpStatus.FORBIDDEN;
            case UNKNOWN_TOPIC, SESSION_NOT_FOUND, SUBSCRIPTION_NOT_FOUND -> HttpStatus.NOT_FOUND;
            case SESSION_DISCONNECTED, SESSION_CLOSED, SUBSCRIPTION_EVICTED -> HttpStatus.CONFLICT;
            case MALFORMED_MESSAGE, MESSAGE_TOO_LARGE -> HttpStatus.BAD_REQUEST;
        };
        return ResponseEntity.status(status)
                .body(new BusApiDtos.ErrorResponse(exception.code().name(), exception.getMessage()));
    }
}
