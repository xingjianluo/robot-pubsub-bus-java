package com.github.xingjianluo.robotpubsubbusjava.web;

import com.github.xingjianluo.robotpubsubbusjava.bus.BusException;
import com.github.xingjianluo.robotpubsubbusjava.bus.ErrorCode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * 总线异常到 HTTP 响应的统一映射：错误码随响应体返回，鉴权类、越权类、
 * 未知主题类、断线/摘除类彼此可区分。
 */
@RestControllerAdvice
public class BusExceptionHandler {

    /** 错误响应体。 */
    public record ErrorResponse(String error, String message) {
    }

    @ExceptionHandler(BusException.class)
    public ResponseEntity<ErrorResponse> handleBus(BusException ex) {
        return ResponseEntity.status(statusOf(ex.code())).body(
                new ErrorResponse(ex.code().name(), ex.getMessage()));
    }

    /** 请求体无法解析（缺字段 / Base64 非法等）统一归类为畸形报文。 */
    @ExceptionHandler({IllegalArgumentException.class, HttpMessageNotReadableException.class})
    public ResponseEntity<ErrorResponse> handleMalformed(Exception ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(
                new ErrorResponse(ErrorCode.MALFORMED_MESSAGE.name(),
                        "请求格式非法：" + ex.getMessage()));
    }

    private HttpStatus statusOf(ErrorCode code) {
        return switch (code) {
            case UNAUTHENTICATED -> HttpStatus.UNAUTHORIZED;            // 401 未带身份
            case INVALID_CREDENTIALS -> HttpStatus.FORBIDDEN;           // 403 身份不合法
            case UNAUTHORIZED_PUBLISH, UNAUTHORIZED_SUBSCRIBE -> HttpStatus.FORBIDDEN; // 403 越权
            case UNKNOWN_TOPIC, SUBSCRIPTION_NOT_FOUND -> HttpStatus.NOT_FOUND;        // 404
            case SESSION_NOT_FOUND -> HttpStatus.NOT_FOUND;                              // 404
            case SESSION_DISCONNECTED -> HttpStatus.CONFLICT;           // 409 断线中
            case SESSION_CLOSED -> HttpStatus.GONE;                     // 410 会话已废
            case SUBSCRIPTION_EVICTED -> HttpStatus.CONFLICT;           // 409 订阅被摘除
            case MALFORMED_MESSAGE -> HttpStatus.BAD_REQUEST;           // 400
            case MESSAGE_TOO_LARGE -> HttpStatus.PAYLOAD_TOO_LARGE;     // 413
        };
    }
}
