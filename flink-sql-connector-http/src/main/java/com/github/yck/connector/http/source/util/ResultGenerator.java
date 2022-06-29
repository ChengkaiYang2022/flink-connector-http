package com.github.yck.connector.http.source.util;

public class ResultGenerator {
    public static final String DEFAULT_SUCCESS_MESSAGE = "SUCCESS";
    public static Result getSuccessResult(){
        return new Result()
                .setCode(ResultCode.SUCCESS)
                .setMessage(DEFAULT_SUCCESS_MESSAGE);
    }

    public static <T> Result<T> getSuccessResult(T data){
        return new Result()
                .setCode(ResultCode.SUCCESS)
                .setMessage(DEFAULT_SUCCESS_MESSAGE)
                .setData(data);
    }
    public static Result getSuccessResult(String message){
        return new Result()
                .setCode(ResultCode.SUCCESS)
                .setMessage(message);
    }
    public static Result getFailResult(String message){
        return new Result()
                .setCode(ResultCode.FAIL)
                .setMessage(message);
    }

    public static Result getUnauthorizedResult(String message){
        return new Result()
                .setCode(ResultCode.UNAUTHORIZED)
                .setMessage(message);
    }
    public static Result getForbiddenResult(String message){
        return new Result()
                .setCode(ResultCode.FORBIDDEN)
                .setMessage(message);
    }
    public static Result getCodeAndResult(Integer errCode,String message){
        return new Result()
                .setCode(errCode)
                .setMessage(message);
    }

}
