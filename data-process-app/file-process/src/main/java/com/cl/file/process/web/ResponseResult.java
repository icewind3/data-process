package com.cl.file.process.web;

/**
 * @author yejianyu
 * @date 2019/7/5
 */
public class ResponseResult {

    public static final Integer CODE_SUCCESS = 0;
    public static final Integer CODE_ERROR = -1;

    public static final ResponseResult SUCCESS = new ResponseResult(null);

    private Integer code;
    private String msg;
    private Object data;

    public static ResponseResult build(Integer code, String msg, Object data) {
        return new ResponseResult(code, msg, data);
    }

    public static ResponseResult success(Object data) {
        return new ResponseResult(data);
    }

    public static ResponseResult error(String msg) {
        return new ResponseResult(CODE_ERROR, msg, null);
    }

    private ResponseResult(Integer code, String msg, Object data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }

    private ResponseResult(Object data) {
        this.code = ResponseResult.CODE_SUCCESS;
        this.msg = "OK";
        this.data = data;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
