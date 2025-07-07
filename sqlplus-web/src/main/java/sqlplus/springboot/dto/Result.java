package sqlplus.springboot.dto;

public class Result {
    public final static String SUCCESS = "success";
    public final static String FALLBACK = "fallback";
    public final static String FAIL = "fail";

    private int code;
    private String message = SUCCESS;
    private Object data;

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
