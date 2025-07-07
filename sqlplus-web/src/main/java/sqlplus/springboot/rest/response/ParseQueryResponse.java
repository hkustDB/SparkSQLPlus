package sqlplus.springboot.rest.response;

import sqlplus.springboot.rest.object.*;

import java.util.ArrayList;
import java.util.List;

public class ParseQueryResponse {
    long time;
    long size;

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}


