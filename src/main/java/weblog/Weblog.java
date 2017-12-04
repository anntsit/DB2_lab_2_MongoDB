package weblog;

import lombok.*;

import java.sql.Timestamp;

@Getter
@Setter
public class Weblog {
    private String URL;
    private String IP;
    private Timestamp timeStamp;
    private long timeSpent;


}
