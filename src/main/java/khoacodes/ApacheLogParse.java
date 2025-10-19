package khoacodes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Example:
// 64.242.88.10 - - [07/Mar/2004:16:05:49 -0800] "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1" 401 12846

public class ApacheLogParse {
    private static final String LOG_PATTERN = 
        "^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] \"([^\"]+)\" (\\d{3}) (\\S+)";

    // Capture the format of Apache log format
    private static final Pattern pattern = Pattern.compile(LOG_PATTERN);
    
    private String host; // 64.242.88.10
    private String timestamp; // 07/Mar/2004:16:05:49 -0800
    private String request; // = "GET /twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables HTTP/1.1"
    private String method; // = "GET"
    private String url; // = "/twiki/bin/edit/Main/Double_bounce_sender?topicparent=Main.ConfigurationVariables"
    private String protocol; // = "HTTP/1.1"
    private int statusCode; // = 401
    private long bytes; // = 12846

    public boolean parse(String logline)
    {
        Matcher matcher = pattern.matcher(logline);
        
        if (!matcher.matches()) {
            return false;
}
        this.host = matcher.group(1);
        this.timestamp = matcher.group(4);
        this.request = matcher.group(5);
        this.statusCode = Integer.parseInt(matcher.group(6));
        String byteStr = matcher.group(7);

        if (byteStr.equals("-")) {
            this.bytes = 0;
        } else {
            this.bytes = Long.parseLong(byteStr);
        }
        
        String[] parts = this.request.split(" ");
        if (parts.length >= 3) {
            this.method = parts[0];
            this.url = parts[1];
            this.protocol = parts[2];
        } else if (parts.length >= 2) {
            this.method = parts[0];
            this.url = parts[1];
            this.protocol = "";
        }

        return true;
    }

    public String getHost() { return host; }
    public String getTimestamp() { return timestamp; }
    public String getRequest() { return request; }
    public String getMethod() { return method; }
    public String getUrl() { return url; }
    public String getProtocol() { return protocol; }
    public int getStatusCode() { return statusCode; }
    public long getBytes() { return bytes; }

    public String getYearMonth()
    {
        String[] parts = timestamp.split("[/:]");
        String day = parts[0];
        String month = parts[1];
        String year = parts[2];

        int monthNum = getMonthNumber(month);
        return String.format("%s-%02d-%s", year, monthNum, day);
    }

    public String getDate()
    {
        String[] parts = timestamp.split("[/:]");

        String day = parts[0];
        String month = parts[1];
        String year = parts[2];

        int monthNum = getMonthNumber(month);
        return String.format("%s-%02d-%s", year, monthNum, day);
    }

    private int getMonthNumber(String monthName) {
        switch (monthName) {
            case "Jan": return 1;
            case "Feb": return 2;
            case "Mar": return 3;
            case "Apr": return 4;
            case "May": return 5;
            case "Jun": return 6;
            case "Jul": return 7;
            case "Aug": return 8;
            case "Sep": return 9;
            case "Oct": return 10;
            case "Nov": return 11;
            case "Dec": return 12;
            default: return 0;
        }
}
}