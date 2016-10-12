package org.maxkons.hadoop_misc.yarn;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 *  Helper class for collecting Yarn apps performance(vcore-seconds/Mb-seconds), May help in the case of a big number of apps.
 */

public class YarnAppMetricsCollector {

    public static final RestTemplate REST_TEMPLATE = new RestTemplate();

    public static final String YARN_APP_REST_ENDPOINT = "http://localhost:8088/ws/v1/cluster/apps";
    public static final ZoneId ZONE_ID = ZoneId.of("Europe/Moscow");
    public static final String DATE_PATTERN = "yyyy.MM.dd HH:mm:ss";

    public static final String FILTER_USER = "userName";
    public static final String FILTER_APP_NAME = "appName";
    public static final String FILTER_STARTED_TIME_BEGIN = "" + Date.from(LocalDateTime.of(2016, 10, 12, 0, 50).atZone(ZONE_ID).toInstant()).getTime();
    public static final String FILTER_STARTED_TIME_END = "" + Date.from(LocalDateTime.of(2016, 10, 12, 1, 30).atZone(ZONE_ID).toInstant()).getTime();
    public static final String FILTER_STATE = "FINISHED";
    public static final String FILTER_APPLICATION_TYPE = "SPARK";
    public static final String FILTER_FINAL_STATUS = "SUCCEEDED";

    public static void main(String[] args) throws IOException, YarnException {
        ArrayList<AppRespHolder> filteredApps = applyFilters(getYarnCallResults());
        long totalMemorySeconds = 0L;
        long totalVcoreSeconds = 0L;
        System.out.println("appName\tstartedTime\telapsedTime\tmemorySeconds\tvcoreSeconds");
        for (AppRespHolder app : filteredApps) {
            totalMemorySeconds += app.memorySeconds;
            totalVcoreSeconds += app.vcoreSeconds;
            String startTime = LocalDateTime.ofInstant(new Date(app.startedTime).toInstant(), ZONE_ID).format(DateTimeFormatter.ofPattern(DATE_PATTERN));
            System.out.println(app.name + "\t" + startTime + "\t" + app.elapsedTime + "\t" + app.memorySeconds + "\t" + app.vcoreSeconds);
        }
        System.out.println("Total\t" + totalMemorySeconds + "\t" + totalVcoreSeconds);
    }

    private static List<AppRespHolder> getYarnCallResults() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);

        UriComponentsBuilder builder = UriComponentsBuilder.fromHttpUrl(YARN_APP_REST_ENDPOINT)
                .queryParam("user", FILTER_USER)
                .queryParam("applicationTypes", FILTER_APPLICATION_TYPE)
                .queryParam("states", FILTER_STATE)
                .queryParam("finalStatus", FILTER_FINAL_STATUS)
                .queryParam("startedTimeBegin", FILTER_STARTED_TIME_BEGIN)
                .queryParam("startedTimeEnd", FILTER_STARTED_TIME_END);

        HttpEntity<?> entity = new HttpEntity<>(headers);

        ResponseEntity<AppRespWrapper> resp = REST_TEMPLATE.exchange(
                builder.build().encode().toUri(),
                HttpMethod.GET,
                entity,
                AppRespWrapper.class);

        return resp.getBody().apps.app;
    }

    private static ArrayList<AppRespHolder> applyFilters(List<AppRespHolder> apps) {
        ArrayList<AppRespHolder> filteredResp = new ArrayList<>();
        for (int i = apps.size() - 1; i >= 0; i--) {
            AppRespHolder currApp = apps.get(i);
            if (!FILTER_APP_NAME.isEmpty() && !currApp.name.contains(FILTER_APP_NAME)) {
                continue;
            }
            filteredResp.add(currApp);
        }
        return filteredResp;
    }

    public static class AppRespWrapper {
        public AppRespWrapper2 apps;
    }

    public static class AppRespWrapper2 {
        public List<AppRespHolder> app;
    }

    public static class AppRespHolder {
        public String name;
        public long memorySeconds, vcoreSeconds, elapsedTime, startedTime;
    }

}
