package org.maxkons.hadoop_snippets.yarn;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.springframework.http.*;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 *  Helper class for collecting Yarn apps performance(vcore-seconds/Mb-seconds) metrics, May help in the case of a big number of apps.
 */

public class YarnAppMetricsCollector {

    public static final String YARN_APP_REST_ENDPOINT = "http://localhost:8088/ws/v1/cluster/apps";
    public static final ZoneId USER_ZONE_ID = ZoneId.of("Europe/Moscow");
    public static final String OUTPUT_DATE_PATTERN = "yyyy.MM.dd HH:mm:ss";

    private MultiValueMap<String, String> appFilters = new LinkedMultiValueMap<>();
    private MultiValueMap<ADDITIONAL_FILTERS, String> appFiltersAdditional = new LinkedMultiValueMap<>();

    private static final RestTemplate REST_TEMPLATE = new RestTemplate();

    public enum ADDITIONAL_FILTERS {APP_NAME}

    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Example usage

    public static void main(String[] args) throws IOException, YarnException {
        MultiValueMap<String, String> appFilters = new LinkedMultiValueMap<>();
        appFilters.put("startedTimeBegin", ImmutableList.of("" + Date.from(LocalDateTime.of(2016, 10, 12, 0, 50).atZone(USER_ZONE_ID).toInstant()).getTime()));
        appFilters.put("startedTimeEnd",   ImmutableList.of("" + Date.from(LocalDateTime.of(2016, 10, 17, 1, 30).atZone(USER_ZONE_ID).toInstant()).getTime()));
        appFilters.put("user",             ImmutableList.of("userName"));
        appFilters.put("finalStatus",      ImmutableList.of("SUCCEEDED"));
        appFilters.put("states",           ImmutableList.of("FINISHED"));
        appFilters.put("applicationTypes", ImmutableList.of("SPARK"));

        MultiValueMap<ADDITIONAL_FILTERS, String> appFiltersAdditional = new LinkedMultiValueMap<>();
        appFiltersAdditional.put(ADDITIONAL_FILTERS.APP_NAME, ImmutableList.of("appName"));

        new YarnAppMetricsCollector(appFilters, appFiltersAdditional).requestMetricsAndPrintToConsole();
    }

    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> CONSTRUCTORS

    public YarnAppMetricsCollector(MultiValueMap<String, String> appFilters, MultiValueMap<ADDITIONAL_FILTERS, String> appFiltersAdditional) {
        this.appFilters = appFilters;
        this.appFiltersAdditional = appFiltersAdditional;
    }

    public YarnAppMetricsCollector() {
    }

    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> PUBLIC MEMBERS

    public void requestMetricsAndPrintToConsole() {
        List<AppRespHolder> appList = makeYarnHttpCall();
        appList = applyAdditionalFilters(appList);
        long totalMemorySeconds = 0L;
        long totalVcoreSeconds = 0L;
        System.out.println("appName\tstartedTime\telapsedTime\tmemorySeconds\tvcoreSeconds");
        for (AppRespHolder app : appList) {
            totalMemorySeconds += app.memorySeconds;
            totalVcoreSeconds += app.vcoreSeconds;
            String startTime = LocalDateTime.ofInstant(new Date(app.startedTime).toInstant(), USER_ZONE_ID).format(DateTimeFormatter.ofPattern(OUTPUT_DATE_PATTERN));
            System.out.printf("%s\t%s\t%s\t%s\t%s\n", app.name, startTime, app.elapsedTime, app.memorySeconds, app.vcoreSeconds);
        }
        System.out.printf("Total memorySeconds/vcoreSeconds - %s/%s\n", totalMemorySeconds, totalVcoreSeconds);
    }

    public List<AppRespHolder> makeYarnHttpCall() {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Accept", MediaType.APPLICATION_JSON_VALUE);

        ResponseEntity<AppsRespWrapper> resp = REST_TEMPLATE.exchange(
                UriComponentsBuilder.fromHttpUrl(YARN_APP_REST_ENDPOINT).queryParams(appFilters).build().encode().toUri(),
                HttpMethod.GET,
                new HttpEntity<>(headers),
                AppsRespWrapper.class);

        return resp.getBody().apps == null || resp.getBody().apps.app == null
                ? new ArrayList<>()
                : resp.getBody().apps.app;
    }

    public List<AppRespHolder> applyAdditionalFilters(List<AppRespHolder> apps) {
        ArrayList<AppRespHolder> filteredResp = new ArrayList<>();
        for (AppRespHolder currApp: apps) {
            boolean rowNeedToBeFiltered = false;
            for (Map.Entry<ADDITIONAL_FILTERS, List<String>> entry: appFiltersAdditional.entrySet()) {
                for (String filterValue: entry.getValue()) {
                    if (entry.getKey() == ADDITIONAL_FILTERS.APP_NAME && filterValue != null && !filterValue.isEmpty() && !currApp.name.contains(filterValue)) {
                        rowNeedToBeFiltered = true;
                        break;
                    }
                }
                if (rowNeedToBeFiltered) break;
            }
            if (!rowNeedToBeFiltered) {
                filteredResp.add(currApp);
            }
        }
        return filteredResp;
    }

    // >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> PRIVATE MEMBERS

    private static class AppsRespWrapper {
        public AppRespWrapper apps;
    }

    private static class AppRespWrapper {
        public List<AppRespHolder> app;
    }

    private static class AppRespHolder {
        public String name;
        public long memorySeconds, vcoreSeconds, elapsedTime, startedTime;
    }

}
