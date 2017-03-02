/*
    Copyright 2017, Google, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/
package com.google.demo.pubsub.publisher;

import java.util.*;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class Publisher {

    private static final String TOPIC_NAME = "publisher-dataflow-bigquery";

    private static DateTimeFormatter fmt =
            DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS")
                    .withZone(DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC")));

    private static Random random = new Random();

    private static final List<String> REGIONS = new ArrayList<>(Arrays.asList("Europe", "Asia"));
    private static final Map<String, List<String>> REGIONS_TO_SERVERS = new HashMap<>();

    static {
        REGIONS_TO_SERVERS.put("Europe", Arrays.asList("A", "B"));
        REGIONS_TO_SERVERS.put("Asia", Arrays.asList("C", "D"));
    }

    private static final int RANDOM_CAP = 10;

    public void publishIndefinitely(long milliseconds) {
        try {
            PublisherService publisherService = new PublisherService(TOPIC_NAME);
            for (int i = 0; i < 3; i++) {
                Long currentTime = System.currentTimeMillis();
                String eventTimeString = Long.toString(currentTime);
                publisherService.publish(generateMessage(currentTime), eventTimeString);

                try {
                    Thread.sleep(milliseconds);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch(Throwable throwable) {
            throwable.printStackTrace();
            throw new RuntimeException(throwable);
        }
    }

    public String generateMessage(long timestamp) {
        return randomElement() + "," + random.nextInt(RANDOM_CAP) + "," + generateTimestamp(timestamp);
    }

    /** Utility to grab a random element from an array of Strings. */
    private String randomElement() {
        int regionIndex = random.nextInt(REGIONS.size());
        String region = REGIONS.get(regionIndex);

        List<String> servers = REGIONS_TO_SERVERS.get(region);
        int serverIndex = random.nextInt(servers.size());
        String server = servers.get(serverIndex);

        return region + "," + server;
    }

    private String generateTimestamp(long currentTime) {
        // Add a (redundant) 'human-readable' date string to make the data semantics more clear.
        return fmt.print(currentTime);

    }
}
