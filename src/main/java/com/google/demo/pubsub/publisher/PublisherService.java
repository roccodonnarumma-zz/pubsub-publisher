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

import com.google.cloud.pubsub.deprecated.*;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

public class PublisherService {

    private Logger logger = LogManager.getLogger();

    private PubSub pubsub;
    private Topic topic;

    public PublisherService(String topicName) throws IOException {
        pubsub = PubSubOptions.getDefaultInstance().getService();
        topic = createTopic(topicName);
    }

    public void publish(String message) {
        logger.log(Level.INFO, String.format("Publishing message: %s", message));

        topic.publish(Message.of(message));
    }

    private Topic createTopic(String topicName) {
        Topic topic = pubsub.getTopic(topicName);
        if(topic != null) {
            return topic;
        }

        logger.log(Level.INFO, String.format("Creating topic: %s", topicName));
        return pubsub.create(TopicInfo.of(topicName));
    }
}
