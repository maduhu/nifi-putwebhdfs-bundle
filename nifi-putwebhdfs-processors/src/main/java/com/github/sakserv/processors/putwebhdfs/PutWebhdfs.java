/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.sakserv.processors.putwebhdfs;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

// Tags are searchable in the Processor dialog
@Tags({"hdfs", "hadoop", "put", "http"})

// The Capability Description is a short description for display in the UI
@CapabilityDescription("Writes FlowFiles to HDFS using WebHDFS")

public class PutWebHDFS extends AbstractProcessor {

    // Variables set via init
    private String webHdfsUrl;
    private String webHdfsUser;

    // Properties for this Processor
    public static final PropertyDescriptor WEBHDFS_BASE_URL = new PropertyDescriptor
            .Builder().name("WebHDFS Base URL")
            .description("Base URL for WebHDFS: example: http://localhost:50070/webhdfs/v1")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WEBHDFS_USER = new PropertyDescriptor
            .Builder().name("WebHDFS User")
            .description("Username to use as the authenticated user")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WEBHDFS_OUTPUT_DIRECTORY = new PropertyDescriptor
            .Builder().name("Output Directory")
            .description("Output Directory on HDFS where files will be stored")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Releationships for this Processor
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FIles that have been successfully written to HDFS are transferred to this relationship")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Files that could not be written to HDFS via WebHDFS are transferred to this relationship")
            .build();


    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(WEBHDFS_BASE_URL);
        descriptors.add(WEBHDFS_USER);
        descriptors.add(WEBHDFS_OUTPUT_DIRECTORY);
        this.properties = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

        final String webHdfsBaseUrl = context.getProperty(WEBHDFS_BASE_URL).getValue();
        webHdfsUser = context.getProperty(WEBHDFS_USER).getValue();
        final String webHdfsOutputDir = context.getProperty(WEBHDFS_OUTPUT_DIRECTORY).getValue();

        StringBuilder sb = new StringBuilder();
        sb.append(webHdfsBaseUrl);
        sb.append(webHdfsOutputDir);
        webHdfsUrl = sb.toString();

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final FlowFile flowFile = session.get();
		if ( flowFile == null ) {
			return;
		}

        // Variables
        final ProcessorLog log = this.getLogger();
        final String flowFileFileName = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        StringBuilder sb = new StringBuilder();
        sb.append(webHdfsUrl);
        sb.append("/");
        sb.append(flowFileFileName);
        final String fullUrlWithFlowFile = sb.toString();

        // Read the FlowFile
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream inputStream) throws IOException {
                StreamUtils.fillBuffer(inputStream, messageContent, true);
            }
        });

        // Setup the query strings for the request
        Map<String, Object> queryStrings = new HashMap<String, Object>();
        queryStrings.put("user.name", webHdfsUser);
        queryStrings.put("op", "homedir");

        try {
            // Make the request and succeed if not exception - VERY BAD IDEA AGAIN, CHECK REPONSE AT LEAST FFS!
            Unirest.put(fullUrlWithFlowFile)
                    .queryString(queryStrings)
                    .body(messageContent)
                    .asString();

            // Exception encounter, transfer to the failure relationship
            log.info("NIFI: Success! Transferring to the success relationship.");
            session.transfer(flowFile, REL_SUCCESS);

        } catch (UnirestException e) {
            // Exception encounter, transfer to the failure relationship
            log.info("NIFI: Failure! Transferring to the failure relationship.");
            log.error(e.getLocalizedMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

}
