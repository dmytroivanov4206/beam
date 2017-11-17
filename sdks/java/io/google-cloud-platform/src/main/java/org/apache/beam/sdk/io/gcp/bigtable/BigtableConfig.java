/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.bigtable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.config.CredentialOptions;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Configuration for a Cloud Bigtable client.
 */
@AutoValue
abstract class BigtableConfig implements Serializable {

  /**
   * Modification function for Bigtable Options.
   */
  abstract SerializableFunction<BigtableOptions, BigtableOptions> getBigtableOptionsModificationFunction();

  /**
   * Returns the options for Bigtable.
   */
  @Nullable
  abstract ValueProvider<BigtableOptions> getBigtableOptions();

  /**
   * Returns the project id being written to.
   */
  @Nullable
  abstract ValueProvider<String> getProjectId();

  /**
   * Returns the innstance id being written to.
   */
  @Nullable
  abstract ValueProvider<String> getInstanceId();

  /**
   * Returns the table id being written to.
   */
  @Nullable
  abstract ValueProvider<String> getTableId();

  /**
   * Weather validate that table exists before writing.
   */
  @Nullable
  abstract ValueProvider<Boolean> getValidation();

  /**
   * For testing purposes only.
   */
  @Nullable
  abstract BigtableService getBigtableService();

  abstract Builder toBuilder();

  static BigtableConfig create() {
    return builder().build();
  }

  static Builder builder() {
    return new AutoValue_BigtableConfig.Builder();
  }

  void validate() {
    checkNotNull(
        getTableId(),
        "BigtableIO requires table id to be set with withTableId method");
    checkArgument(getTableId().isAccessible(), "bigtable table is not accessible");
    checkNotNull(getTableId().get(), "bigtable options could not be null");

    boolean bigtableOptionsAvailable = getBigtableOptions() != null && getBigtableOptions().isAccessible();
    boolean projectIdAvailable = getProjectId() != null && getProjectId().isAccessible();
    boolean instanceIdAvailable = getInstanceId() != null && getInstanceId().isAccessible();

    checkArgument(!projectIdAvailable
            && (!bigtableOptionsAvailable || getBigtableOptions().get().getProjectId() == null),
        "Could not obtain project id");

    checkArgument(!instanceIdAvailable
            && (!bigtableOptionsAvailable || getBigtableOptions().get().getInstanceId() == null),
        "Could not obtain instance id");
  }

  void populateDisplayData(DisplayData.Builder builder) {
    builder
        .addIfNotNull(DisplayData.item("projectId", getProjectId()).withLabel("Project ID"))
        .addIfNotNull(DisplayData.item("instanceId", getInstanceId()).withLabel("Instance ID"))
        .addIfNotNull(DisplayData.item("tableId", getTableId()).withLabel("Table ID"))
        .addIfNotNull(DisplayData.item("bigtableOptions", getBigtableOptions()).withLabel("Bigtable Options"));
  }

  /**
   * Builder for {@link BigtableConfig}.
   */
  @AutoValue.Builder
  abstract static class Builder {

    abstract Builder setBigtableOptionsModificationFunction(SerializableFunction<BigtableOptions, BigtableOptions> fun);

    abstract Builder setBigtableOptions(ValueProvider<BigtableOptions> options);

    abstract Builder setProjectId(ValueProvider<String> projectId);

    abstract Builder setInstanceId(ValueProvider<String> instanceId);

    abstract Builder setTableId(ValueProvider<String> tableId);

    abstract Builder setValidation(ValueProvider<Boolean> isEnabled);

    abstract Builder setBigtableService(BigtableService bigtableService);

    abstract BigtableConfig build();
  }

  BigtableConfig withBigtableOptionsModificationFunction(SerializableFunction<BigtableOptions, BigtableOptions> fun) {
    return toBuilder().setBigtableOptionsModificationFunction(fun).build();
  }

  BigtableConfig withBigtableOptions(ValueProvider<BigtableOptions> options) {
    return toBuilder().setBigtableOptions(options).build();
  }

  BigtableConfig withBigtableOptions(BigtableOptions options) {
    return withBigtableOptions(ValueProvider.StaticValueProvider.of(options));
  }

  BigtableConfig withProjectId(ValueProvider<String> projectId) {
    return toBuilder().setProjectId(projectId).build();
  }

  BigtableConfig withProjectId(String projectId) {
    return withProjectId(ValueProvider.StaticValueProvider.of(projectId));
  }

  BigtableConfig withInstanceId(ValueProvider<String> instanceId) {
    return toBuilder().setInstanceId(instanceId).build();
  }

  BigtableConfig withInstanceId(String instanceId) {
    return withInstanceId(ValueProvider.StaticValueProvider.of(instanceId));
  }

  BigtableConfig withTableId(ValueProvider<String> tableId) {
    return toBuilder().setTableId(tableId).build();
  }

  BigtableConfig withTableId(String tableId) {
    return withTableId(ValueProvider.StaticValueProvider.of(tableId));
  }

  BigtableConfig withValidation(ValueProvider<Boolean> isEnabled) {
    return toBuilder().setValidation(isEnabled).build();
  }

  BigtableConfig withValidation(boolean isEnabled) {
    return withValidation(ValueProvider.StaticValueProvider.of(isEnabled));
  }

  @VisibleForTesting
  BigtableConfig withBigtableService(BigtableService bigtableService) {
    return toBuilder().setBigtableService(bigtableService).build();
  }

  /**
   * Helper function that either returns the mock Bigtable service supplied by
   * {@link #withBigtableService} or creates and returns an implementation that talks to
   * {@code Cloud Bigtable}.
   * <p>
   * <p>Also populate the credentials option from {@link GcpOptions#getGcpCredential()} if the
   * default credentials are being used on {@link BigtableOptions}.
   */
  @VisibleForTesting
  BigtableService getBigtableService(PipelineOptions pipelineOptions) {
    if (getBigtableService() != null) {
      return getBigtableService();
    }

    BigtableOptions options = getBigtableOptions() == null || !getBigtableOptions().isAccessible()
        ? new BigtableOptions.Builder().build()
        : getBigtableOptions().get().toBuilder().build();

    BigtableOptions.Builder clonedOptions = options.toBuilder()
        .setUserAgent(pipelineOptions.getUserAgent());

    if (getProjectId() != null && getProjectId().isAccessible()) {
      clonedOptions.setProjectId(getProjectId().get());
    }

    if (getInstanceId() != null && getProjectId().isAccessible()) {
      clonedOptions.setInstanceId(getInstanceId().get());
    }

    if (options.getCredentialOptions()
        .getCredentialType() == CredentialOptions.CredentialType.DefaultCredentials) {
      clonedOptions.setCredentialOptions(
          CredentialOptions.credential(
              pipelineOptions.as(GcpOptions.class).getGcpCredential()));
    }

    return new BigtableServiceImpl(getBigtableOptionsModificationFunction().apply(clonedOptions.build()));
  }
}
