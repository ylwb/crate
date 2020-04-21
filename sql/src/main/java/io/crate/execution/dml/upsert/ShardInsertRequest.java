/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.dml.upsert;

import io.crate.Streamer;
import io.crate.common.collections.EnumSets;
import io.crate.execution.dml.ShardRequest;
import io.crate.execution.dml.upsert.ShardWriteRequest.DuplicateKeyAction;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.settings.SessionSettings;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.UUID;

public final class ShardInsertRequest extends ShardRequest<ShardInsertRequest, ShardInsertRequest.Item> {

    private SessionSettings sessionSettings;

    /**
     * List of references used on insert
     */
    private ColumnIdent[] insertColumns;

    private DataType<?>[] dataTypes;

    private EnumSet<Property> properties;

    public ShardInsertRequest(
        ShardId shardId,
        UUID jobId,
        SessionSettings sessionSettings,
        Reference[] references,
        boolean continueOnError,
        boolean validateGeneratedColumns,
        DuplicateKeyAction duplicateKeyAction
    ) {
        super(shardId, jobId);
        this.sessionSettings = sessionSettings;
        this.insertColumns = new ColumnIdent[references.length];
        this.dataTypes = new DataType[references.length];
        for (int i = 0; i < references.length; i++) {
            insertColumns[i] = references[i].column();
            dataTypes[i] = references[i].valueType();
        }
        this.properties = Property.toEnumSet(continueOnError, validateGeneratedColumns, duplicateKeyAction);
    }

    public ShardInsertRequest(StreamInput in) throws IOException {
        super(in);
        int missingAssignmentsColumnsSize = in.readVInt();
        if (missingAssignmentsColumnsSize > 0) {
            insertColumns = new ColumnIdent[missingAssignmentsColumnsSize];
            for (int i = 0; i < missingAssignmentsColumnsSize; i++) {
                insertColumns[i] = new ColumnIdent(in);
            }
        }
        int dataTypesSize = in.readVInt();
        Streamer[] insertValuesStreamer = new Streamer[dataTypesSize];
        if (dataTypesSize > 0) {
            dataTypes = new DataType<?>[dataTypesSize];
            for (int i = 0; i < dataTypesSize; i++) {
                DataType<?> dataType = DataTypes.fromStream(in);
                insertValuesStreamer[i] = dataType.streamer();
                dataTypes[i] = dataType;
            }
        }
        properties = EnumSets.unpackFromInt(in.readVInt(), Property.class);
        sessionSettings = new SessionSettings(in);
        int numItems = in.readVInt();
        items = new ArrayList<>(numItems);
        for (int i = 0; i < numItems; i++) {
            items.add(new ShardInsertRequest.Item(in, insertValuesStreamer));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (insertColumns != null) {
            out.writeVInt(insertColumns.length);
            for (ColumnIdent columns : insertColumns) {
                columns.writeTo(out);
            }
        } else {
            out.writeVInt(0);
        }
        Streamer[] insertValuesStreamer = null;
        if (dataTypes != null) {
            insertValuesStreamer = new Streamer[insertColumns.length];
            out.writeVInt(dataTypes.length);
            if (dataTypes.length > 0) {
                for (int i = 0; i < dataTypes.length; i++) {
                    DataTypes.toStream(dataTypes[i], out);
                    insertValuesStreamer[i] = dataTypes[i].streamer();
                }
            }
        } else {
            out.writeVInt(0);
        }

        out.writeVInt(EnumSets.packToInt(properties));
        sessionSettings.writeTo(out);
        out.writeVInt(items.size());
        for (ShardInsertRequest.Item item : items) {
            item.writeTo(out, insertValuesStreamer);
        }
    }

    public SessionSettings sessionSettings() {
        return sessionSettings;
    }

    @Nullable
    public String[] updateColumns() {
        return null;
    }

    public ColumnIdent[] insertColumns() {
        return insertColumns;
    }

    public boolean continueOnError() {
        return Property.continueOnError(properties);
    }

    public boolean validateConstraints() {
        return Property.validateConstraints(properties);
    }

    public ShardWriteRequest.DuplicateKeyAction duplicateKeyAction() {
        return Property.duplicationAction(properties);
    }

    // Property is only used for internal storage and serialization
    private enum Property {
        DUPLICATE_KEY_UPDATE_OR_FAIL,
        DUPLICATE_KEY_OVERWRITE,
        DUPLICATE_KEY_IGNORE,
        CONTINUE_ON_ERROR,
        VALIDATE_CONSTRAINTS;

        static EnumSet<Property> toEnumSet(boolean continueOnError, boolean validateConstraints, DuplicateKeyAction action) {
            ArrayList<Property> properties = new ArrayList<>(3);
            if (continueOnError) {
                properties.add(Property.CONTINUE_ON_ERROR);
            }
            if (validateConstraints) {
                properties.add(Property.VALIDATE_CONSTRAINTS);
            }
            switch (action) {
                case IGNORE:
                    properties.add(Property.DUPLICATE_KEY_IGNORE);
                    break;
                case OVERWRITE:
                    properties.add(Property.DUPLICATE_KEY_OVERWRITE);
                    break;
                case UPDATE_OR_FAIL:
                    properties.add(Property.DUPLICATE_KEY_UPDATE_OR_FAIL);
                    break;
                default:
                    throw new IllegalArgumentException("DuplicateKeyAction not supported for serialization: " + action.name());
            }
            return EnumSet.copyOf(properties);
        }

        static DuplicateKeyAction duplicationAction(EnumSet<Property> values) {
            if (values.contains(Property.DUPLICATE_KEY_UPDATE_OR_FAIL)) {
                return DuplicateKeyAction.UPDATE_OR_FAIL;
            }
            if (values.contains(Property.DUPLICATE_KEY_OVERWRITE)) {
                return DuplicateKeyAction.OVERWRITE;
            }
            if (values.contains(Property.DUPLICATE_KEY_IGNORE)) {
                return DuplicateKeyAction.IGNORE;
            }
            throw new IllegalArgumentException("DuplicateKeyAction not found");
        }

        static boolean continueOnError(EnumSet<Property> values) {
            return values.contains(Property.CONTINUE_ON_ERROR);
        }

        static boolean validateConstraints(EnumSet<Property> values) {
            return values.contains(Property.VALIDATE_CONSTRAINTS);
        }
    }

    /**
     * A single insert item.
     */
    public static final class Item extends ShardRequest.Item {

        @Nullable
        protected BytesReference source;

        /**
         * List of objects used on insert
         */
        @Nullable
        private Object[] insertValues;

        public Item(
            String id,
            Object[] insertValues,
            @Nullable Long version,
            @Nullable Long seqNo,
            @Nullable Long primaryTerm
        ) {
            super(id);
            if (version != null) {
                this.version = version;
            }
            if (seqNo != null) {
                this.seqNo = seqNo;
            }
            if (primaryTerm != null) {
                this.primaryTerm = primaryTerm;
            }
            this.insertValues = insertValues;
        }

        @Nullable
        public BytesReference source() {
            return source;
        }

        public void source(BytesReference source) {
            this.source = source;
        }

        @Nullable
        public Object[] insertValues() {
            return insertValues;
        }

        public Item(StreamInput in, Streamer[] insertValueStreamers) throws IOException {
            super(in);
            int missingAssignmentsSize = in.readVInt();
            if (missingAssignmentsSize > 0) {
                assert insertValueStreamers != null : "streamers are required if reading insert values";
                this.insertValues = new Object[missingAssignmentsSize];
                for (int i = 0; i < missingAssignmentsSize; i++) {
                    insertValues[i] = insertValueStreamers[i].readValueFrom(in);
                }
            }
            if (in.readBoolean()) {
                source = in.readBytesReference();
            }
        }

        public void writeTo(StreamOutput out, Streamer[] insertValueStreamers) throws IOException {
            super.writeTo(out);
            assert insertValueStreamers != null : "streamers are required to stream insert values";
            out.writeVInt(insertValues.length);
            for (int i = 0; i < insertValues.length; i++) {
                insertValueStreamers[i].writeValueTo(out, insertValues[i]);
            }
            boolean sourceAvailable = source != null;
            out.writeBoolean(sourceAvailable);
            if (sourceAvailable) {
                out.writeBytesReference(source);
            }
        }
    }

    public static class Builder {

        private final SessionSettings sessionSettings;
        private final TimeValue timeout;
        private final Reference[] insertColumns;
        private final UUID jobId;
        private final boolean validateGeneratedColumns;
        private final boolean continueOnError;
        private final DuplicateKeyAction duplicateKeyAction;

        public Builder(
            SessionSettings sessionSettings,
            TimeValue timeout,
            ShardWriteRequest.DuplicateKeyAction duplicateKeyAction,
            boolean continueOnError,
            Reference[] insertColumns,
            UUID jobId,
            boolean validateGeneratedColumns) {
            this.sessionSettings = sessionSettings;
            this.timeout = timeout;
            this.insertColumns = insertColumns;
            this.jobId = jobId;
            this.validateGeneratedColumns = validateGeneratedColumns;
            this.continueOnError = continueOnError;
            this.duplicateKeyAction = duplicateKeyAction;
        }

        public ShardInsertRequest newRequest(ShardId shardId) {
            return new ShardInsertRequest(
                shardId,
                jobId,
                sessionSettings,
                insertColumns,
                continueOnError,
                validateGeneratedColumns,
                duplicateKeyAction
            ).timeout(timeout);
        }
    }
}
