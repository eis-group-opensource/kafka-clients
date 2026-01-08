/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// THIS CODE IS AUTOMATICALLY GENERATED.  DO NOT EDIT.

package org.apache.kafka.common.message;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Message;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.apache.kafka.common.protocol.types.RawTaggedFieldWriter;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.apache.kafka.common.utils.ByteUtils;


public class DescribeClientQuotasRequestData implements ApiMessage {
    private List<ComponentData> components;
    private boolean strict;
    private List<RawTaggedField> _unknownTaggedFields;
    
    public static final Schema SCHEMA_0 =
        new Schema(
            new Field("components", new ArrayOf(ComponentData.SCHEMA_0), "Filter components to apply to quota entities."),
            new Field("strict", Type.BOOLEAN, "Whether the match is strict, i.e. should exclude entities with unspecified entity types.")
        );
    
    public static final Schema[] SCHEMAS = new Schema[] {
        SCHEMA_0
    };
    
    public DescribeClientQuotasRequestData(Readable _readable, short _version) {
        read(_readable, _version);
    }
    
    public DescribeClientQuotasRequestData(Struct _struct, short _version) {
        fromStruct(_struct, _version);
    }
    
    public DescribeClientQuotasRequestData() {
        this.components = new ArrayList<ComponentData>(0);
        this.strict = false;
    }
    
    @Override
    public short apiKey() {
        return 48;
    }
    
    @Override
    public short lowestSupportedVersion() {
        return 0;
    }
    
    @Override
    public short highestSupportedVersion() {
        return 0;
    }
    
    @Override
    public void read(Readable _readable, short _version) {
        {
            int arrayLength;
            arrayLength = _readable.readInt();
            if (arrayLength < 0) {
                throw new RuntimeException("non-nullable field components was serialized as null");
            } else {
                ArrayList<ComponentData> newCollection = new ArrayList<ComponentData>(arrayLength);
                for (int i = 0; i < arrayLength; i++) {
                    newCollection.add(new ComponentData(_readable, _version));
                }
                this.components = newCollection;
            }
        }
        this.strict = _readable.readByte() != 0;
        this._unknownTaggedFields = null;
    }
    
    @Override
    public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
        int _numTaggedFields = 0;
        _writable.writeInt(components.size());
        for (ComponentData componentsElement : components) {
            componentsElement.write(_writable, _cache, _version);
        }
        _writable.writeByte(strict ? (byte) 1 : (byte) 0);
        RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
        _numTaggedFields += _rawWriter.numFields();
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void fromStruct(Struct struct, short _version) {
        this._unknownTaggedFields = null;
        {
            Object[] _nestedObjects = struct.getArray("components");
            this.components = new ArrayList<ComponentData>(_nestedObjects.length);
            for (Object nestedObject : _nestedObjects) {
                this.components.add(new ComponentData((Struct) nestedObject, _version));
            }
        }
        this.strict = struct.getBoolean("strict");
    }
    
    @Override
    public Struct toStruct(short _version) {
        TreeMap<Integer, Object> _taggedFields = null;
        Struct struct = new Struct(SCHEMAS[_version]);
        {
            Struct[] _nestedObjects = new Struct[components.size()];
            int i = 0;
            for (ComponentData element : this.components) {
                _nestedObjects[i++] = element.toStruct(_version);
            }
            struct.set("components", (Object[]) _nestedObjects);
        }
        struct.set("strict", this.strict);
        return struct;
    }
    
    @Override
    public int size(ObjectSerializationCache _cache, short _version) {
        int _size = 0, _numTaggedFields = 0;
        {
            int _arraySize = 0;
            _arraySize += 4;
            for (ComponentData componentsElement : components) {
                _arraySize += componentsElement.size(_cache, _version);
            }
            _size += _arraySize;
        }
        _size += 1;
        if (_unknownTaggedFields != null) {
            _numTaggedFields += _unknownTaggedFields.size();
            for (RawTaggedField _field : _unknownTaggedFields) {
                _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                _size += _field.size();
            }
        }
        if (_numTaggedFields > 0) {
            throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
        }
        return _size;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DescribeClientQuotasRequestData)) return false;
        DescribeClientQuotasRequestData other = (DescribeClientQuotasRequestData) obj;
        if (this.components == null) {
            if (other.components != null) return false;
        } else {
            if (!this.components.equals(other.components)) return false;
        }
        if (strict != other.strict) return false;
        return true;
    }
    
    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + (components == null ? 0 : components.hashCode());
        hashCode = 31 * hashCode + (strict ? 1231 : 1237);
        return hashCode;
    }
    
    @Override
    public DescribeClientQuotasRequestData duplicate() {
        DescribeClientQuotasRequestData _duplicate = new DescribeClientQuotasRequestData();
        ArrayList<ComponentData> newComponents = new ArrayList<ComponentData>(components.size());
        for (ComponentData _element : components) {
            newComponents.add(_element.duplicate());
        }
        _duplicate.components = newComponents;
        _duplicate.strict = strict;
        return _duplicate;
    }
    
    @Override
    public String toString() {
        return "DescribeClientQuotasRequestData("
            + "components=" + MessageUtil.deepToString(components.iterator())
            + ", strict=" + (strict ? "true" : "false")
            + ")";
    }
    
    public List<ComponentData> components() {
        return this.components;
    }
    
    public boolean strict() {
        return this.strict;
    }
    
    @Override
    public List<RawTaggedField> unknownTaggedFields() {
        if (_unknownTaggedFields == null) {
            _unknownTaggedFields = new ArrayList<>(0);
        }
        return _unknownTaggedFields;
    }
    
    public DescribeClientQuotasRequestData setComponents(List<ComponentData> v) {
        this.components = v;
        return this;
    }
    
    public DescribeClientQuotasRequestData setStrict(boolean v) {
        this.strict = v;
        return this;
    }
    
    static public class ComponentData implements Message {
        private String entityType;
        private byte matchType;
        private String match;
        private List<RawTaggedField> _unknownTaggedFields;
        
        public static final Schema SCHEMA_0 =
            new Schema(
                new Field("entity_type", Type.STRING, "The entity type that the filter component applies to."),
                new Field("match_type", Type.INT8, "How to match the entity {0 = exact name, 1 = default name, 2 = any specified name}."),
                new Field("match", Type.NULLABLE_STRING, "The string to match against, or null if unused for the match type.")
            );
        
        public static final Schema[] SCHEMAS = new Schema[] {
            SCHEMA_0
        };
        
        public ComponentData(Readable _readable, short _version) {
            read(_readable, _version);
        }
        
        public ComponentData(Struct _struct, short _version) {
            fromStruct(_struct, _version);
        }
        
        public ComponentData() {
            this.entityType = "";
            this.matchType = (byte) 0;
            this.match = "";
        }
        
        
        @Override
        public short lowestSupportedVersion() {
            return 0;
        }
        
        @Override
        public short highestSupportedVersion() {
            return 0;
        }
        
        @Override
        public void read(Readable _readable, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of ComponentData");
            }
            {
                int length;
                length = _readable.readShort();
                if (length < 0) {
                    throw new RuntimeException("non-nullable field entityType was serialized as null");
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field entityType had invalid length " + length);
                } else {
                    this.entityType = _readable.readString(length);
                }
            }
            this.matchType = _readable.readByte();
            {
                int length;
                length = _readable.readShort();
                if (length < 0) {
                    this.match = null;
                } else if (length > 0x7fff) {
                    throw new RuntimeException("string field match had invalid length " + length);
                } else {
                    this.match = _readable.readString(length);
                }
            }
            this._unknownTaggedFields = null;
        }
        
        @Override
        public void write(Writable _writable, ObjectSerializationCache _cache, short _version) {
            int _numTaggedFields = 0;
            {
                byte[] _stringBytes = _cache.getSerializedValue(entityType);
                _writable.writeShort((short) _stringBytes.length);
                _writable.writeByteArray(_stringBytes);
            }
            _writable.writeByte(matchType);
            if (match == null) {
                _writable.writeShort((short) -1);
            } else {
                byte[] _stringBytes = _cache.getSerializedValue(match);
                _writable.writeShort((short) _stringBytes.length);
                _writable.writeByteArray(_stringBytes);
            }
            RawTaggedFieldWriter _rawWriter = RawTaggedFieldWriter.forFields(_unknownTaggedFields);
            _numTaggedFields += _rawWriter.numFields();
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public void fromStruct(Struct struct, short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't read version " + _version + " of ComponentData");
            }
            this._unknownTaggedFields = null;
            this.entityType = struct.getString("entity_type");
            this.matchType = struct.getByte("match_type");
            this.match = struct.getString("match");
        }
        
        @Override
        public Struct toStruct(short _version) {
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't write version " + _version + " of ComponentData");
            }
            TreeMap<Integer, Object> _taggedFields = null;
            Struct struct = new Struct(SCHEMAS[_version]);
            struct.set("entity_type", this.entityType);
            struct.set("match_type", this.matchType);
            struct.set("match", this.match);
            return struct;
        }
        
        @Override
        public int size(ObjectSerializationCache _cache, short _version) {
            int _size = 0, _numTaggedFields = 0;
            if (_version > 0) {
                throw new UnsupportedVersionException("Can't size version " + _version + " of ComponentData");
            }
            {
                byte[] _stringBytes = entityType.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'entityType' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(entityType, _stringBytes);
                _size += _stringBytes.length + 2;
            }
            _size += 1;
            if (match == null) {
                _size += 2;
            } else {
                byte[] _stringBytes = match.getBytes(StandardCharsets.UTF_8);
                if (_stringBytes.length > 0x7fff) {
                    throw new RuntimeException("'match' field is too long to be serialized");
                }
                _cache.cacheSerializedValue(match, _stringBytes);
                _size += _stringBytes.length + 2;
            }
            if (_unknownTaggedFields != null) {
                _numTaggedFields += _unknownTaggedFields.size();
                for (RawTaggedField _field : _unknownTaggedFields) {
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.tag());
                    _size += ByteUtils.sizeOfUnsignedVarint(_field.size());
                    _size += _field.size();
                }
            }
            if (_numTaggedFields > 0) {
                throw new UnsupportedVersionException("Tagged fields were set, but version " + _version + " of this message does not support them.");
            }
            return _size;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ComponentData)) return false;
            ComponentData other = (ComponentData) obj;
            if (this.entityType == null) {
                if (other.entityType != null) return false;
            } else {
                if (!this.entityType.equals(other.entityType)) return false;
            }
            if (matchType != other.matchType) return false;
            if (this.match == null) {
                if (other.match != null) return false;
            } else {
                if (!this.match.equals(other.match)) return false;
            }
            return true;
        }
        
        @Override
        public int hashCode() {
            int hashCode = 0;
            hashCode = 31 * hashCode + (entityType == null ? 0 : entityType.hashCode());
            hashCode = 31 * hashCode + matchType;
            hashCode = 31 * hashCode + (match == null ? 0 : match.hashCode());
            return hashCode;
        }
        
        @Override
        public ComponentData duplicate() {
            ComponentData _duplicate = new ComponentData();
            _duplicate.entityType = entityType;
            _duplicate.matchType = matchType;
            if (match == null) {
                _duplicate.match = null;
            } else {
                _duplicate.match = match;
            }
            return _duplicate;
        }
        
        @Override
        public String toString() {
            return "ComponentData("
                + "entityType=" + ((entityType == null) ? "null" : "'" + entityType.toString() + "'")
                + ", matchType=" + matchType
                + ", match=" + ((match == null) ? "null" : "'" + match.toString() + "'")
                + ")";
        }
        
        public String entityType() {
            return this.entityType;
        }
        
        public byte matchType() {
            return this.matchType;
        }
        
        public String match() {
            return this.match;
        }
        
        @Override
        public List<RawTaggedField> unknownTaggedFields() {
            if (_unknownTaggedFields == null) {
                _unknownTaggedFields = new ArrayList<>(0);
            }
            return _unknownTaggedFields;
        }
        
        public ComponentData setEntityType(String v) {
            this.entityType = v;
            return this;
        }
        
        public ComponentData setMatchType(byte v) {
            this.matchType = v;
            return this;
        }
        
        public ComponentData setMatch(String v) {
            this.match = v;
            return this;
        }
    }
}
