/**
 * Copyright 2013 Twitter, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.crunch.tools.jsontopology;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.ListIterator;

import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.Version;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.JsonSerializer;
import org.codehaus.jackson.map.Module;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.map.SerializationConfig;
import org.codehaus.jackson.map.SerializerProvider;
import org.codehaus.jackson.map.annotate.JsonSerialize;
import org.codehaus.jackson.map.introspect.BasicBeanDescription;
import org.codehaus.jackson.map.ser.BeanPropertyWriter;
import org.codehaus.jackson.map.ser.BeanSerializerModifier;

import com.twitter.crunch.Node;
import com.twitter.crunch.Selector;

public final class JsonTopologySerializer implements TopologySerializer {
  public void writeTopology(Topology topology, OutputStream os) throws IOException {
    getWriter().writeValue(os, topology);
  }

  public void writeTopology(Topology topology, String path) throws IOException {
    getWriter().writeValue(new File(path), topology);
  }

  private ObjectWriter getWriter() {
    ObjectMapper mapper = new ObjectMapper();

    // omit null fields from serialization
    mapper.setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
    // exclude certain fields and getter methods from node serialization via mixin
    mapper.getSerializationConfig().addMixInAnnotations(Node.class, MixIn.class);
    // register the module that suppresses the failed property if false
    mapper.registerModule(new IsFailedSuppressor());

    return mapper.writer().withDefaultPrettyPrinter();
  }

  private abstract class MixIn {
    @JsonIgnore public abstract long getId();
    @JsonIgnore public abstract Node getParent();
    @JsonIgnore public abstract boolean isLeaf();
    @JsonIgnore public abstract Selector getSelector();
    @JsonIgnore public abstract List<Node> getAllLeafNodes();
    @JsonIgnore public abstract int getChildrenCount();
    @JsonIgnore public abstract Node getRoot();
  }

  private static class IsFailedSuppressor extends Module {
    public String getModuleName() {
      return "IsFailedSuppressor";
    }

    public Version version() {
      return new Version(1, 0, 0, null);
    }

    public void setupModule(SetupContext context) {
      context.addBeanSerializerModifier(new BeanSerializerModifier() {
        @Override
        public List<BeanPropertyWriter> changeProperties(SerializationConfig config,
          BasicBeanDescription beanDesc, List<BeanPropertyWriter> beanProperties) {
          ListIterator<BeanPropertyWriter> it = beanProperties.listIterator();
          while (it.hasNext()) {
            BeanPropertyWriter writer = it.next();
            // replace the bean writer with my own if it is for "failed"
            if (writer.getName().equals("failed")) {
              BeanPropertyWriter newWriter = new IsFailedWriter(writer);
              it.set(newWriter);
            }
          }
          return beanProperties;
        }
      });
    }
  }

  private static class IsFailedWriter extends BeanPropertyWriter {
    public IsFailedWriter(BeanPropertyWriter base) {
      super(base);
    }

    public IsFailedWriter(BeanPropertyWriter base, JsonSerializer<Object> ser) {
      super(base, ser);
    }

    @Override
    public void serializeAsField(Object bean, JsonGenerator jgen, SerializerProvider prov)
        throws Exception {
      Object value = get(bean);
      if (value instanceof Boolean) {
        Boolean b = (Boolean)value;
        if (!b.booleanValue()) {
          // filter if "failed" is false
          return;
        }
      }
      super.serializeAsField(bean, jgen, prov);
    }

    @Override
    public BeanPropertyWriter withSerializer(JsonSerializer<Object> ser) {
      return new IsFailedWriter(this, ser);
    }
  }
}
