/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle.protocol;

//Needed by ScalaDoc. See SPARK-7726
import static org.apache.spark.network.shuffle.protocol.BlockTransferMessage.Type;

import org.apache.spark.network.protocol.Encoders;

import com.google.common.base.Objects;

import io.netty.buffer.ByteBuf;

/**
 * Remove the given application and optionally delete corresponding shuffle files.
 */
public class RemoveApplication extends BlockTransferMessage {
  public final String appId;
  public final boolean cleanLocalDirs;

  public RemoveApplication(String appId, boolean cleanLocalDirs) {
    this.appId = appId;
    this.cleanLocalDirs = cleanLocalDirs;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null && obj instanceof RemoveApplication) {
      RemoveApplication o = (RemoveApplication)obj;

      return (appId == o.appId && cleanLocalDirs == o.cleanLocalDirs);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(appId, cleanLocalDirs);
  }

  @Override
  public int encodedLength() {
    return Encoders.Strings.encodedLength(appId)
        + 1; // boolean
  }

  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, appId);
    buf.writeBoolean(cleanLocalDirs);
  }

  @Override
  protected Type type() {
    return Type.REMOVE_APPLICATION;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("appId", appId)
        .add("cleanLocalDirs", cleanLocalDirs)
        .toString();
  }

  public static RemoveApplication decode(ByteBuf buf) {
    return new RemoveApplication(Encoders.Strings.decode(buf), buf.readBoolean());
  }
}
