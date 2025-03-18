/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.segment.local.segment.index.loader;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.FieldIndexConfigsUtil;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;


public class TimeSeriesIndexHandler extends BaseIndexHandler {
  private final Set<String> _columnsToAddIdx;
  public TimeSeriesIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> fieldIndexConfigs,
      @Nullable TableConfig tableConfig) {
    super(segmentDirectory, fieldIndexConfigs, tableConfig);
    _columnsToAddIdx = FieldIndexConfigsUtil.columnsWithIndexEnabled(StandardIndexes.text(), _fieldIndexConfigs);
    Preconditions.checkArgument(_columnsToAddIdx.size() <= 1,
        "Only one column should be configured with time series index");
  }

  @Override
  public void updateIndices(SegmentDirectory.Writer segmentWriter)
      throws Exception {
    // no-op
  }

  @Override
  public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader)
      throws Exception {
    return false;
  }
}
