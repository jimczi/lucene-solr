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

package org.apache.lucene.index;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

final class SortingStoredFieldsConsumer extends StoredFieldsConsumer {
  TrackingTmpOutputDirectoryWrapper tmpDirectory;

  SortingStoredFieldsConsumer(DocumentsWriterPerThread docWriter) {
    super(docWriter);
  }

  @Override
  protected void initStoredFieldsWriter() throws IOException {
    if (writer == null) {
      this.tmpDirectory = new TrackingTmpOutputDirectoryWrapper(docWriter.directory);
      this.writer = docWriter.codec.storedFieldsFormat().fieldsWriter(tmpDirectory, docWriter.getSegmentInfo(),
          IOContext.DEFAULT);
    }
  }

  @Override
  void flush(SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    super.flush(state, sortMap);
    if (sortMap == null) {
      // we're lucky the index is already sorted, just rename the temporary file and return
      for (Map.Entry<String, String> entry : tmpDirectory.getTemporaryFiles().entrySet()) {
        tmpDirectory.rename(entry.getValue(), entry.getKey());
      }
      return;
    }
    StoredFieldsReader reader = docWriter.codec.storedFieldsFormat()
        .fieldsReader(tmpDirectory, state.segmentInfo, state.fieldInfos, IOContext.DEFAULT);
    StoredFieldsReader mergeReader = reader.getMergeInstance();
    StoredFieldsWriter writer = docWriter.codec.storedFieldsFormat()
        .fieldsWriter(state.directory, state.segmentInfo, IOContext.DEFAULT);
    try {
      writer.sort(state.segmentInfo.maxDoc(), mergeReader, state.fieldInfos, sortMap::newToOld);
    } finally {
      IOUtils.close(reader, writer);
      IOUtils.deleteFiles(tmpDirectory,
          tmpDirectory.getTemporaryFiles().values());
    }
  }

  @Override
  void abort() {
    try {
      super.abort();
    } finally {
      IOUtils.deleteFilesIgnoringExceptions(tmpDirectory,
          tmpDirectory.getTemporaryFiles().values());
    }
  }
}
