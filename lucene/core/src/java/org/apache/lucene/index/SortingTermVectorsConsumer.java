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

import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;

final class SortingTermVectorsConsumer extends TermVectorsConsumer {
  TrackingTmpOutputDirectoryWrapper tmpDirectory;

  public SortingTermVectorsConsumer(DocumentsWriterPerThread docWriter) {
    super(docWriter);
  }

  @Override
  void flush(Map<String, TermsHashPerField> fieldsToFlush, final SegmentWriteState state, Sorter.DocMap sortMap) throws IOException {
    super.flush(fieldsToFlush, state, sortMap);
    if (tmpDirectory != null) {
      if (sortMap == null) {
        // we're lucky the index is already sorted, just rename the temporary file and return
        for (Map.Entry<String, String> entry : tmpDirectory.getTemporaryFiles().entrySet()) {
          tmpDirectory.rename(entry.getValue(), entry.getKey());
        }
        return;
      }
      TermVectorsReader reader = docWriter.codec.termVectorsFormat()
          .vectorsReader(tmpDirectory, state.segmentInfo, state.fieldInfos, IOContext.DEFAULT);
      TermVectorsReader mergeReader = reader.getMergeInstance();
      TermVectorsWriter writer = docWriter.codec.termVectorsFormat()
          .vectorsWriter(state.directory, state.segmentInfo, IOContext.DEFAULT);
      try {
        writer.sort(state.segmentInfo.maxDoc(), mergeReader, state.fieldInfos, sortMap::newToOld);
      } finally {
        IOUtils.close(reader, writer);
        IOUtils.deleteFiles(tmpDirectory,
            tmpDirectory.getTemporaryFiles().values());
      }
    }
  }

  @Override
  void initTermVectorsWriter() throws IOException {
    if (writer == null) {
      IOContext context = new IOContext(new FlushInfo(docWriter.getNumDocsInRAM(), docWriter.bytesUsed()));
      tmpDirectory = new TrackingTmpOutputDirectoryWrapper(docWriter.directory);
      writer = docWriter.codec.termVectorsFormat().vectorsWriter(tmpDirectory, docWriter.getSegmentInfo(), context);
      lastDocID = 0;
    }
  }

  @Override
  public void abort() {
    try {
      super.abort();
    } finally {
      IOUtils.deleteFilesIgnoringExceptions(tmpDirectory,
          tmpDirectory.getTemporaryFiles().values());
    }
  }
}
