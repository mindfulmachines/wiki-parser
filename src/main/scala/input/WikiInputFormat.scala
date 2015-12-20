package input

/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import java.io.IOException
import org.apache.hadoop.classification.InterfaceAudience
import org.apache.hadoop.classification.InterfaceStability
import org.apache.hadoop.fs._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred._

/**
  * An {@link InputFormat} for wikipedia xml dump files.  Files are broken into articles
  * as determined by the <page>...</page> tags on the xml dump.  Keys are
  * the position in the raw file, and values are the article text including the page tags.
  */
@InterfaceAudience.Public
@InterfaceStability.Stable class WikiInputFormat extends FileInputFormat[LongWritable, Text] with JobConfigurable {
  def configure(conf: JobConf) {
  }

  protected override def isSplitable(fs: FileSystem, file: Path): Boolean = {
    false
  }

  @throws(classOf[IOException])
  def getRecordReader(genericSplit: InputSplit, job: JobConf, reporter: Reporter): RecordReader[LongWritable, Text] = {
     new WikiRecordReader(job, genericSplit.asInstanceOf[FileSplit])
  }
}
