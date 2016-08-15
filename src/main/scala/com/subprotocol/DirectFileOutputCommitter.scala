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

package com.subprotocol

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.lib.output.{FileOutputCommitter, FileOutputFormat}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}


// Avro requires a FileOutputCommitter, so in order to use the DirectOutputCommitter
// we have to extend and override functionality in FileOutputCommitter.
class DirectFileOutputCommitter(
  outputPath: Path,
  context: JobContext
) extends FileOutputCommitter(outputPath, context) {

  override def isRecoverySupported: Boolean = true

  override def getCommittedTaskPath(context: TaskAttemptContext): Path = getWorkPath

  override def getCommittedTaskPath(appAttemptId: Int, context: TaskAttemptContext): Path = getWorkPath

  override def getJobAttemptPath(context: JobContext): Path = getWorkPath

  override def getJobAttemptPath(appAttemptId: Int): Path = getWorkPath

  override def needsTaskCommit(taskAttemptContext: TaskAttemptContext): Boolean = true

  override def getWorkPath: Path = outputPath

  override def commitTask(context: TaskAttemptContext, taskAttemptPath: Path): Unit = {
    if(outputPath != null) {
      context.progress()
    }
  }

  override def commitJob(jobContext: JobContext): Unit = {

    val conf = jobContext.getConfiguration
    val shouldCreateSuccessFile = jobContext
      .getConfiguration
      .getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true)

    if (shouldCreateSuccessFile) {
      val outputPath = FileOutputFormat.getOutputPath(jobContext)
      if (outputPath != null) {
        val fileSys = outputPath.getFileSystem(conf)
        val filePath = new Path(outputPath, FileOutputCommitter.SUCCEEDED_FILE_NAME)
        fileSys.create(filePath).close()
      }
    }
  }
}