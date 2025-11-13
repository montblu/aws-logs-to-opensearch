/*
 * This project based on https://github.com/awslabs/amazon-elasticsearch-lambda-samples
 * Sample code for AWS Lambda to get AWS ELB log files from S3, parse
 * and add them to an Amazon Elasticsearch Service domain.
 *
 *
 * Copyright 2015- Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at http://aws.amazon.com/asl/
 * or in the "license" file accompanying this file.  This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

/* Imports */
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { Client } = require('@opensearch-project/opensearch');
var LineStream = require("byline").LineStream;
var AlbLogParser = require("./lib/alb-log-parser"); // local copy of alb-log-parser
var VpcFlowLogParser = require("./lib/vpc-flow-log-parser"); // local copy of vpc-flow-log-parser
var path = require("path");
var stream = require("stream");
const zlib = require("zlib");

var indexTimestamp = new Date()
  .toISOString()
  .replace(/\-/g, ".")
  .replace(/T.+/, "");
const esEndpoint = process.env["es_endpoint"];
var endpoint = "";
if (!esEndpoint) {
  console.log("ERROR: Environment variable es_endpoint not set");
} else {
  endpoint = esEndpoint;
}

const region = process.env["region"];
if (!region) {
  console.log("Error. Environment variable region is not defined");
}

var index;
const indexPrefix = process.env["INDEX_PREFIX"];
if (!indexPrefix) {
  console.log("Error. Environment variable INDEX_PREFIX is not defined");
}

var logType;
function getLogType(objPath) {
  if (objPath.includes("elasticloadbalancing")) {
    console.log("Processing ALB log file");
    return "alb";
  } else if (objPath.includes("vpcflowlogs")) {
    console.log("Processing VPC Flow log file");
    return "vpc";
  } else {
    throw new Error("Unknown log file type for object path: " + objPath);
  }
}

var parser;
function getParser(logType) {
  if (logType === "alb") {
    return AlbLogParser;
  } else if (logType === "vpc") {
    return VpcFlowLogParser;
  } else {
    throw new Error("Unknown log type: " + logType);
  }
}

/*
We want to send only VPC Flow Logs that correspond to a specific port.
Otherwise, we will send all the logs of the VPC
*/
var VPCFlowLogsPorts = process.env["VPC_SEND_ONLY_LOGS_WITH_PORTS"];
var destinationPortFilters;
if (VPCFlowLogsPorts) {
  try {
    // Split the string by comma and map each element to a number
    destinationPortFilters = VPCFlowLogsPorts.split(",").map(Number);
    console.log(
      "Sending only VPC Flow Logs with 'dsport' = '" +
        JSON.stringify(destinationPortFilters) +
        "'",
    );
  } catch (e) {
    console.error("Error parsing ports:", e);
  }
}

/* Globals */
const s3 = new S3Client({ region });
const esClient = new Client({
  node: `https://${endpoint}`,
  // The client will automatically use AWS credentials from the environment or IAM role
});
var totLogLines = 0; // Total number of log lines in the file
var numDocsAdded = 0; // Number of log lines added to ES so far

/*
 * The AWS credentials are picked up from the environment.
 * They belong to the IAM role assigned to the Lambda function.
 * Since the ES requests are signed using these credentials,
 * make sure to apply a policy that permits ES domain operations
 * to the role.
 */

console.log("Initializing AWS Lambda Function");

/*
 * Get the log file from the given S3 bucket and key.  Parse it and add
 * each log record to the ES domain.
 */
async function s3LogsToES(bucket, key, context, lineStream, recordStream) {
  // Note: The Lambda function should be configured to filter for .log files
  // (as part of the Event Source "suffix" setting).
  if (!esEndpoint) {
    var error = new Error("ERROR: Environment variable es_endpoint not set");
    context.fail(error);
  }

  const command = new GetObjectCommand({ Bucket: bucket, Key: key });
  const response = await s3.send(command);
  var s3Stream = response.Body;

  // Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
  s3Stream
    .pipe(zlib.createGunzip())
    .pipe(lineStream)
    .pipe(recordStream)
    .on("data", function (parsedEntry) {
      postDocumentToES(parsedEntry, context);
    });

  s3Stream.on("error", function () {
    console.log(
      'Error getting object "' +
        key +
        '" from bucket "' +
        bucket +
        '".  ' +
        "Make sure they exist and your bucket is in the same region as this function.",
    );
    context.fail();
  });
}

/*
 * Add the given document to the ES domain.
 * If all records are successfully added, indicate success to lambda
 * (using the "context" parameter).
 */
async function postDocumentToES(doc, context) {
  try {
    await esClient.index({
      index: index,
      body: doc
    });
    numDocsAdded++;
    if (numDocsAdded === totLogLines) {
      console.log(
        "All " +
          numDocsAdded +
          " log records added to index " +
          index +
          " in region " +
          region +
          ".",
      );
      context.succeed("All log records processed successfully");
    }
  } catch (err) {
    console.log("Error: " + err.message);
    console.log(numDocsAdded + " of " + totLogLines + " log records added to ES.");
    context.fail(err);
  }
}

/* Lambda "main": Execution starts here */
exports.handler = function (event, context) {
  console.log("Received event: ", JSON.stringify(event, null, 2));

  // Set the log type variable depending on the path of the object
  var objPath = event.Records[0].s3.object.key;
  logType = getLogType(objPath);

  // Set parser
  parser = getParser(logType);

  // Set the index name (use a fresh timestamp per invocation)
  const invocationIndexTimestamp = new Date()
    .toISOString()
    .replace(/\-/g, ".")
    .replace(/T.+/, "");
  index = indexPrefix + "-" + logType + "-" + invocationIndexTimestamp;

  // Reset counters per invocation
  totLogLines = 0;
  numDocsAdded = 0;

  /* == Streams ==
   * To avoid loading an entire (typically large) log file into memory,
   * this is implemented as a pipeline of filters, streaming log data
   * from S3 to ES.
   * Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
   */

  // Process each S3 record with its own streams to avoid interleaving
  event.Records.forEach(function (record) {
    var bucket = record.s3.bucket.name;
    var objKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

    var lineStream = new LineStream();
    // A stream of log records, from parsing each log line
    var recordStream = new stream.Transform({ objectMode: true });
    recordStream._transform = function (line, encoding, done) {
    var logRecord = parser(line.toString());

    // If parser returned null (e.g., header lines), skip
    if (!logRecord) {
      done();
      return;
    }

    // Normalize fields: convert '-' or empty strings to null and coerce numeric strings
    for (let key in logRecord) {
      if (typeof logRecord[key] === 'string') {
        const trimmed = logRecord[key].trim();
        if (trimmed === '-' || trimmed === '') {
          logRecord[key] = null;
          continue;
        }
        const n = Number(trimmed);
        if (!Number.isNaN(n) && trimmed !== '') {
          logRecord[key] = Number.isInteger(n) ? parseInt(trimmed, 10) : n;
        } else {
          logRecord[key] = trimmed;
        }
      }
    }


      // In case of VPC Flow Logs
      if (logType === "vpc") {
        if (logRecord.start_utc !== "Invalid date" && logRecord.start_utc != null) {
          let startDateTime = new Date(logRecord.start_utc);
          logRecord.timestamp = startDateTime.toISOString();
          logRecord.request_creation_time = startDateTime.toISOString();
        }
      }

      let dstPort = parseInt(logRecord.dstport);
      if (
        logType === "alb" ||
        (logType === "vpc" && !VPCFlowLogsPorts) ||
        (logType === "vpc" && Array.isArray(destinationPortFilters) && destinationPortFilters.includes(dstPort))
      ) {
        // Push object (not JSON string) so types are preserved
        this.push(logRecord);
        totLogLines++;
      }
      done();
    };

    s3LogsToES(bucket, objKey, context, lineStream, recordStream);
  });
};
