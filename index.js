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
var AWS = require("aws-sdk");
var LineStream = require("byline").LineStream;
var AlbLogParser = require("alb-log-parser"); // alb-log-parser  https://github.com/igtm/node-alb-log-parser
var VpcFlowLogParser = require("vpc-flow-log-parser"); // vpc-flow-log-parser  https://github.com/toshihirock/node-vpc-flow-log-parser
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
  endpoint = new AWS.Endpoint(esEndpoint);
}

const region = process.env["region"];
if (!region) {
  console.log("Error. Environment variable region is not defined");
}

const indexPrefix = process.env["INDEX_PREFIX"] + "-";
const index = indexPrefix + "-" + indexTimestamp; // adds a timestamp to index. Example: elblogs-2016.03.31
const doctype = process.env["doctype"] || "elb-access-logs";

/* Globals */
var s3 = new AWS.S3();
var totLogLines = 0; // Total number of log lines in the file
var numDocsAdded = 0; // Number of log lines added to ES so far

/*
 * The AWS credentials are picked up from the environment.
 * They belong to the IAM role assigned to the Lambda function.
 * Since the ES requests are signed using these credentials,
 * make sure to apply a policy that permits ES domain operations
 * to the role.
 */
var creds = new AWS.EnvironmentCredentials("AWS");

console.log("Initializing AWS Lambda Function");

/*
 * Get the log file from the given S3 bucket and key.  Parse it and add
 * each log record to the ES domain.
 */
function s3LogsToES(bucket, key, context, lineStream, recordStream) {
  // Note: The Lambda function should be configured to filter for .log files
  // (as part of the Event Source "suffix" setting).
  if (!esEndpoint) {
    var error = new Error("ERROR: Environment variable es_endpoint not set");
    context.fail(error);
  }

  var s3Stream = s3.getObject({ Bucket: bucket, Key: key }).createReadStream();

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
function postDocumentToES(doc, context) {
  var req = new AWS.HttpRequest(endpoint);

  req.method = "POST";
  req.path = path.join("/", index, doctype);
  req.region = region;
  req.body = doc;
  req.headers["presigned-expires"] = false;
  req.headers["Host"] = endpoint.host;
  // needed to make it work with ES 6.x
  // https://www.elastic.co/blog/strict-content-type-checking-for-elasticsearch-rest-requests
  req.headers["Content-Type"] = "application/json";

  // Sign the request (Sigv4)
  var signer = new AWS.Signers.V4(req, "es");
  signer.addAuthorization(creds, new Date());

  // Post document to ES
  var send = new AWS.NodeHttpClient();
  send.handleRequest(
    req,
    null,
    function (httpResp) {
      var body = "";
      httpResp.on("data", function (chunk) {
        body += chunk;
      });
      httpResp.on("end", function (chunk) {
        numDocsAdded++;
        if (numDocsAdded === totLogLines) {
          // Mark lambda success.  If not done so, it will be retried.
          console.log(
            "All " +
              numDocsAdded +
              " log records added to index " +
              index +
              " in region " +
              region +
              ".",
          );
          context.succeed();
        }
      });
    },
    function (err) {
      console.log("Error: " + err);
      console.log(
        numDocsAdded + "of " + totLogLines + " log records added to ES.",
      );
      context.fail();
    },
  );
}

/* Lambda "main": Execution starts here */
exports.handler = function (event, context) {
  console.log("Received event: ", JSON.stringify(event, null, 2));

  // Get the type of log file depending on the path of the object
  var objPath = event.Records[0].s3.object.key;
  if (objPath.includes("elasticloadbalancing")) {
    console.log("Processing ALB log file");
    var parse = AlbLogParser;
  } else if (objPath.includes("vpcflowlogs/")) {
    console.log("Processing VPC Flow log file");
    var parse = VpcFlowLogParser;
  } else {
    console.log("Unknown log file type");
    context.fail();
  }

  /* == Streams ==
   * To avoid loading an entire (typically large) log file into memory,
   * this is implemented as a pipeline of filters, streaming log data
   * from S3 to ES.
   * Flow: S3 file stream -> Log Line stream -> Log Record stream -> ES
   */
  var lineStream = new LineStream();
  // A stream of log records, from parsing each log line
  var recordStream = new stream.Transform({ objectMode: true });
  recordStream._transform = function (line, encoding, done) {
    var logRecord = parse(line.toString());
    var serializedRecord = JSON.stringify(logRecord);
    this.push(serializedRecord);
    totLogLines++;
    done();
  };

  event.Records.forEach(function (record) {
    var bucket = record.s3.bucket.name;
    var objKey = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));
    s3LogsToES(bucket, objKey, context, lineStream, recordStream);
  });
};
