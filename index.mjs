import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, UpdateCommand } from "@aws-sdk/lib-dynamodb";
import {
  SNSClient,
  CreatePlatformEndpointCommand,
  GetEndpointAttributesCommand,
  SetEndpointAttributesCommand,
} from "@aws-sdk/client-sns";
import { unmarshall } from "@aws-sdk/util-dynamodb";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const sns = new SNSClient({});

const TABLE_NAME = process.env.TABLE_NAME;
const ANDROID_PLATFORM_APPLICATION_ARN =
  process.env.ANDROID_PLATFORM_APPLICATION_ARN;
const IOS_PLATFORM_APPLICATION_ARN =
  process.env.IOS_PLATFORM_APPLICATION_ARN || "";

function nowIso() {
  return new Date().toISOString();
}

function getPlatformApplicationArn(platform) {
  if (platform === "android") return ANDROID_PLATFORM_APPLICATION_ARN;
  if (platform === "ios") return IOS_PLATFORM_APPLICATION_ARN;
  return null;
}

function isRealSnsEndpointArn(value) {
  return typeof value === "string" && value.startsWith("arn:aws:sns:");
}

async function createEndpoint({ platformApplicationArn, pushToken, userId, deviceId }) {
  const response = await sns.send(
    new CreatePlatformEndpointCommand({
      PlatformApplicationArn: platformApplicationArn,
      Token: pushToken,
      CustomUserData: userId || deviceId,
    })
  );

  return response.EndpointArn;
}

async function ensureEndpointUpToDate({ endpointArn, pushToken }) {
  const response = await sns.send(
    new GetEndpointAttributesCommand({
      EndpointArn: endpointArn,
    })
  );

  const attrs = response.Attributes || {};
  const tokenMismatch = attrs.Token !== pushToken;
  const disabled = attrs.Enabled !== "true";

  if (tokenMismatch || disabled) {
    await sns.send(
      new SetEndpointAttributesCommand({
        EndpointArn: endpointArn,
        Attributes: {
          Token: pushToken,
          Enabled: "true",
        },
      })
    );
  }

  return endpointArn;
}

async function writeBackEndpoint({
  deviceId,
  endpointArn,
}) {
  const now = nowIso();

  await ddb.send(
    new UpdateCommand({
      TableName: TABLE_NAME,
      Key: { deviceId },
      UpdateExpression: `
        SET snsEndpointArn = :arn,
            endpointEnabled = :enabled,
            updatedAt = :updatedAt,
            lastSeenAt = :lastSeenAt
      `,
      ExpressionAttributeValues: {
        ":arn": endpointArn,
        ":enabled": true,
        ":updatedAt": now,
        ":lastSeenAt": now,
      },
    })
  );
}

async function processRecord(record) {
  if (record.eventName !== "INSERT" && record.eventName !== "MODIFY") {
    return;
  }

  const newImage = record.dynamodb?.NewImage
    ? unmarshall(record.dynamodb.NewImage)
    : null;
  const oldImage = record.dynamodb?.OldImage
    ? unmarshall(record.dynamodb.OldImage)
    : null;

  if (!newImage) return;

  const {
    deviceId,
    userId,
    platform,
    pushToken,
    snsEndpointArn,
    endpointEnabled,
  } = newImage;

  if (!deviceId || !platform || !pushToken) {
    console.log("Skipping record: missing required fields", {
      deviceId,
      platform,
      hasPushToken: Boolean(pushToken),
    });
    return;
  }

  const platformApplicationArn = getPlatformApplicationArn(platform);
  if (!platformApplicationArn) {
    console.log("Skipping record: unsupported platform or missing platform application ARN", {
      deviceId,
      platform,
    });
    return;
  }

  const oldToken = oldImage?.pushToken;
  const oldArn = oldImage?.snsEndpointArn;
  const tokenChanged = oldToken !== pushToken;
  const hasRealArn = isRealSnsEndpointArn(snsEndpointArn);
  const oldHadRealArn = isRealSnsEndpointArn(oldArn);
  const endpointNeedsRepair = endpointEnabled === false;

  // Skip noisy updates:
  // - existing real ARN
  // - token did not change
  // - endpoint not explicitly disabled
  if (record.eventName === "MODIFY" && hasRealArn && !tokenChanged && !endpointNeedsRepair) {
    console.log("Skipping unchanged endpoint record", { deviceId });
    return;
  }

  let finalEndpointArn = snsEndpointArn;

  if (!hasRealArn) {
    // First-time registration or still PENDING
    finalEndpointArn = await createEndpoint({
      platformApplicationArn,
      pushToken,
      userId,
      deviceId,
    });
    console.log("Created platform endpoint", { deviceId, endpointArn: finalEndpointArn });
  } else {
    try {
      finalEndpointArn = await ensureEndpointUpToDate({
        endpointArn: snsEndpointArn,
        pushToken,
      });
      console.log("Verified/repaired existing endpoint", {
        deviceId,
        endpointArn: finalEndpointArn,
      });
    } catch (err) {
      // If saved ARN is stale or invalid, fall back to creating a new endpoint
      console.warn("Existing endpoint check failed; creating a new endpoint", {
        deviceId,
        previousArn: snsEndpointArn,
        errorName: err?.name,
        errorMessage: err?.message,
      });

      finalEndpointArn = await createEndpoint({
        platformApplicationArn,
        pushToken,
        userId,
        deviceId,
      });

      console.log("Created replacement endpoint", {
        deviceId,
        endpointArn: finalEndpointArn,
      });
    }
  }

  if (!finalEndpointArn) {
    throw new Error(`Failed to resolve SNS endpoint ARN for deviceId=${deviceId}`);
  }

  await writeBackEndpoint({
    deviceId,
    endpointArn: finalEndpointArn,
  });
}

export const handler = async (event) => {
  console.log("Received stream batch", {
    recordCount: event?.Records?.length || 0,
  });

  const failures = [];

  for (const record of event.Records || []) {
    try {
      await processRecord(record);
    } catch (err) {
      console.error("Failed processing record", {
        eventID: record.eventID,
        errorName: err?.name,
        errorMessage: err?.message,
        stack: err?.stack,
      });

      // Partial batch response for DynamoDB Streams:
      // only failed records are retried
      failures.push({
        itemIdentifier: record.eventID,
      });
    }
  }

  return {
    batchItemFailures: failures,
  };
};