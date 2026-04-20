import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  GetCommand,
} from "@aws-sdk/lib-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { unmarshall } from "@aws-sdk/util-dynamodb";

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
const sns = new SNSClient({});

const FOLLOWERS_TABLE = process.env.FOLLOWERS_TABLE;
const DEVICES_TABLE = process.env.DEVICES_TABLE;

function normalizeStatus(status) {
  return typeof status === "string" ? status.trim().toLowerCase() : "";
}

function getTransitionStatus(oldStatus, newStatus) {
  const oldNorm = normalizeStatus(oldStatus);
  const newNorm = normalizeStatus(newStatus);

  if (newNorm === "online" && oldNorm !== "online") {
    return "online";
  }

  if (oldNorm === "online" && newNorm === "offline") {
    return "offline";
  }

  return null;
}

function inferPrayerName(value) {
  if (!value || typeof value !== "string") return "Live";
  const cleaned = value.trim();
  if (!cleaned) return "Live";

  const lower = cleaned.toLowerCase();
  const withoutSuffix = lower.endsWith(" azaan")
    ? cleaned.slice(0, -6).trim()
    : cleaned;

  const firstWord = withoutSuffix.split(/\s+/)[0];
  return firstWord || "Live";
}

function slugify(value) {
  return (value || "channel")
    .toString()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 50) || "channel";
}

function getTimestampParts(date) {
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(date.getUTCDate()).padStart(2, "0");
  const hh = String(date.getUTCHours()).padStart(2, "0");
  const min = String(date.getUTCMinutes()).padStart(2, "0");
  const ss = String(date.getUTCSeconds()).padStart(2, "0");

  return {
    ymd: `${yyyy}${mm}${dd}`,
    hms: `${hh}${min}${ss}`,
  };
}

function resolveServerAddress(streamUrl) {
  if (process.env.SERVER_ADDRESS) return process.env.SERVER_ADDRESS;

  if (typeof streamUrl === "string" && streamUrl) {
    try {
      const url = new URL(streamUrl);
      return `${url.protocol}//${url.host}`;
    } catch {
      // Ignore parse errors and use fallback.
    }
  }

  return "https://your-server-domain";
}

function resolveStreamUrl(streamUrl, serverAddress, mountName) {
  if (streamUrl) return streamUrl;
  if (!mountName) return "";

  if (serverAddress.endsWith("/") && mountName.startsWith("/")) {
    return `${serverAddress.slice(0, -1)}${mountName}`;
  }

  if (!serverAddress.endsWith("/") && !mountName.startsWith("/")) {
    return `${serverAddress}/${mountName}`;
  }

  return `${serverAddress}${mountName}`;
}

function isRealEndpointArn(value) {
  return typeof value === "string" && value.startsWith("arn:aws:sns:");
}

function buildSnsPayload({ status, prayerName, channelName, mountName, streamUrl }) {
  const body = status === "online"
    ? `${prayerName} Azaan started`
    : `${prayerName} stream ended`;

  const title = status === "online"
    ? "Mosque is Live"
    : "Mosque went offline";

  const now = new Date();
  const { ymd, hms } = getTimestampParts(now);
  const channelSlug = slugify(channelName);
  const prayerSlug = slugify(prayerName);
  const eventId = `evt_${ymd}_${hms}_${channelSlug}_${prayerSlug}_${status}`;
  const serverAddress = resolveServerAddress(streamUrl);
  const resolvedStreamUrl = resolveStreamUrl(streamUrl, serverAddress, mountName);

  return {
    default: body,
    GCM: JSON.stringify({
      notification: {
        title,
        body,
      },
      data: {
        type: "channel_live",
        status,
        eventId,
        mountName: mountName || "",
        prayerName,
        channelName: channelName || "",
        serverAddress,
        streamUrl: resolvedStreamUrl || "",
      },
    }),
  };
}

async function getFollowersByMountName(mountName) {
  const result = await ddb.send(
    new QueryCommand({
      TableName: FOLLOWERS_TABLE,
      KeyConditionExpression: "mountName = :m",
      ExpressionAttributeValues: {
        ":m": mountName,
      },
    })
  );

  return result.Items || [];
}

async function getDeviceEndpoint(deviceId) {
  const result = await ddb.send(
    new GetCommand({
      TableName: DEVICES_TABLE,
      Key: { deviceId },
    })
  );

  return result.Item || null;
}

async function sendPushToEndpoint(endpointArn, payload) {
  await sns.send(
    new PublishCommand({
      TargetArn: endpointArn,
      MessageStructure: "json",
      Message: JSON.stringify(payload),
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

  const oldStatus = oldImage?.current_status;
  const newStatus = newImage?.current_status;

  const transitionStatus = getTransitionStatus(oldStatus, newStatus);
  if (!transitionStatus) {
    console.log("Skipping non-state-change transition", {
      mountName: newImage?.mountName,
      oldStatus,
      newStatus,
    });
    return;
  }

  const mountName = newImage.mountName;
  const channelName = newImage.minaretName || newImage.channelName || "Mosque";
  const prayerName = newImage.prayerName || inferPrayerName(newImage.liveTitle);
  const streamUrl = newImage.streamUrl || "";

  if (!mountName) {
    console.log("Skipping record with missing mountName");
    return;
  }

  const payload = buildSnsPayload({
    status: transitionStatus,
    prayerName,
    channelName,
    mountName,
    streamUrl,
  });

  const followers = await getFollowersByMountName(mountName);

  console.log("Followers found", {
    mountName,
    followerCount: followers.length,
  });

  let sentCount = 0;
  let skippedCount = 0;
  let failedCount = 0;

  for (const follower of followers) {
    try {
      if (follower.notificationsEnabled === false || follower.isActive === false) {
        skippedCount += 1;
        continue;
      }

      const deviceId = follower.deviceId;
      if (!deviceId) {
        skippedCount += 1;
        continue;
      }

      const device = await getDeviceEndpoint(deviceId);

      if (!device) {
        console.log("Device endpoint record not found", { deviceId });
        skippedCount += 1;
        continue;
      }

      const endpointArn = device.snsEndpointArn;
      const endpointEnabled = device.endpointEnabled !== false;

      if (!isRealEndpointArn(endpointArn) || !endpointEnabled) {
        console.log("Skipping device without valid endpoint", {
          deviceId,
          endpointArn,
          endpointEnabled,
        });
        skippedCount += 1;
        continue;
      }

      await sendPushToEndpoint(endpointArn, payload);

      sentCount += 1;
      console.log("Notification sent", {
        mountName,
        deviceId,
        endpointArn,
      });
    } catch (err) {
      failedCount += 1;
      console.error("Failed sending notification to follower", {
        mountName,
        deviceId: follower?.deviceId,
        errorName: err?.name,
        errorMessage: err?.message,
      });
    }
  }

  console.log("Broadcast notification summary", {
    mountName,
    sentCount,
    skippedCount,
    failedCount,
  });
}

export const handler = async (event) => {
  const failures = [];

  console.log("Received broadcaster stream batch", {
    recordCount: event?.Records?.length || 0,
  });

  for (const record of event.Records || []) {
    try {
      await processRecord(record);
    } catch (err) {
      console.error("Failed processing broadcaster record", {
        eventID: record.eventID,
        errorName: err?.name,
        errorMessage: err?.message,
        stack: err?.stack,
      });

      failures.push({
        itemIdentifier: record.eventID,
      });
    }
  }

  return {
    batchItemFailures: failures,
  };
};
