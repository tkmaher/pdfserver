#!/usr/bin/env node
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { S3Client, PutObjectCommand, DeleteObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import chokidar from "chokidar";
import dotenv from "dotenv";
import mime from "mime-types";
import pLimit from "p-limit";

dotenv.config();

// Config from env
const R2_ENDPOINT = process.env.R2_ENDPOINT;
const BUCKET = process.env.R2_BUCKET;
const ACCESS_KEY = process.env.R2_ACCESS_KEY_ID;
const SECRET_KEY = process.env.R2_SECRET_ACCESS_KEY;
const LOCAL_DIR = process.env.LOCAL_DIR || "./pdf";
const CONCURRENCY = parseInt(process.env.CONCURRENCY || "4", 10);
const RETRY_LIMIT = parseInt(process.env.RETRY_LIMIT || "3", 10);
const DEBOUNCE_MS = parseInt(process.env.DEBOUNCE_MS || "500", 10);

if (!R2_ENDPOINT || !BUCKET || !ACCESS_KEY || !SECRET_KEY) {
  console.error("Missing R2 config. Set R2_ENDPOINT, R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY in .env");
  process.exit(1);
}

const client = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: ACCESS_KEY,
    secretAccessKey: SECRET_KEY,
  },
  forcePathStyle: false, // Cloudflare R2 is compatible with virtual-host style
});

const limit = pLimit(CONCURRENCY);

// Map for debouncing changes: key -> timeoutId
const debounceMap = new Map();

function localPathToKey(localPath) {
  // Convert local path to key relative to LOCAL_DIR, and normalize slashes
  const rel = path.relative(path.resolve(LOCAL_DIR), path.resolve(localPath));
  return rel.split(path.sep).join("/"); // S3 key uses forward slashes
}

async function uploadFile(localPath, attempt = 1) {
  const key = localPathToKey(localPath);
  try {
    const data = await fs.promises.readFile(localPath);
    const contentType = mime.lookup(localPath) || "application/octet-stream";

    const cmd = new PutObjectCommand({
      Bucket: BUCKET,
      Key: key,
      Body: data,
      ContentType: contentType,
      ACL: "public-read"
    });

    await client.send(cmd);
    console.log(`[UPLOADED] ${key}`);
  } catch (err) {
    console.error(`[ERROR] upload ${key} attempt ${attempt}:`, err.message || err);
    if (attempt < RETRY_LIMIT) {
      const backoff = 200 * Math.pow(2, attempt); // exponential backoff
      console.log(`Retrying ${key} in ${backoff}ms`);
      await new Promise((res) => setTimeout(res, backoff));
      return uploadFile(localPath, attempt + 1);
    } else {
      console.error(`[FAILED] ${key} after ${RETRY_LIMIT} attempts`);
    }
  }
}

async function deleteKey(localPath, attempt = 1) {
  const key = localPathToKey(localPath);
  try {
    const cmd = new DeleteObjectCommand({
      Bucket: BUCKET,
      Key: key,
    });
    await client.send(cmd);
    console.log(`[DELETED] ${key}`);
  } catch (err) {
    console.error(`[ERROR] delete ${key} attempt ${attempt}:`, err.message || err);
    if (attempt < RETRY_LIMIT) {
      const backoff = 200 * Math.pow(2, attempt);
      await new Promise((res) => setTimeout(res, backoff));
      return deleteKey(localPath, attempt + 1);
    } else {
      console.error(`[FAILED] delete ${key} after ${RETRY_LIMIT} attempts`);
    }
  }
}

async function ensureLocalDirExists() {
  try {
    await fs.promises.access(LOCAL_DIR, fs.constants.R_OK);
  } catch (err) {
    console.error(`Local dir ${LOCAL_DIR} does not exist or is not readable.`);
    process.exit(1);
  }
}

async function initialSync() {
  console.log("Starting initial sync...");
  const files = [];
  async function walk(dir) {
    const entries = await fs.promises.readdir(dir, { withFileTypes: true });
    for (const ent of entries) {
      const full = path.join(dir, ent.name);
      if (ent.isDirectory()) {
        await walk(full);
      } else if (ent.isFile()) {
        // Only PDFs? we can restrict by ext if desired
        const ext = path.extname(ent.name).toLowerCase();
        if (ext === ".pdf") files.push(full);
      }
    }
  }
  await walk(LOCAL_DIR);
  console.log(`Found ${files.length} PDF(s) to sync.`);

  // upload with concurrency limit
  await Promise.all(files.map((f) => limit(() => uploadFile(f))));
  console.log("Initial sync done.");
}

/** Setup chokidar watcher */
function startWatcher() {
  console.log(`Watching ${LOCAL_DIR} for changes (debounce ${DEBOUNCE_MS}ms)...`);
  const watcher = chokidar.watch(LOCAL_DIR, {
    ignored: (p) => p.includes("node_modules") || p.includes(".git"),
    ignoreInitial: true,
    persistent: true,
    depth: undefined,
    awaitWriteFinish: {
      stabilityThreshold: 200,
      pollInterval: 100,
    },
  });

  function scheduleOp(type, filePath) {
    // debounce per key to avoid double uploads on editors
    const key = localPathToKey(filePath);
    const existing = debounceMap.get(key);
    if (existing) clearTimeout(existing);

    const id = setTimeout(() => {
      debounceMap.delete(key);
      if (type === "add" || type === "change") {
        limit(() => uploadFile(filePath));
      } else if (type === "unlink") {
        limit(() => deleteKey(filePath));
      }
    }, DEBOUNCE_MS);
    debounceMap.set(key, id);
  }

  watcher
    .on("add", (p) => {
      console.log("[WATCH] add", p);
      scheduleOp("add", p);
    })
    .on("change", (p) => {
      console.log("[WATCH] change", p);
      scheduleOp("change", p);
    })
    .on("unlink", (p) => {
      console.log("[WATCH] unlink", p);
      scheduleOp("unlink", p);
    })
    .on("error", (err) => {
      console.error("[WATCHER ERROR]", err);
    });

  // optional: listen for SIGINT to close watcher gracefully
  process.on("SIGINT", async () => {
    console.log("Shutting down watcher...");
    await watcher.close();
    process.exit(0);
  });
}

async function main() {
  await ensureLocalDirExists();
  await initialSync();
  startWatcher();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
