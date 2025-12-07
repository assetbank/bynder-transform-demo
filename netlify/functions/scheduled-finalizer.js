// scheduled-finalizer.js - Runs every 5 minutes to finalize pending uploads
// Auto-processes uploads stored in Upstash Redis

exports.handler = async function (event, context) {
  console.log("Scheduled finalizer running at:", new Date().toISOString());

  // Get pending uploads from storage
  const pendingUploads = await getPendingUploads();

  if (!pendingUploads || pendingUploads.length === 0) {
    console.log("No pending uploads to process");
    return {
      statusCode: 200,
      body: JSON.stringify({ message: "No pending uploads" }),
    };
  }

  console.log(`Processing ${pendingUploads.length} pending upload(s)...`);

  const results = [];
  const completedIds = [];

  // Filter uploads: only process those older than 3 minutes
  const minAge = 3 * 60 * 1000; // 3 minutes in milliseconds
  const now = Date.now();
  const readyUploads = pendingUploads.filter(upload => {
    const age = now - new Date(upload.createdAt).getTime();
    return age >= minAge;
  });

  if (readyUploads.length < pendingUploads.length) {
    console.log(`Skipping ${pendingUploads.length - readyUploads.length} upload(s) - too new (need 3+ min)`);
  }

  if (readyUploads.length === 0) {
    console.log("No uploads ready for finalization yet");
    return {
      statusCode: 200,
      body: JSON.stringify({ message: "No uploads ready yet - all too new" }),
    };
  }

  // Process each ready upload
  for (const upload of readyUploads) {
    const { id, uploadId, targetid, s3_filename, filename, presetName, brandId, totalChunks } = upload;

    console.log(`\nProcessing upload: ${presetName} - ${filename}`);

    try {
      // Step 1: Finalize the upload
      const importId = await finalizeUploadWithRetry(
        uploadId,
        targetid,
        s3_filename,
        totalChunks || 1
      );

      if (!importId) {
        console.log(`Failed to finalize ${presetName}, will retry next run`);
        results.push({ presetName, filename, success: false, error: "Finalize failed" });
        continue;
      }

      // Step 2: Save as new asset
      const saved = await saveAsNewAsset(importId, brandId, filename);

      if (saved) {
        console.log(`✓ Successfully completed ${presetName}`);
        completedIds.push(id);
        results.push({ presetName, filename, success: true, importId });
      } else {
        console.log(`Failed to save ${presetName}, will retry next run`);
        results.push({ presetName, filename, success: false, error: "Save failed" });
      }
    } catch (err) {
      console.error(`Error processing ${presetName}:`, err.message);
      results.push({ presetName, filename, success: false, error: err.message });
    }
  }

  // Remove completed uploads from storage
  if (completedIds.length > 0) {
    await removeCompletedUploads(completedIds);
    console.log(`\nRemoved ${completedIds.length} completed upload(s) from queue`);
  }

  const successCount = results.filter(r => r.success).length;
  console.log(`\n✓ Completed: ${successCount}/${readyUploads.length}`);
  console.log(`Total in queue: ${pendingUploads.length}, Ready: ${readyUploads.length}, Skipped: ${pendingUploads.length - readyUploads.length}`);

  return {
    statusCode: 200,
    body: JSON.stringify({
      message: "Scheduled finalization complete",
      totalInQueue: pendingUploads.length,
      processed: readyUploads.length,
      completed: successCount,
      skipped: pendingUploads.length - readyUploads.length,
      results: results,
    }),
  };
};

// Get pending uploads from Upstash Redis
async function getPendingUploads() {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_TOKEN;

  if (!UPSTASH_URL || !UPSTASH_TOKEN) {
    console.error("Missing Upstash credentials in environment variables");
    return [];
  }

  try {
    const response = await fetch(`${UPSTASH_URL}/lrange/pending-uploads/0/-1`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });

    const data = await response.json();
    if (!data.result || data.result.length === 0) {
      return [];
    }

    // Each item is a double-encoded JSON string: "[\"...\"]"
    // Need to parse twice: first to get array, then to get object
    return data.result.map(item => {
      const parsed = JSON.parse(item); // First parse: "[\"...\"]" -> ["{...}"]
      const jsonString = Array.isArray(parsed) ? parsed[0] : parsed; // Unwrap: ["{...}"] -> "{...}"
      return JSON.parse(jsonString); // Second parse: "{...}" -> {...}
    });
  } catch (err) {
    console.error("Error fetching pending uploads:", err);
    return [];
  }
}

// Remove completed uploads from storage
async function removeCompletedUploads(ids) {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_TOKEN;

  try {
    // Get all uploads
    const allUploads = await getPendingUploads();

    // Filter out completed ones
    const remaining = allUploads.filter(u => !ids.includes(u.id));

    // Clear the list
    await fetch(`${UPSTASH_URL}/del/pending-uploads`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });

    // Re-add remaining uploads
    if (remaining.length > 0) {
      for (const upload of remaining) {
        await fetch(`${UPSTASH_URL}/rpush/pending-uploads`, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${UPSTASH_TOKEN}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify([JSON.stringify(upload)])
        });
      }
    }
  } catch (err) {
    console.error("Error removing completed uploads:", err);
  }
}

// Helper: Finalize upload with retry logic
async function finalizeUploadWithRetry(uploadId, targetid, s3_filename, totalChunks, retries = 3) {
  const finalizeParams = new URLSearchParams({
    id: uploadId,
    targetid: targetid,
    s3_filename: `${s3_filename}/p1`,
    chunks: String(totalChunks),
  });

  for (let i = 0; i < retries; i++) {
    console.log(`  Finalize attempt ${i + 1}/${retries}...`);

    try {
      const finalizeRes = await fetch(
        `https://jakob-spott.bynder.com/api/v4/upload/`,
        {
          method: "POST",
          headers: {
            Authorization: process.env.BYNDER_TOKEN,
            "Content-Type": "application/x-www-form-urlencoded",
          },
          body: finalizeParams.toString(),
        }
      );

      const finalizeJson = await finalizeRes.json().catch(() => null);

      if (finalizeRes.ok && finalizeJson?.importId) {
        console.log(`  ✓ Got importId: ${finalizeJson.importId}`);
        return finalizeJson.importId;
      }

      if (finalizeJson?.retry || finalizeJson?.message === "Upload not ready") {
        console.log(`  Upload not ready, waiting 2s...`);
        await new Promise(resolve => setTimeout(resolve, 2000));
        continue;
      }

      console.log(`  Finalize failed:`, finalizeJson);
      return null;
    } catch (err) {
      console.error(`  Finalize error:`, err.message);
      if (i < retries - 1) {
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
  }

  return null;
}

// Helper: Save as new asset
async function saveAsNewAsset(importId, brandId, filename) {
  console.log(`  Saving as new asset...`);

  const saveParams = new URLSearchParams({
    brandId: brandId || "",
    name: filename,
  });

  try {
    const saveRes = await fetch(
      `https://jakob-spott.bynder.com/api/v4/media/save/${importId}`,
      {
        method: "POST",
        headers: {
          Authorization: process.env.BYNDER_TOKEN,
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: saveParams.toString(),
      }
    );

    if (!saveRes.ok) {
      const saveText = await saveRes.text();
      console.error(`  Save failed: ${saveRes.status}`, saveText);
      return false;
    }

    console.log(`  ✓ Asset saved successfully`);
    return true;
  } catch (err) {
    console.error(`  Save error:`, err.message);
    return false;
  }
}
