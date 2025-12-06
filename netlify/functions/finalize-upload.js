// finalize-upload.js - Finalizes pending S3 uploads to Bynder

exports.handler = async function (event, context) {
  console.log("Finalize upload request received");

  // Parse the request body
  let uploads;
  try {
    const body = JSON.parse(event.body);
    uploads = body.uploads;

    if (!uploads || !Array.isArray(uploads) || uploads.length === 0) {
      return {
        statusCode: 400,
        body: JSON.stringify({
          error: "Missing or invalid 'uploads' array in request body"
        }),
      };
    }
  } catch (err) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: "Invalid JSON in request body" }),
    };
  }

  console.log(`Processing ${uploads.length} pending upload(s)...`);

  const results = [];

  // Process each upload
  for (const upload of uploads) {
    const {
      uploadId,
      targetid,
      s3_filename,
      filename,
      presetName,
      brandId,
      totalChunks,
    } = upload;

    console.log(`\nProcessing upload for preset "${presetName}"...`);
    console.log(`Upload ID: ${uploadId}`);

    try {
      // Step 1: Finalize the upload with retry logic
      const importId = await finalizeUploadWithRetry(
        uploadId,
        targetid,
        s3_filename,
        totalChunks || 1
      );

      if (!importId) {
        results.push({
          presetName,
          filename,
          success: false,
          error: "Failed to finalize upload after retries",
        });
        continue;
      }

      console.log(`Got importId: ${importId} for "${presetName}"`);

      // Step 2: Save as new asset
      const saved = await saveAsNewAsset(importId, brandId, filename);

      results.push({
        presetName,
        filename,
        success: saved,
        importId: importId,
        error: saved ? null : "Failed to save as new asset",
      });
    } catch (err) {
      console.error(`Error processing upload for "${presetName}":`, err);
      results.push({
        presetName,
        filename,
        success: false,
        error: err.message,
      });
    }
  }

  // Return results
  const successCount = results.filter((r) => r.success).length;
  console.log(
    `\nFinished processing: ${successCount}/${uploads.length} successful`
  );

  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      message: `Processed ${uploads.length} upload(s)`,
      successCount: successCount,
      failureCount: uploads.length - successCount,
      results: results,
    }),
  };
};

// Helper: Finalize upload with retry logic
async function finalizeUploadWithRetry(
  uploadId,
  targetid,
  s3_filename,
  totalChunks,
  retries = 10,
  delayMs = 3000
) {
  const finalizeParams = new URLSearchParams({
    id: uploadId,
    targetid: targetid,
    s3_filename: `${s3_filename}/p1`,
    chunks: String(totalChunks),
  });

  for (let i = 0; i < retries; i++) {
    console.log(`Finalize attempt ${i + 1}/${retries}...`);

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

    // Check if successful and has importId
    if (finalizeRes.ok && finalizeJson?.importId) {
      console.log("Finalize successful! Import ID:", finalizeJson.importId);
      return finalizeJson.importId;
    }

    // Check if we should retry
    if (
      finalizeJson?.retry ||
      finalizeJson?.message === "Upload not ready"
    ) {
      console.log(
        `Upload not ready yet. Waiting ${delayMs} ms before retry...`
      );
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      continue;
    }

    // Other error - don't retry
    console.error(
      `Failed to finalise upload. Status: ${finalizeRes.status}`,
      finalizeJson
    );
    return null;
  }

  console.error("Failed to finalize upload after retries");
  return null;
}

// Helper: Save as new asset in Bynder
async function saveAsNewAsset(importId, brandId, filename) {
  console.log(`Saving as new asset with importId: ${importId}...`);

  const saveParams = new URLSearchParams({
    brandId: brandId || "",
    name: filename,
  });

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
    console.error(
      `Failed to save asset. Status: ${saveRes.status} ${saveRes.statusText}`
    );
    const saveText = await saveRes.text();
    console.error("Save asset response body:", saveText);
    return false;
  }

  const saveJson = await saveRes.json().catch(() => null);
  console.log(
    `Successfully saved asset "${filename}". Save response:`,
    saveJson
  );

  return true;
}
