// bynder-webhook.js

const FormData = require('form-data');

const CHUNK_SIZE = 5 * 1024 * 1024; // 5 MB

exports.handler = async function (event, context) {
  console.log("Incoming request body:", event.body);

  // STEP 1: Parse incoming SNS wrapper
  let parsed;
  try {
    parsed = JSON.parse(event.body);
  } catch (err) {
    console.error("Failed to parse event.body as JSON", err);
    return { statusCode: 400, body: "Invalid JSON" };
  }

  let mediaId = null;
  let originalMedia = null;

  if (parsed.Type === "Notification" && parsed.Message) {
    try {
      const binderMessage = JSON.parse(parsed.Message);
      mediaId = binderMessage.media_id;
      originalMedia = binderMessage.media;
      console.log("Extracted media ID:", mediaId);
    } catch (err) {
      console.error("Failed to parse Binder Message", err);
    }
  }

  // STEP 2: If no media ID, nothing more to do
  if (!mediaId) {
    console.log("No mediaId found in webhook payload.");
    return {
      statusCode: 200,
      body: "No mediaId found in webhook",
    };
  }

  // STEP 2.5: Prevent infinite loop - skip processing transformed assets
  // Transformed assets use naming pattern: originalname__presetname.ext
  if (originalMedia && originalMedia.name && originalMedia.name.includes("__")) {
    console.log(`Skipping processing for transformed asset: ${originalMedia.name}`);
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: "Skipped - asset is a transformed derivative",
        assetName: originalMedia.name
      }),
    };
  }

  // STEP 3: Helper retry function to fetch Binder metadata
  async function fetchAssetInfoWithRetry(mediaId, retries = 6, delayMs = 4000) {
    for (let i = 0; i < retries; i++) {
      console.log(`Fetching metadata attempt ${i + 1}/${retries}...`);

      const response = await fetch(
        `https://jakob-spott.bynder.com/api/v4/media/${mediaId}/`,
        {
          method: "GET",
          headers: {
            Authorization: process.env.BYNDER_TOKEN,
            "Content-Type": "application/json",
          },
        }
      );

      let assetInfo;
      try {
        assetInfo = await response.json();
      } catch (err) {
        console.error("Failed to parse Binder metadata JSON:", err);
        return null;
      }

      // Check if transformBaseUrl is available
      if (
        assetInfo.transformBaseUrl &&
        assetInfo.transformBaseUrl.trim().length > 0
      ) {
        console.log("transformBaseUrl found!");
        return assetInfo;
      }

      console.log(
        `transformBaseUrl empty. Waiting ${delayMs} ms before retry...`
      );
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }

    console.log("Failed to retrieve transformBaseUrl after retries.");
    return null;
  }

  // STEP 4: Fetch metadata with retry handling
  let assetInfo = await fetchAssetInfoWithRetry(mediaId);

  if (!assetInfo) {
    console.log(
      "Asset info could not be retrieved with a valid transformBaseUrl."
    );
  } else {
    console.log("Asset info:", assetInfo);
    console.log("DAT base URL:", assetInfo.transformBaseUrl);
  }

  // STEP A: Generate DAT transformation URLs for presets
  function generateDatUrls(assetInfo, presets = []) {
    if (!assetInfo || !assetInfo.transformBaseUrl) return {};

    const base = assetInfo.transformBaseUrl;

    // base looks like:
    // https://jakob-spott.bynder.com/transform/<id>/<slug>

    const parts = base.split("/");
    const slug = parts.pop(); // last segment
    const id = parts.pop(); // second to last segment

    const urls = {};

    presets.forEach((preset) => {
      urls[preset] = `https://jakob-spott.bynder.com/transform/${preset}/${id}/${slug}`;
    });

    return urls;
  }

  // STEP B1: Helper to download a DAT-transformed image
  async function downloadDatImage(url, presetName) {
    console.log(`Downloading DAT image for preset "${presetName}" from: ${url}`);

    try {
      const res = await fetch(url, {
        method: "GET",
        headers: {
          // For non-public assets, keep the same auth
          Authorization: process.env.BYNDER_TOKEN,
        },
      });

      if (!res.ok) {
        console.error(
          `Failed to download DAT image for preset "${presetName}". Status: ${res.status} ${res.statusText}`
        );
        return null;
      }

      const arrayBuffer = await res.arrayBuffer();
      const buffer = Buffer.from(arrayBuffer);

      console.log(
        `Downloaded DAT image for preset "${presetName}". Size: ${buffer.length} bytes`
      );

      return buffer;
    } catch (err) {
      console.error(
        `Error while downloading DAT image for preset "${presetName}":`,
        err
      );
      return null;
    }
  }

  // STEP C1: Helper to upload a DAT image back to Binder as a new asset
  async function uploadDatImageToBynder(presetName, buffer, assetInfo) {
    if (!buffer || !assetInfo) {
      console.log(
        `Skipping upload for preset "${presetName}" - missing buffer or assetInfo.`
      );
      return;
    }

    const originalName = assetInfo.name || "DAT-asset";
    const sanitizedOriginal = originalName.replace(/[^a-zA-Z0-9-_]+/g, "-");

    const ext =
      (assetInfo.extension && assetInfo.extension[0]) || "jpg";

    // Your chosen naming convention:
    // <original-name>__<preset-name>.<ext>
    const filename = `${sanitizedOriginal}__${presetName}.${ext}`;

    console.log(
      `Preparing to upload transformed file for preset "${presetName}" as "${filename}", size ${buffer.length} bytes`
    );

    try {

        // 1) Bynder S3 bucket is fixed for your region (us-east-1)
        const s3Endpoint = "https://bynder-public-us-east-1.s3.amazonaws.com/";
        console.log("Upload S3 endpoint:", s3Endpoint);

        // 2) Initialise upload
        const initBody = new URLSearchParams({
          filename,
          filesize: buffer.length.toString(),
        });


      const initRes = await fetch(
        "https://jakob-spott.bynder.com/api/upload/init",
        {
          method: "POST",
          headers: {
            Authorization: process.env.BYNDER_TOKEN,
          },
          body: initBody,
        }
      );

      if (!initRes.ok) {
        console.error(
          `Failed to initialise upload. Status: ${initRes.status} ${initRes.statusText}`
        );
        const text = await initRes.text();
        console.error("Init upload response body:", text);
        return;
      }

      const initJson = await initRes.json();
      console.log("Upload init response:", initJson);

      const uploadId = initJson.s3file?.uploadid;
      const mp = initJson.multipart_params;

      if (!uploadId || !mp) {
        console.error("Init response missing uploadid or multipart_params.", initJson);
        return;
      }

      const totalChunks = Math.ceil(buffer.length / CHUNK_SIZE);
      console.log(
        `Uploading in ${totalChunks} chunk(s) of up to ${CHUNK_SIZE} bytes.`
      );

          // 3) Upload file chunks to S3
        for (let i = 0; i < totalChunks; i++) {
          const chunkNumber = i + 1;
          const start = i * CHUNK_SIZE;
          const end = Math.min(start + CHUNK_SIZE, buffer.length);
          const chunkBuffer = buffer.subarray(start, end);

          // Build multipart/form-data for S3
          const form = new FormData();

          // Include all multipart_params first
          for (const [k, v] of Object.entries(mp)) {
            form.append(k, v);
          }

          // Required chunk-specific fields
          form.append("name", filename);
          form.append("chunk", String(chunkNumber));
          form.append("chunks", String(totalChunks));
          form.append("Filename", filename);

          // Append the file chunk as a buffer
          form.append("file", chunkBuffer, {
            filename: filename,
            contentType: 'application/octet-stream'
          });

          console.log(
            `Uploading chunk ${chunkNumber}/${totalChunks} to S3`
          );

          // Convert FormData stream to Buffer for fetch
          const formBuffer = await new Promise((resolve, reject) => {
            const chunks = [];
            form.on('data', (chunk) => {
              // Ensure chunk is a Buffer
              chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
            });
            form.on('end', () => resolve(Buffer.concat(chunks)));
            form.on('error', reject);
            // Trigger the stream to start emitting data
            form.resume();
          });

          const s3Res = await fetch(s3Endpoint, {
            method: "POST",
            headers: form.getHeaders(),
            body: formBuffer,
          });

          if (!s3Res.ok) {
            console.error(
              `S3 upload failed for chunk ${chunkNumber}. Status: ${s3Res.status} ${s3Res.statusText}`
            );
            const s3Text = await s3Res.text();
            console.error("S3 response body:", s3Text);
            return;
          }

          console.log(`Chunk ${chunkNumber}/${totalChunks} uploaded successfully.`);
        }


      // 4) Register uploaded chunks with Bynder (only for multi-chunk uploads)
      if (totalChunks > 1) {
        const chunksArray = Array.from(
          { length: totalChunks },
          (_, idx) => idx + 1
        );

        const registerBody = JSON.stringify({
          chunks: chunksArray,
        });

        const registerRes = await fetch(
          `https://jakob-spott.bynder.com/api/v4/upload/${uploadId}`,
          {
            method: "POST",
            headers: {
              Authorization: process.env.BYNDER_TOKEN,
              "Content-Type": "application/json",
            },
            body: registerBody,
          }
        );

        if (!registerRes.ok) {
          console.error(
            `Failed to register uploaded chunks. Status: ${registerRes.status} ${registerRes.statusText}`
          );
          const regText = await registerRes.text();
          console.error("Register chunks response body:", regText);
          return;
        }

        const registerJson = await registerRes.json().catch(() => null);
        console.log("Register uploaded chunks response:", registerJson);
      } else {
        console.log("Single chunk upload - skipping chunk registration");
      }

      // Wait for S3 to process the uploaded file before finalizing
      console.log("Waiting 5 seconds for S3 to process the upload...");
      await new Promise((resolve) => setTimeout(resolve, 5000));

      // 5) Finalise the upload to get the importId (with retry logic)
      console.log(`Finalizing upload for uploadId: ${uploadId}`);

      // Helper function to finalize upload with retry
      async function finalizeUploadWithRetry(retries = 3, delayMs = 3000) {
        const finalizeParams = new URLSearchParams({
          id: uploadId,
          targetid: initJson.s3file.targetid,
          s3_filename: `${initJson.s3_filename}/p1`, // Format: path/p{chunkNumber}
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
            return finalizeJson;
          }

          // Check if we should retry
          if (finalizeJson?.retry || finalizeJson?.message === "Upload not ready") {
            console.log(
              `Upload not ready yet. Waiting ${delayMs} ms before retry...`,
              finalizeJson
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

      const finalizeJson = await finalizeUploadWithRetry();
      if (!finalizeJson?.importId) {
        console.error("No importId obtained from finalize endpoint");
        return;
      }

      const importId = finalizeJson.importId;

      console.log(`Got importId: ${importId}, saving as new asset...`);

      // 6) Save as new asset using the importId
      const saveParams = new URLSearchParams({
        brandId: assetInfo.brandId || '',
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
        return;
      }

      const saveJson = await saveRes.json().catch(() => null);
      console.log(
        `Successfully saved asset for preset "${presetName}" as "${filename}". Save response:`,
        saveJson
      );
    } catch (err) {
      console.error(
        `Error during upload flow for preset "${presetName}" / transformed file:`,
        err
      );
    }
  }

  // Define your DAT presets here (keep in sync with your portal)
  const presets = ["TestPreset", "crop300"]; // adjust as needed

  // Extract DAT URLs directly from assetInfo.thumbnails instead of constructing them
  let datUrls = {};
  if (assetInfo && assetInfo.thumbnails) {
    presets.forEach((preset) => {
      if (assetInfo.thumbnails[preset]) {
        datUrls[preset] = assetInfo.thumbnails[preset];
      }
    });
    console.log("DAT URLs from thumbnails:", datUrls);
  }

  // STEP B2: Download all DAT images (into memory)
  const downloadedImages = {};

  if (assetInfo && Object.keys(datUrls).length > 0) {
    for (const [presetName, url] of Object.entries(datUrls)) {
      const fileBuffer = await downloadDatImage(url, presetName);
      if (fileBuffer) {
        downloadedImages[presetName] = fileBuffer;
      }
    }

    console.log(
      "Finished downloading DAT images. Presets downloaded:",
      Object.keys(downloadedImages)
    );
  } else {
    console.log("No DAT URLs generated, skipping download step.");
  }

  // STEP C2: Upload downloaded DAT images back to Binder as new assets
  if (assetInfo && Object.keys(downloadedImages).length > 0) {
    for (const [presetName, buffer] of Object.entries(downloadedImages)) {
      await uploadDatImageToBynder(presetName, buffer, assetInfo);
    }
  } else {
    console.log("No downloaded DAT images to upload.");
  }

  // STEP 5: Return success response
  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
    body: JSON.stringify({
      message: "Webhook processed including DAT download and upload flow",
      receivedAt: new Date().toISOString(),
      downloadedPresets: Object.keys(downloadedImages),
    }),
  };
};
