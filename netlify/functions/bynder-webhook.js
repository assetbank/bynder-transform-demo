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

          const keyBase = mp.key; // from init
          const chunkKey = `${keyBase}/p${chunkNumber}`;

          // Build multipart/form-data for S3
          const form = new FormData();

          // Include all multipart_params first (except key/Filename/name, which we override)
          for (const [k, v] of Object.entries(mp)) {
            if (k === "key" || k === "Filename" || k === "name") continue;
            form.append(k, v);
          }

          // Required chunk-specific fields
          form.append("key", chunkKey);
          form.append("Filename", chunkKey);
          form.append("name", filename);
          form.append("chunk", String(chunkNumber));
          form.append("chunks", String(totalChunks));

          // Append the file chunk as a buffer
          form.append("file", chunkBuffer, {
            filename: filename,
            contentType: 'application/octet-stream'
          });

          console.log(
            `Uploading chunk ${chunkNumber}/${totalChunks} to S3 as key ${chunkKey}`
          );

          const s3Res = await fetch(s3Endpoint, {
            method: "POST",
            body: form,
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


      // 4) Register uploaded chunks with Bynder
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

      // 5) Finalise the upload so Bynder creates the asset and triggers derivatives
      const finalizeRes = await fetch(
        `https://jakob-spott.bynder.com/api/v4/upload/${uploadId}/`,
        {
          method: "POST",
          headers: {
            Authorization: process.env.BYNDER_TOKEN,
            "Content-Type": "application/json",
          },
          // keeping it simple: no extra metadata in the body (Option A)
          body: JSON.stringify({
            // You could pass brandId, originalId, etc. here if needed.
            // brandId: assetInfo.brandId,
            intent: "upload",
          }),
        }
      );

      if (!finalizeRes.ok) {
        console.error(
          `Failed to finalise upload. Status: ${finalizeRes.status} ${finalizeRes.statusText}`
        );
        const finText = await finalizeRes.text();
        console.error("Finalize upload response body:", finText);
        return;
      }

      const finalizeJson = await finalizeRes.json().catch(() => null);
      console.log(
        `Successfully finalised upload for preset "${presetName}" as "${filename}". Finalize response:`,
        finalizeJson
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

  let datUrls = {};
  if (assetInfo) {
    datUrls = generateDatUrls(assetInfo, presets);
    console.log("Generated DAT URLs:", datUrls);
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
