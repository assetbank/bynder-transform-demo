// bynder-webhook.js

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

  if (parsed.Type === "Notification" && parsed.Message) {
    try {
      const binderMessage = JSON.parse(parsed.Message);
      mediaId = binderMessage.media_id;
      console.log("Extracted media ID:", mediaId);
    } catch (err) {
      console.error("Failed to parse Binder Message", err);
    }
  }

  // STEP 2: If no media ID, nothing more to do
  if (!mediaId) {
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

  // Define your DAT presets here (keep in sync with your portal)
  const presets = ["TestPreset", "crop300"]; // adjust as needed

  let datUrls = {};
  if (assetInfo) {
    datUrls = generateDatUrls(assetInfo, presets);
    console.log("Generated DAT URLs:", datUrls);
  }

  // STEP B2: Download all DAT images (into memory for now)
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

  // STEP C (next step, not implemented yet):
  // ----------------------------------------
  // Here we will:
  // - Take each entry in `downloadedImages`
  // - Run the modern upload flow:
  //   1) POST /v7/file_cmds/upload/prepare  (filename, filesize, chunksCount, sha256, etc.)
  //   2) POST to the returned S3 URL to upload the file (single chunk for our case)
  //   3) POST /api/v4/upload/{id} to register the chunk
  //   4) Finalize and save as a new asset via the appropriate "save as new asset" endpoint
  //
  // For now we just log what we have, so you can see the full end of Step B in action.

  // STEP 5: Return success response
  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
    body: JSON.stringify({
      message: "Webhook processed up to DAT download step",
      receivedAt: new Date().toISOString(),
      downloadedPresets: Object.keys(downloadedImages),
    }),
  };
};
