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

  // Define your DAT presets here
  const presets = ["TestPreset", "crop300"]; // add more as needed

  let datUrls = {};
  if (assetInfo) {
    datUrls = generateDatUrls(assetInfo, presets);
    console.log("Generated DAT URLs:", datUrls);
  }

  // STEP 5: Return success response
  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
    },
    body: JSON.stringify({
      message: "Webhook processed",
      receivedAt: new Date().toISOString(),
    }),
  };
};
