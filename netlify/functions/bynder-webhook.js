exports.handler = async function(event, context) {
  console.log("Incoming request body:", event.body);

  let parsed;
  try {
    parsed = JSON.parse(event.body);
  } catch (err) {
    console.error("Failed to parse event.body as JSON", err);
    return { statusCode: 400, body: "Invalid JSON" };
  }

  let mediaId = null;

  // Parse SNS wrapper
  if (parsed.Type === "Notification" && parsed.Message) {
    try {
      const binderMessage = JSON.parse(parsed.Message);
      mediaId = binderMessage.media_id;
      console.log("Extracted media ID:", mediaId);
    } catch (err) {
      console.error("Failed to parse Binder Message", err);
    }
  }

  // Fetch asset info from Bynder API
  if (mediaId) {
    try {
      const response = await fetch(
        `https://jakob-spott.bynder.com/api/v4/media/${mediaId}`,
        {
          method: "GET",
          headers: {
            "Authorization": process.env.BYNDER_TOKEN,
            "Content-Type": "application/json"
          }
        }
      );

      // Parse JSON directly
      const assetInfo = await response.json();
      console.log("Asset info:", assetInfo);

      // Extract the DAT transform base URL
      const datBaseUrl = assetInfo.transformBaseUrl;
      console.log("DAT base URL:", datBaseUrl);

    } catch (err) {
      console.error("Error fetching Binder asset info:", err);
    }
  }

  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*"
    },
    body: JSON.stringify({
      message: "Webhook processed",
      receivedAt: new Date().toISOString()
    })
  };
};
