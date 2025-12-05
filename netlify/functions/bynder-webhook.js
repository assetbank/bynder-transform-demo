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

  // If we extracted a media ID, try to fetch asset info
  if (mediaId) {
    try {
      const response = await fetch(
        `https://jakob-spott.bynder.com/api/v4/media/${mediaId}`,
        {
          method: "GET",
          headers: {
            "Authorization": `Token ${process.env.BYNDER_TOKEN}`,
            "Content-Type": "application/json"
          }
        }
);


      const raw = await response.text();
      console.log("Raw Binder response:", raw);


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
