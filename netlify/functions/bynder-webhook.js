exports.handler = async function(event, context) {
  console.log("Incoming request body:", event.body);

  let parsed;
  try {
    // First parse the outer SNS wrapper
    parsed = JSON.parse(event.body);
  } catch (err) {
    console.error("Failed to parse event.body as JSON", err);
    return { statusCode: 400, body: "Invalid JSON" };
  }

  // Ensure it is an SNS notification
  if (parsed.Type === "Notification" && parsed.Message) {
    try {
      // Parse the REAL Binder message inside the SNS wrapper
      const binderMessage = JSON.parse(parsed.Message);

      // Extract the asset ID
      const mediaId = binderMessage.media_id;
      console.log("Extracted media ID:", mediaId);

    } catch (err) {
      console.error("Failed to parse Binder Message", err);
    }
  } else {
    console.log("Received non SNS payload");
  }

  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*"
    },
    body: JSON.stringify({
      message: "Webhook received and parsed",
      receivedAt: new Date().toISOString()
    })
  };
};
