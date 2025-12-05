exports.handler = async function(event, context) {
  // Netlify sends the request data in "event"
  // We just log the body for now

  console.log("Incoming request body:", event.body);

  return {
    statusCode: 200,
    headers: {
      "Content-Type": "application/json"
    },
    body: JSON.stringify({
      message: "Webhook received",
      receivedAt: new Date().toISOString()
    })
  };
};
