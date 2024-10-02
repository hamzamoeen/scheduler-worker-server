const clients = new Map();  // Map to store active WebSocket connections by shop identifier

module.exports = {
    // Store a connection
    addClient: (shop, ws) => {
        clients.set(shop, ws);
    },

    // Remove a connection when the client disconnects
    removeClient: (shop) => {
        clients.delete(shop);
    },

    // Send a message to a specific shop's WebSocket
    sendMessageToShop: (shop, message) => {
        const client = clients.get(shop);
        if (client && client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(message));
        } else {
            console.log(`Client for shop ${shop} is not connected`);
        }
    },

    // Broadcast a message to all connected clients (optional)
    broadcastMessage: (message) => {
        clients.forEach((ws, shop) => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify(message));
            }
        });
    }
};
