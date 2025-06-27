const { setGlobalOptions } = require("firebase-functions");
const { onRequest } = require("firebase-functions/v2/https");
const { onSchedule } = require("firebase-functions/v2/scheduler");
const logger = require("firebase-functions/logger");
const admin = require("firebase-admin");

admin.initializeApp();
const db = admin.database();
setGlobalOptions({
  maxInstances: 10,
  timeoutSeconds: 60
});

exports.chirpstackWebhook = onRequest(
  { cors: true, methods: ["POST"] },
  async (request, response) => {
    try {
      // 1. Parse payload robustly
      let payload = request.body;
      if (typeof payload === "string") {
        try { payload = JSON.parse(payload); } catch (e) {}
      }
      if (!payload && request.rawBody) {
        try {
          payload = JSON.parse(request.rawBody.toString());
        } catch {}
      }

      // 2. Check method
      if (request.method !== "POST") {
        response.status(405).send("Method Not Allowed");
        return;
      }

      // 3. Validate payload
      if (!payload) {
        response.status(400).send("No payload received");
        return;
      }

      // 4. Smart event type detection
      let eventType = payload.event || payload.eventType || payload.type;
      if (!eventType) {
        if (payload.data && payload.fCnt !== undefined && payload.deviceInfo) {
          eventType = "up";
        } else if (payload.joinEvent) {
          eventType = "join";
        } else if (payload.statusEvent) {
          eventType = "status";
        } else if (payload.uplinkEvent) {
          eventType = "up";
        } else {
          eventType = "unknown";
        }
      }

      const deviceInfo = payload.deviceInfo || {};
      const devEui = deviceInfo.devEui || deviceInfo.devEUI || "unknown";

      // 5. Extract gatewayId from payload
      const gatewayId = payload.rxInfo?.[0]?.gatewayId ||
        payload.uplinkEvent?.rxInfo?.[0]?.gatewayId ||
        payload.gatewayId ||
        payload.gateway_id ||
        null;

      logger.info("Processing webhook:", { eventType, devEui, gatewayId });

      // Helper function: flatten data if object has "data" field
      function flattenData(decodedData) {
        let flat = decodedData;
        if (flat && typeof flat === 'object' && flat.data && typeof flat.data === 'object') {
          flat = { ...flat, ...flat.data };
          delete flat.data;
        }
        return flat;
      }

      // 6. Process uplink data only (ignore other event types)
      if (eventType === "up") {
        const data = payload.data || payload.uplinkEvent?.data || "";
        let decodedData = {};

        if (data) {
          try {
            const decodedString = Buffer.from(data, 'base64').toString('utf8');
            logger.info("Decoded string:", decodedString);

            try {
              decodedData = JSON.parse(decodedString);
            } catch {
              decodedData = {};
              const pairs = decodedString.split(',');
              pairs.forEach(pair => {
                const [key, value] = pair.split(':');
                if (key && value) {
                  const cleanKey = key.trim();
                  const cleanValue = value.trim();
                  decodedData[cleanKey] = isNaN(cleanValue) ? cleanValue : parseFloat(cleanValue);
                }
              });
            }
          } catch (error) {
            logger.error("Error decoding data:", error);
            decodedData = { rawData: data };
          }
        }

        const flatData = flattenData(decodedData);

        if (Object.keys(flatData).length > 0) {
          let savedToBuilding = false;

          if (gatewayId) {
            try {
              const buildingsSnapshot = await db.ref('buildings').once('value');
              const buildings = buildingsSnapshot.val();

              if (buildings) {
                for (const [buildingId, buildingData] of Object.entries(buildings)) {
                  if (buildingData.gateway_id === gatewayId) {
                    logger.info(`Found matching building: ${buildingId} with gateway: ${gatewayId}`);
                    if (buildingData.rooms) {
                      for (const [roomId, roomData] of Object.entries(buildingData.rooms)) {
                        if (roomData.nodes && roomData.nodes[devEui]) {
                          const nodeRef = db.ref(`buildings/${buildingId}/rooms/${roomId}/nodes/${devEui}`);
                          if (flatData.batt !== undefined) {
                            const battV = flatData.batt / 100;
                            let batt_percent = ((battV - 3.2) / (4.2 - 3.2)) * 100;
                            batt_percent = Math.round(batt_percent);
                            batt_percent = Math.max(0, Math.min(100, batt_percent));
                            flatData.batt = batt_percent;
                          }

                          await nodeRef.child('lastData').update(flatData);
                          logger.info(`Data replaced in building structure: buildings/${buildingId}/rooms/${roomId}/nodes/${devEui}/lastData`);
                          savedToBuilding = true;

                          response.status(200).json({
                            success: true,
                            message: "Data replaced in building structure successfully",
                            deviceEui: devEui,
                            gatewayId: gatewayId,
                            buildingId: buildingId,
                            roomId: roomId,
                            savedPath: `buildings/${buildingId}/rooms/${roomId}/nodes/${devEui}/lastData`,
                            savedFields: flatData
                          });
                          return;
                        }
                      }
                    }
                  }
                }
              }
            } catch (error) {
              logger.error("Error searching building structure:", error);
            }
          }

          if (!savedToBuilding) {
            const deviceRef = db.ref(`devices/${devEui}`);
            await deviceRef.child('lastData').remove();
            await deviceRef.child('lastData').set(flatData);

            response.status(200).json({
              success: true,
              message: gatewayId ?
                "Data replaced in devices (no matching building/room found)" :
                "Data replaced in devices (no gatewayId provided)",
              deviceEui: devEui,
              gatewayId: gatewayId,
              savedPath: `devices/${devEui}/lastData`,
              savedFields: flatData
            });
          }
        } else {
          response.status(200).json({
            success: false,
            message: "No data to save",
            deviceEui: devEui,
            gatewayId: gatewayId
          });
        }
      } else if (eventType === "unknown" && (payload.data || payload.uplinkEvent)) {
        const uplinkData = payload.uplinkEvent || payload;
        const data = uplinkData.data || "";
        if (data) {
          try {
            const decodedString = Buffer.from(data, 'base64').toString('utf8');
            let decodedData = {};

            try {
              decodedData = JSON.parse(decodedString);
            } catch {
              decodedData = {};
              const pairs = decodedString.split(',');
              pairs.forEach(pair => {
                const [key, value] = pair.split(':');
                if (key && value) {
                  const cleanKey = key.trim();
                  const cleanValue = value.trim();
                  decodedData[cleanKey] = isNaN(cleanValue) ? cleanValue : parseFloat(cleanValue);
                }
              });
            }
            const flatData = flattenData(decodedData);

            if (Object.keys(flatData).length > 0) {
              let savedToBuilding = false;
              if (gatewayId) {
                try {
                  const buildingsSnapshot = await db.ref('buildings').once('value');
                  const buildings = buildingsSnapshot.val();
                  if (buildings) {
                    for (const [buildingId, buildingData] of Object.entries(buildings)) {
                      if (buildingData.gateway_id === gatewayId) {
                        if (buildingData.rooms) {
                          for (const [roomId, roomData] of Object.entries(buildingData.rooms)) {
                            if (roomData.nodes && roomData.nodes[devEui]) {
                              const nodeRef = db.ref(`buildings/${buildingId}/rooms/${roomId}/nodes/${devEui}`);
                              await nodeRef.child('lastData').remove();
                              await nodeRef.child('lastData').set(flatData);
                              savedToBuilding = true;
                              response.status(200).json({
                                success: true,
                                message: "Data replaced in building structure (unknown event with data)",
                                deviceEui: devEui,
                                gatewayId: gatewayId,
                                buildingId: buildingId,
                                roomId: roomId,
                                savedPath: `buildings/${buildingId}/rooms/${roomId}/nodes/${devEui}/lastData`,
                                savedFields: flatData
                              });
                              return;
                            }
                          }
                        }
                      }
                    }
                  }
                } catch (error) {
                  logger.error("Error searching building structure for unknown event:", error);
                }
              }
              if (!savedToBuilding) {
                const deviceRef = db.ref(`devices/${devEui}`);
                await deviceRef.child('lastData').update(flatData);

                response.status(200).json({
                  success: true,
                  message: "Data replaced in devices (unknown event with data)",
                  deviceEui: devEui,
                  gatewayId: gatewayId,
                  savedPath: `devices/${devEui}/lastData`,
                  savedFields: flatData
                });
              }
            } else {
              response.status(200).json({
                success: false,
                message: "No decodable data found",
                deviceEui: devEui,
                gatewayId: gatewayId
              });
            }
          } catch (error) {
            response.status(200).json({
              success: false,
              message: "Error decoding unknown event data",
              deviceEui: devEui,
              gatewayId: gatewayId,
              error: error.message
            });
          }
        } else {
          response.status(200).json({
            success: false,
            message: "No data field in unknown event",
            deviceEui: devEui,
            gatewayId: gatewayId
          });
        }
      } else {
        response.status(200).json({
          success: true,
          message: `Event type '${eventType}' acknowledged but not processed`,
          deviceEui: devEui,
          gatewayId: gatewayId,
          eventType: eventType
        });
      }
    } catch (error) {
      logger.error("Error processing webhook:", error);
      response.status(500).json({
        success: false,
        error: error.message
      });
    }
  }
);

exports.dailyHistoryLogger = onSchedule(
  {
    schedule: '55 23 * * *',
    timeZone: 'Asia/Ho_Chi_Minh',
    maxInstances: 1,
  },
  async (event) => {
    const db = admin.database();
    const buildingsSnapshot = await db.ref('buildings').once('value');
    const buildings = buildingsSnapshot.val();

    if (!buildings) return null;
    const now = new Date();
    now.setHours(now.getHours() + 7);
    const dateStr = now.toISOString().slice(0, 10);

    for (const [buildingId, buildingData] of Object.entries(buildings)) {
      if (!buildingData.rooms) continue;
      for (const [roomId, roomData] of Object.entries(buildingData.rooms)) {
        if (!roomData.nodes) continue;

        let electric = 0;
        let water = 0;
        let hasElectric = false;
        let hasWater = false;

        for (const nodeData of Object.values(roomData.nodes)) {
          const lastData = nodeData.lastData;
          if (!lastData) continue;
          if (typeof lastData.electric === 'number') {
            electric += lastData.electric;
            hasElectric = true;
          }
          if (typeof lastData.water === 'number') {
            water += lastData.water;
            hasWater = true;
          }
        }

        if (hasElectric || hasWater) {
          const historyObj = {};
          if (hasElectric) historyObj.electric = electric;
          if (hasWater) historyObj.water = water;
          const historyRef = db.ref(`buildings/${buildingId}/rooms/${roomId}/history/${dateStr}`);
          await historyRef.set(historyObj);
        }
      }
    }
    return null;
  }
);
