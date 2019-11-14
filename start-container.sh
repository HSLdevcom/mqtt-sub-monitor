#!/bin/bash
docker run -d -p 127.0.0.1:5000:5000 -e MQTT_HOST='mqtt.hsl.fi' -e MQTT_TOPIC='/hfp/v2/journey/ongoing/#' hellej/mqtt-sub-monitor:latest
