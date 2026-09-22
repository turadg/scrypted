# Doorbird Plugin for Scrypted

The Doorbird Plugin bridges compatible Doorbird video doorbell cameras to Scrypted.

# Notes
* Make sure that the user you want to use for the Doorbird plugin login has the API access rights.
* Doorbrid cameras are quite limited in terms of maximum number of concurrent streams. Keep this in mind if you are also using other software with the Doorbird station. You have the possibility to override the internally used RTSP URL and provide another RTSP server which provides the video stream.
* The doorbird mobile apps always have precedence over the public LAN API. So when somebody uses the Doorbird app to talk to the Doorbird station, the streams will be interrupted.
* The doorbird camera just provides JPEG snapshots with VGA resolution. You can use the scrypted snapshot plugin to get a snapshot from the higher resolution video stream. Just set the option in the snapshot plugin to "enabled".

# Door Relays

Each relay reported by the Doorbird station is exposed as a separate Lock device underneath the camera, so an electric door strike wired to a relay can be released from Scrypted, HomeKit, Google Home, or an automation.

* The relays are discovered from the station itself. Built-in relays appear as `1` and `2`; relays on peripheral controllers (e.g. a DoorBird A1081 gatekeeper) appear under ids such as `ghdoor1@1`.
* Doorbird relays are momentary: the station closes the contact for the duration configured in its own settings and releases it again by itself. There is no way to read the relay back, so unlocking reports the lock as unlocked and then reports it as locked again after the "Relock Delay" configured in the camera's advanced settings. Locking only resets the reported state; it does not send anything to the station.
* The API user must have the "door opener" permission assigned in the Doorbird app, otherwise the station rejects the request and the unlock fails.
