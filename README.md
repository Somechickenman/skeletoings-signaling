# Skeletoings signaling server

A very small server that does one job: it hands out **room codes** (like `BONE-7K2Q`) and passes the
WebRTC "hello, here's how to reach me" messages between the host and the people joining.
It never sees game traffic. Once players are introduced, they talk to each other directly.

It has **no dependencies** (nothing to `npm install`), runs on Node 18+, and is happy on a free host.

## What you need to know first
- You deploy this **once**. Everyone who plays uses your server's address.
- Free hosts **go to sleep when nobody uses them**. The first person to connect after a quiet spell waits
  roughly 30 to 60 seconds. The game shows "Waking up the server..." and keeps trying for up to 60 seconds.
- Free hosting is more than enough: the server only exchanges a few small messages per player.

## Try it on your own computer first
1. Install Node.js (https://nodejs.org, the LTS version).
2. Open a terminal in this `server` folder and run:
   ```
   node server.js
   ```
   You should see `signaling server listening on port 8787`.
3. Open http://localhost:8787 in a browser. You should see `Skeletoings signaling server: ok`.
4. In the game folder, open `config/network.cfg` and set `url="ws://127.0.0.1:8787"`.
   Now **Play, Host** and **Play, Join** work between two copies of the game on the same computer.

## Deploy it for free on Render (recommended)
Render needs your code to live on GitHub, so we do that first.

**Part 1: put the server on GitHub**
1. Make a free account at https://github.com if you don't have one.
2. Click the **+** in the top right, then **New repository**. Name it `skeletoings-signaling`.
   Leave it **Public**. Click **Create repository**.
3. On the new empty repo page click **uploading an existing file**.
4. Drag the **contents** of this `server` folder into the page (`server.js`, `package.json`, `README.md`,
   `Dockerfile`, `fly.toml`). Not the folder itself, just the files in it.
5. Click **Commit changes**.

**Part 2: deploy on Render**
1. Make a free account at https://render.com (you can sign in with GitHub).
2. Click **New +**, then **Web Service**.
3. Choose **Build and deploy from a Git repository**, connect GitHub when asked, and pick `skeletoings-signaling`.
4. Fill in the form:
   - **Name**: anything, for example `skeletoings-signal`. This becomes part of your web address.
   - **Region**: the one closest to you and your friends.
   - **Branch**: `main`.
   - **Runtime**: `Node`.
   - **Build Command**: `npm install` (it finishes instantly, there is nothing to install).
   - **Start Command**: `node server.js`
   - **Instance Type**: **Free**.
5. Click **Create Web Service** and wait a minute or two. When the log says `signaling server listening`,
   it is live.
6. At the top of the page Render shows your address, like `https://skeletoings-signal.onrender.com`.
   Open it in a browser. You should see `Skeletoings signaling server: ok`. (The first load may take a minute.)

**Part 3: point the game at it**
1. Open `config/network.cfg` in the game folder.
2. Change the url to your address, but with `wss://` instead of `https://`:
   ```
   url="wss://skeletoings-signal.onrender.com"
   ```
3. Start the game. **Play, Host** should now show a room code. Send that code to a friend (they use
   **Play, Join**). Done.

If you don't want to edit the game folder, copy `config/network.cfg` to
`%APPDATA%\Godot\app_userdata\Skeletoings\network.cfg` and edit that copy. The game prefers it.

## Fly.io instead (also free-ish)
Fly.io has no "sleep" delay on small machines, but needs a credit card on file and a command line.
1. Install `flyctl`: https://fly.io/docs/flyctl/install/ and run `fly auth signup`.
2. In this `server` folder run `fly launch --no-deploy` and accept the existing `fly.toml`
   (choose a unique app name when asked).
3. Run `fly deploy`.
4. Your address is `https://YOUR-APP-NAME.fly.dev`. Use `wss://YOUR-APP-NAME.fly.dev` in `config/network.cfg`.

## Settings (all optional, set as environment variables on the host)
| Variable | Default | Meaning |
|---|---|---|
| `PORT` | `8787` | Render and Fly set this for you. |
| `MAX_PLAYERS` | `8` | Players per room, host included. |
| `MAX_MESSAGE_BYTES` | `16384` | Biggest message accepted. |
| `MAX_ROOMS` | `500` | Rooms open at once. |
| `ROOM_MAX_AGE_HOURS` | `12` | Rooms are also removed this long after creation. |
| `RATE_LIMIT` | `120` | Messages allowed per connection per 10 seconds. |

Rooms always disappear as soon as their host disconnects.

## TURN (only if some players can't connect)
Most players connect fine with the free Google STUN servers the game uses by default. Very strict
networks (some schools, offices and mobile carriers) block direct connections, and the game then says
the network may need a TURN server. A TURN server relays the traffic instead.
1. Get a free-tier TURN account from any provider (for example Metered.ca, Open Relay, or Cloudflare).
2. Put its address and login in the `[turn]` section of `config/network.cfg`:
   ```
   [turn]
   url="turn:your-turn-server.example.com:3478"
   username="your-username"
   credential="your-password"
   ```
   Everyone who needs it must have these settings (they live in the game's config file).

## The protocol (for the curious)
JSON text messages over a WebSocket.

| Direction | Message |
|---|---|
| player to server | `{"type":"host"}`, `{"type":"join","room":"BONE-7K2Q"}`, `{"type":"relay","to":2,"data":{...}}`, `{"type":"ping"}`, `{"type":"presence","ver":"0.1.0"}`, `{"type":"list"}`, `{"type":"set_public","public":true,"info":{...}}` |
| server to player | `hosted`, `joined`, `peer_joined`, `peer_left`, `relay`, `host_left`, `pong`, `online`, `rooms`, `error` |
| error codes | `room_not_found`, `room_full`, `bad_message`, `busy`, `rate_limited` |

Joiners can only message the host and the host can message any joiner, which keeps the game
host-authoritative (a star, not a mesh).

## Players online and public parties
- Every running copy of the game sends `presence` and gets `online` (the count of copies connected). Nothing but the game version is sent.
- A host can turn on **Public party**: the game sends `set_public` with the party's name, mode, map and phase. `list` returns public rooms that are still in the lobby and not full.
- Presence connections that go quiet for 120 seconds are dropped (`PRESENCE_IDLE_SECONDS`, `MAX_PRESENCE`).
