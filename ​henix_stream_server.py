import os
import asyncio
from pyrogram import Client
from aiohttp import web

# Henix 2GB-4GB Telegram MTProto Streamer (24/7 Free Hosting Ready)
API_ID = int(os.environ.get("API_ID", "33799155"))
API_HASH = os.environ.get("API_HASH", "07a8a35b00434b46db2eb490a6da241b")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "9117374395:AAGe0mKePsCKQciNJpkh610ASrLrv1gy3ss")
CHANNEL_ID = int(os.environ.get("CHANNEL_ID", "-1004310348370"))

app = Client(
    "henix_streamer",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

async def health(request):
    return web.Response(text="Henix 4GB Stream Server Running 24/7!")

async def stream_video(request):
    try:
        msg_id = int(request.match_info["msg_id"])
    except ValueError:
        return web.Response(status=400, text="Invalid message ID")

    msg = await app.get_messages(CHANNEL_ID, msg_id)
    media = msg.video or msg.document
    if not media:
        return web.Response(status=404, text="Video not found in channel")

    file_size = media.file_size
    mime_type = getattr(media, "mime_type", None) or "video/mp4"
    range_header = request.headers.get("Range")

    start = 0
    end = file_size - 1
    status = 200

    if range_header:
        bytes_range = range_header.replace("bytes=", "").split("-")
        start = int(bytes_range[0]) if bytes_range[0] else 0
        end = int(bytes_range[1]) if len(bytes_range) > 1 and bytes_range[1] else file_size - 1
        status = 206

    content_length = end - start + 1
    headers = {
        "Content-Type": mime_type,
        "Accept-Ranges": "bytes",
        "Content-Length": str(content_length),
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Access-Control-Allow-Origin": "*",
    }

    resp = web.StreamResponse(status=status, headers=headers)
    await resp.prepare(request)

    chunk_size = 1024 * 1024  # 1MB chunks
    offset = start // chunk_size
    skip = start % chunk_size
    sent = 0

    async for chunk in app.stream_media(msg, offset=offset):
        if skip:
            chunk = chunk[skip:]
            skip = 0
        if sent + len(chunk) > content_length:
            chunk = chunk[: content_length - sent]
        await resp.write(chunk)
        sent += len(chunk)
        if sent >= content_length:
            break

    await resp.write_eof()
    return resp

async def main():
    await app.start()
    server = web.Application()
    server.router.add_get("/", health)
    server.router.add_get("/stream/{msg_id}", stream_video)
    runner = web.AppRunner(server)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Henix Streamer live on port {port}")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
