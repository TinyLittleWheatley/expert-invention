import asyncio
import json
import shlex
import boto3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI()

app.mount("/static", StaticFiles(directory="."), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    with open("index.html", "r") as f:
        return f.read()


def sse(data: dict):
    return f"data: {json.dumps(data)}\n\n"


@app.get("/stream")
async def stream(request: Request):
    p = request.query_params

    endpoint = p["endpoint"]
    access_key = p["access_key"]
    secret_key = p["secret_key"]
    bucket = p["bucket"]
    key = p["key"]
    url = p["url"]

    async def event_generator():
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        # start yt-dlp
        cmd = f'yt-dlp -o - "{url}"'
        process = await asyncio.create_subprocess_exec(
            *shlex.split(cmd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )

        mp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mp["UploadId"]

        parts = []
        queue = asyncio.Queue(maxsize=3)
        done_reading = False

        yield sse({"type": "status", "message": "started"})

        # ----------------------
        # Producer
        # ----------------------
        async def producer():
            nonlocal done_reading

            buffer = b""
            chunk_size = 5 * 1024 * 1024

            while True:
                chunk = await process.stdout.read(1024 * 64)
                if not chunk:
                    break

                buffer += chunk

                if len(buffer) >= chunk_size:
                    await queue.put(buffer)
                    buffer = b""

                # lightweight heartbeat
                await queue.put(None)  # signal activity

            if buffer:
                await queue.put(buffer)

            done_reading = True

        producer_task = asyncio.create_task(producer())

        part_number = 1
        heartbeat_count = 0

        try:
            while not (done_reading and queue.empty()):
                item = await queue.get()

                # heartbeat signal
                if item is None:
                    heartbeat_count += 1
                    yield sse({
                        "type": "heartbeat",
                        "count": heartbeat_count
                    })
                    continue

                data = item

                resp = s3.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data,
                )

                parts.append({
                    "PartNumber": part_number,
                    "ETag": resp["ETag"]
                })

                yield sse({
                    "type": "upload",
                    "part": part_number,
                    "bytes": len(data)
                })

                part_number += 1

                if await request.is_disconnected():
                    raise Exception("Client disconnected")

            await producer_task

            s3.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            yield sse({"type": "status", "message": "completed"})

        except Exception as e:
            s3.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
            )
            yield sse({"type": "error", "message": str(e)})

        finally:
            await process.wait()

    return StreamingResponse(event_generator(), media_type="text/event-stream")
