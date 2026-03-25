import asyncio
import json
import subprocess
import shlex
import boto3
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, StreamingResponse

app = FastAPI()

# -------------------------
# HTML UI
# -------------------------
@app.get("/", response_class=HTMLResponse)
async def index():
    return """
    <html>
    <body>
        <h2>YouTube → S3 uploader</h2>
        <form id="form">
            <input placeholder="S3 endpoint" name="endpoint"><br>
            <input placeholder="Access key" name="access_key"><br>
            <input placeholder="Secret key" name="secret_key"><br>
            <input placeholder="Bucket" name="bucket"><br>
            <input placeholder="Object key (filename)" name="key"><br>
            <input placeholder="YouTube URL" name="url"><br>
            <button type="submit">Start</button>
        </form>

        <pre id="log"></pre>

        <script>
        document.getElementById("form").onsubmit = async (e) => {
            e.preventDefault();

            const formData = new FormData(e.target);
            const params = new URLSearchParams(formData);

            const es = new EventSource("/stream?" + params.toString());

            es.onmessage = (e) => {
                document.getElementById("log").textContent += e.data + "\\n";
            };

            es.onerror = () => {
                es.close();
            };
        };
        </script>
    </body>
    </html>
    """


# -------------------------
# SSE helper
# -------------------------
def sse(data: dict):
    return f"data: {json.dumps(data)}\n\n"


# -------------------------
# Main streaming endpoint
# -------------------------
@app.get("/stream")
async def stream(request: Request):
    params = request.query_params

    endpoint = params["endpoint"]
    access_key = params["access_key"]
    secret_key = params["secret_key"]
    bucket = params["bucket"]
    key = params["key"]
    url = params["url"]

    async def event_generator():
        # Init S3 client (stateless per request)
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        # Start yt-dlp subprocess
        # Output to stdout (pipe)
        cmd = f'yt-dlp -o - "{url}"'
        process = await asyncio.create_subprocess_exec(
            *shlex.split(cmd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        # Multipart upload init
        mp = s3.create_multipart_upload(Bucket=bucket, Key=key)
        upload_id = mp["UploadId"]

        parts = []
        part_number = 1
        buffer = b""
        chunk_size = 5 * 1024 * 1024  # 5MB

        yield sse({"status": "started"})

        try:
            while True:
                chunk = await process.stdout.read(1024 * 64)
                if not chunk:
                    break

                buffer += chunk

                # upload chunks when large enough
                if len(buffer) >= chunk_size:
                    resp = s3.upload_part(
                        Bucket=bucket,
                        Key=key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=buffer,
                    )

                    parts.append({
                        "PartNumber": part_number,
                        "ETag": resp["ETag"]
                    })

                    yield sse({
                        "upload_part": part_number,
                        "bytes": len(buffer)
                    })

                    part_number += 1
                    buffer = b""

                # crude progress signal (download ongoing)
                yield sse({"download": "streaming"})

                if await request.is_disconnected():
                    raise Exception("Client disconnected")

            # final part
            if buffer:
                resp = s3.upload_part(
                    Bucket=bucket,
                    Key=key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=buffer,
                )

                parts.append({
                    "PartNumber": part_number,
                    "ETag": resp["ETag"]
                })

            # complete upload
            s3.complete_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

            yield sse({"status": "completed"})

        except Exception as e:
            s3.abort_multipart_upload(
                Bucket=bucket,
                Key=key,
                UploadId=upload_id,
            )
            yield sse({"error": str(e)})

        finally:
            await process.wait()

    return StreamingResponse(event_generator(), media_type="text/event-stream")
