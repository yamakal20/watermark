import os
import uuid
import subprocess
import threading
import requests
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import boto3
from botocore.config import Config

app = FastAPI(title="Video Watermark + R2 Streaming")

R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")
R2_BUCKET = os.getenv("R2_BUCKET")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "")

WATERMARK_TEXT = os.getenv("WATERMARK_TEXT", "lugyiapplication.vercel.app")
CRF = os.getenv("CRF", "28")

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY,
    aws_secret_access_key=R2_SECRET_KEY,
    config=Config(
        signature_version="s3v4",
        s3={"addressing_style": "path"},
    ),
    region_name="auto",
)


class VideoRequest(BaseModel):
    url: HttpUrl
    filename: str | None = None


def build_ffmpeg_cmd(input_url: str, text: str) -> list:
    """
    Read directly from URL, write to stdout as fragmented MP4.
    No disk usage at all.
    """
    font_path = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
    drawtext = (
        f"drawtext=fontfile={font_path}:"
        f"text='{text}':"
        f"fontcolor=white:"
        f"fontsize=h/30:"
        f"box=1:boxcolor=black@0.4:boxborderw=8:"
        f"x=w-tw-20:y=h-th-20"
    )

    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-i", input_url,                              # FFmpeg downloads directly
        "-vf", drawtext,
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", CRF,
        "-c:a", "aac",
        "-b:a", "128k",
        # Fragmented MP4 so it can be streamed (no seek required)
        "-movflags", "+frag_keyframe+empty_moov+default_base_moof+faststart",
        "-f", "mp4",
        "pipe:1",                                     # output to stdout
    ]


class FFmpegStreamReader:
    """
    Wraps ffmpeg stdout as a file-like object for boto3 upload_fileobj.
    boto3 will read in chunks and use multipart upload automatically.
    """
    def __init__(self, process):
        self.process = process
        self.stdout = process.stdout

    def read(self, size=-1):
        if size == -1:
            return self.stdout.read()
        return self.stdout.read(size)


def log_ffmpeg_stderr(process):
    """Drain stderr so the pipe doesn't block."""
    for line in iter(process.stderr.readline, b""):
        if line:
            print(f"[ffmpeg] {line.decode(errors='ignore').strip()}")


@app.get("/")
def root():
    return {"status": "ok", "service": "video-watermark-streaming"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.post("/process")
def process_video(req: VideoRequest):
    job_id = uuid.uuid4().hex[:12]
    key = req.filename or f"videos/{job_id}.mp4"
    if not key.endswith(".mp4"):
        key += ".mp4"

    cmd = build_ffmpeg_cmd(str(req.url), WATERMARK_TEXT)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=10 * 1024 * 1024,  # 10MB buffer
    )

    # Start stderr drainer thread so ffmpeg won't block
    stderr_thread = threading.Thread(
        target=log_ffmpeg_stderr, args=(process,), daemon=True
    )
    stderr_thread.start()

    try:
        # Stream directly to R2 (multipart upload happens automatically)
        s3.upload_fileobj(
            FFmpegStreamReader(process),
            R2_BUCKET,
            key,
            ExtraArgs={"ContentType": "video/mp4"},
            Config=boto3.s3.transfer.TransferConfig(
                multipart_threshold=8 * 1024 * 1024,    # 8MB parts
                multipart_chunksize=8 * 1024 * 1024,
                max_concurrency=4,
                use_threads=True,
            ),
        )

        # Wait for ffmpeg to finish
        process.wait(timeout=30)

        if process.returncode != 0:
            raise HTTPException(
                status_code=500,
                detail=f"FFmpeg exited with code {process.returncode}",
            )

        # Get final size from R2
        head = s3.head_object(Bucket=R2_BUCKET, Key=key)
        final_size = head["ContentLength"]

        public_url = (
            f"{R2_PUBLIC_URL.rstrip('/')}/{key}"
            if R2_PUBLIC_URL
            else f"{R2_ENDPOINT}/{R2_BUCKET}/{key}"
        )

        return {
            "success": True,
            "job_id": job_id,
            "url": public_url,
            "key": key,
            "final_size_mb": round(final_size / 1024 / 1024, 2),
            "mode": "streaming (no disk)",
        }

    except Exception as e:
        try:
            process.kill()
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Processing failed: {e}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
