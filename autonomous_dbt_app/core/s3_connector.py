import os
import re
from pathlib import Path
from typing import Dict, Any, List, Optional
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

class S3Connector:
    """
    Scout Bee Buzz: S3 Cloud Ingestion Engine.
    Fetches raw client datasets from AWS S3 bucket folders.
    Supports S3 URIs (s3://bucket/prefix/), https URLs, custom credentials,
    and fallback to local staging data when DevOps credentials are in progress.
    """
    def __init__(self,
                 aws_access_key_id: Optional[str] = None,
                 aws_secret_access_key: Optional[str] = None,
                 aws_region: Optional[str] = None,
                 aws_session_token: Optional[str] = None):
        self.access_key = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.region = aws_region or os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self.session_token = aws_session_token or os.getenv("AWS_SESSION_TOKEN")

    def parse_s3_uri(self, s3_uri: str) -> tuple[str, str]:
        """Parses s3://bucket/prefix/ or https://bucket.s3.amazonaws.com/prefix/"""
        clean_uri = s3_uri.strip()
        if clean_uri.startswith("s3://"):
            parts = clean_uri[5:].split("/", 1)
            bucket = parts[0]
            prefix = parts[1] if len(parts) > 1 else ""
            return bucket, prefix
        
        # Handle https://bucket.s3.amazonaws.com/prefix/
        https_match = re.match(r'https?://([^.]+)\.s3[^/]*/(.*)', clean_uri)
        if https_match:
            return https_match.group(1), https_match.group(2)
            
        raise ValueError(f"Invalid S3 URI or URL format: {s3_uri}. Expected format: s3://bucket-name/folder/")

    def sync_from_s3(self, s3_uri: str, destination_dir: Path, fallback_local_dir: Optional[Path] = None) -> Dict[str, Any]:
        """
        Downloads datasets from S3 bucket folder into destination_dir.
        If credentials are not yet provided, uses fallback_local_dir with audit logging.
        """
        destination_dir = Path(destination_dir)
        destination_dir.mkdir(parents=True, exist_ok=True)

        result = {
            "s3_uri": s3_uri,
            "destination_dir": str(destination_dir),
            "files_downloaded": [],
            "source_type": "AWS_S3",
            "status": "SUCCESS",
            "message": ""
        }

        # Check if AWS credentials exist
        if not (self.access_key and self.secret_key):
            if fallback_local_dir and Path(fallback_local_dir).exists():
                print(f"[Scout Bee Buzz] AWS S3 credentials pending from DevOps. Utilizing client staging fallback dataset from: {fallback_local_dir}")
                result["source_type"] = "LOCAL_STAGING_FALLBACK"
                result["message"] = "DevOps AWS S3 credentials pending. Ingested from client staging workspace."
                
                # Copy or link files from fallback
                import shutil
                for src_file in Path(fallback_local_dir).iterdir():
                    if src_file.is_file() and not src_file.name.startswith("."):
                        target_file = destination_dir / src_file.name
                        if not target_file.exists():
                            shutil.copy2(src_file, target_file)
                        result["files_downloaded"].append({
                            "file_name": src_file.name,
                            "size_bytes": src_file.stat().st_size,
                            "s3_key": f"fallback/{src_file.name}"
                        })
                return result
            else:
                result["status"] = "ERROR_CREDENTIALS"
                result["message"] = "AWS S3 Credentials (AWS_ACCESS_KEY_ID & AWS_SECRET_ACCESS_KEY) not provided and no local fallback found."
                return result

        try:
            bucket_name, prefix = self.parse_s3_uri(s3_uri)
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                aws_session_token=self.session_token,
                region_name=self.region
            )

            paginator = s3_client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith("/"):
                        continue
                    filename = Path(key).name
                    dest_path = destination_dir / filename
                    print(f"[Scout Bee Buzz] Downloading s3://{bucket_name}/{key} -> {dest_path}")
                    s3_client.download_file(bucket_name, key, str(dest_path))
                    result["files_downloaded"].append({
                        "file_name": filename,
                        "size_bytes": obj["Size"],
                        "s3_key": key
                    })

            result["message"] = f"Successfully downloaded {len(result['files_downloaded'])} files from s3://{bucket_name}/{prefix}"
            return result

        except Exception as e:
            if fallback_local_dir and Path(fallback_local_dir).exists():
                print(f"[Scout Bee Buzz] S3 connection error ({e}). Engaging client local staging fallback.")
                result["source_type"] = "LOCAL_STAGING_FALLBACK"
                result["message"] = f"S3 fetch failed ({str(e)}). Fallen back to local staging dataset."
                import shutil
                for src_file in Path(fallback_local_dir).iterdir():
                    if src_file.is_file() and not src_file.name.startswith("."):
                        target_file = destination_dir / src_file.name
                        if not target_file.exists():
                            shutil.copy2(src_file, target_file)
                        result["files_downloaded"].append({
                            "file_name": src_file.name,
                            "size_bytes": src_file.stat().st_size,
                            "s3_key": f"fallback/{src_file.name}"
                        })
                return result
            else:
                result["status"] = "ERROR_S3_FETCH"
                result["message"] = str(e)
                return result

    def upload_to_s3(self, local_path: Path, s3_prefix: str, bucket_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Uploads a local file or directory to the target S3 prefix.
        Uses chunked multipart uploading for large datasets.
        """
        local_path = Path(local_path)
        bucket = bucket_name or os.getenv("S3_DEFAULT_BUCKET", "supplycopia-dbt-ingestion")
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            aws_session_token=self.session_token,
            region_name=self.region
        )

        uploaded = []
        clean_prefix = s3_prefix.strip().strip("/")

        files_to_upload = []
        if local_path.is_file():
            files_to_upload.append((local_path, f"{clean_prefix}/{local_path.name}" if clean_prefix else local_path.name))
        elif local_path.is_dir():
            for item in local_path.rglob("*"):
                if item.is_file() and not item.name.startswith("."):
                    rel_sub = item.relative_to(local_path)
                    s3_key = f"{clean_prefix}/{rel_sub}" if clean_prefix else str(rel_sub)
                    files_to_upload.append((item, s3_key))

        for file_p, key in files_to_upload:
            size_mb = file_p.stat().st_size / (1024 * 1024)
            print(f"[Carrier Bee Nectar] Uploading {file_p.name} ({size_mb:.2f} MB) -> s3://{bucket}/{key}...")
            s3_client.upload_file(str(file_p), bucket, key)
            uploaded.append({
                "local_file": str(file_p),
                "s3_key": key,
                "size_bytes": file_p.stat().st_size
            })

        return {
            "status": "SUCCESS",
            "bucket": bucket,
            "s3_prefix": clean_prefix,
            "uploaded_files": uploaded,
            "count": len(uploaded)
        }

