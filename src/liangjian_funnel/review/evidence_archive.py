"""Content-addressed, compressed A5 source observations for later reproducibility."""
import gzip
import hashlib
import json
import os
from pathlib import Path
import tempfile


def archive_observation(directory: Path, payload: dict) -> dict:
    body=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(',',':'),default=str).encode()
    digest=hashlib.sha256(body).hexdigest()
    day=str(payload['cutoff_at'])[:10]
    # The caller supplies a parsed ISO datetime, but validate the path segment
    # here as well; no provider text is used in file names.
    from datetime import date
    date.fromisoformat(day)
    root=directory.resolve()/day
    root.mkdir(parents=True,exist_ok=True)
    path=root/(digest+'.json.gz')
    compressed=gzip.compress(body,compresslevel=6,mtime=0)
    if path.is_file():
        if hashlib.sha256(gzip.decompress(path.read_bytes())).hexdigest()!=digest:
            raise ValueError('A5_EVIDENCE_EXISTING_HASH_MISMATCH')
    else:
        descriptor,temporary=tempfile.mkstemp(prefix='.a5-',dir=root)
        try:
            with os.fdopen(descriptor,'wb') as stream:
                stream.write(compressed);stream.flush();os.fsync(stream.fileno())
            os.replace(temporary,path)
        finally:
            if os.path.exists(temporary):os.unlink(temporary)
    return {'schema_version':'a5-market-observation/1.0','relative_path':f'{day}/{path.name}',
            'sha256':digest,'compressed_bytes':path.stat().st_size,'uncompressed_bytes':len(body),
            'fetched_at':payload.get('verification_fetched_at'),'cutoff_at':payload['cutoff_at'],
            'scope':'POST_HOC_OBSERVATION_NOT_ORIGINAL_DECISION_KNOWLEDGE'}
