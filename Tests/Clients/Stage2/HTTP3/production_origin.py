import argparse
import asyncio
import os

from fidelity import OCTET_FIELDS, field_hex
from peer import Origin, serve


ORIGINAL_FIELDS = OCTET_FIELDS + [(b"x-replaced", b"\xff"), (b"x-wide", b"\xe2\x82\xac")]


class ProductionOrigin(Origin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.production_bodies = {}
        self.stats.setdefault("productionRequests", [])

    def headers(self, event):
        fields = dict(event.headers)
        path = fields.get(b":path")
        if path is None or not path.startswith(b"/production/"):
            return super().headers(event)
        assert len(self.paths) < 64 and len(self.stats["productionRequests"]) < 256
        self.paths[event.stream_id] = path
        self.stats["requests"] += 1
        self.stats["paths"].append(path.decode("ascii"))
        record = {"path": path.decode("ascii"), "headers": field_hex(event.headers), "body": ""}
        self.stats["productionRequests"].append(record)
        self.production_bodies[event.stream_id] = record
        if path == b"/production/replace-request":
            if event.stream_ended:
                self.finish_body(event.stream_id)
            return
        response = [(b":status", b"200")]
        if path == b"/production/headers":
            response += ORIGINAL_FIELDS
        if path == b"/production/replace-response":
            response += [(b"content-type", b"text/plain"), (b"content-md5", b"stale")]
        self.http.send_headers(event.stream_id, response)
        body = b"mapped" if os.environ.get("PRODUCTION_PEER_LABEL") == "mapped" else b"origin"
        if path == b"/production/replace-response":
            self.http.send_data(event.stream_id, b"original-body", end_stream=False)
            self.http.send_headers(event.stream_id, [(b"content-md5", b"stale"), (b"x-trailer", b"keep")], end_stream=True)
        else:
            self.http.send_data(event.stream_id, body, end_stream=True)
        if event.stream_ended:
            self.paths.pop(event.stream_id)
            self.production_bodies.pop(event.stream_id)

    def data(self, event):
        record = self.production_bodies.get(event.stream_id)
        if record is None:
            return super().data(event)
        assert len(record["body"]) + len(event.data) * 2 <= 2 * 1024 * 1024
        record["body"] += event.data.hex()
        self.stats["uploadBytes"] += len(event.data)
        if event.stream_ended:
            if self.paths[event.stream_id] == b"/production/replace-request":
                self.finish_body(event.stream_id)
            else:
                self.paths.pop(event.stream_id)
                self.production_bodies.pop(event.stream_id)

    def finish_body(self, stream):
        record = self.production_bodies.pop(stream)
        self.paths.pop(stream)
        self.http.send_headers(stream, [(b":status", b"200")])
        self.http.send_data(stream, bytes.fromhex(record["body"]), end_stream=True)


if __name__ == "__main__":
    from pathlib import Path
    parser = argparse.ArgumentParser()
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--flow-proof", action="store_true")
    asyncio.run(serve(parser.parse_args(), origin_type=ProductionOrigin))
