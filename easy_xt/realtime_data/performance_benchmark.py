import statistics
import time
import importlib
import importlib.util
from typing import Any

msgpack: Any = None
try:
    if importlib.util.find_spec("msgpack") is not None:
        msgpack = importlib.import_module("msgpack")
except Exception:
    msgpack = None


class EndToEndLatencyBenchmark:
    def __init__(self, data_source_ms: float = 20.0, push_ms: float = 8.0):
        self.data_source_ms = data_source_ms
        self.push_ms = push_ms

    def run_comprehensive_benchmark(self, duration_seconds: int = 30) -> dict[str, Any]:
        test_data = {
            "symbol": "000001",
            "price": 10.5,
            "volume": 10000,
            "timestamp": time.time()
        }
        results = {
            "json": self._benchmark_json(test_data, duration_seconds),
            "msgpack": self._benchmark_msgpack(test_data, duration_seconds) if msgpack else None
        }
        summary = {}
        for key, metric in results.items():
            if not metric:
                summary[key] = None
                continue
            summary[key] = {
                "serialize_avg_ms": metric["serialize_avg_ms"],
                "payload_avg_bytes": metric["payload_avg_bytes"],
                "end_to_end_ms": self.data_source_ms + metric["serialize_avg_ms"] + self.push_ms
            }
        report = {
            "input": {
                "data_source_ms": self.data_source_ms,
                "push_ms": self.push_ms,
                "duration_seconds": duration_seconds
            },
            "details": results,
            "summary": summary
        }
        print(report)
        return report

    def _benchmark_json(self, test_data: dict[str, Any], duration_seconds: int) -> dict[str, Any]:
        import json
        return self._run_loop(test_data, duration_seconds, lambda d: json.dumps(d, ensure_ascii=False).encode("utf-8"))

    def _benchmark_msgpack(self, test_data: dict[str, Any], duration_seconds: int) -> dict[str, Any]:
        return self._run_loop(test_data, duration_seconds, lambda d: msgpack.packb(d, use_bin_type=True))

    def _run_loop(self, test_data: dict[str, Any], duration_seconds: int, serializer) -> dict[str, Any]:
        end_time = time.time() + duration_seconds
        serialize_times: list[float] = []
        sizes: list[int] = []
        while time.time() < end_time:
            start = time.perf_counter()
            payload = serializer(test_data)
            serialize_times.append((time.perf_counter() - start) * 1000)
            sizes.append(len(payload))
        return {
            "iterations": len(serialize_times),
            "serialize_avg_ms": statistics.mean(serialize_times) if serialize_times else 0.0,
            "serialize_p95_ms": statistics.quantiles(serialize_times, n=20)[18] if len(serialize_times) >= 20 else 0.0,
            "payload_avg_bytes": statistics.mean(sizes) if sizes else 0,
            "payload_p95_bytes": statistics.quantiles(sizes, n=20)[18] if len(sizes) >= 20 else 0
        }
