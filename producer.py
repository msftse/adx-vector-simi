#!/usr/bin/env python3
"""
Event Hub Log Producer — sends 250k logs/min to Azure Event Hub
Uses Azure AD service principal auth (SAS keys disabled by policy)
Event Hub → ADX data connection → VectorLogs table (queued ingestion)

Architecture: 4 threads, one per Event Hub partition, each sending independently.
"""

import json
import time
import random
import signal
import sys
import os
import threading
from datetime import datetime, timezone

from azure.identity import ClientSecretCredential
from azure.eventhub import EventHubProducerClient, EventData

# ─── Configuration ───────────────────────────────────────────
TENANT_ID = os.environ["AZURE_TENANT_ID"]
CLIENT_ID = os.environ["AZURE_CLIENT_ID"]
CLIENT_SECRET = os.environ["AZURE_CLIENT_SECRET"]

EH_NAMESPACE = os.environ.get(
    "EVENTHUB_NAMESPACE", "ehvectorlogs2.servicebus.windows.net"
)
EH_NAME = os.environ.get("EVENTHUB_NAME", "vectorlogs")

TARGET_EVENTS_PER_MIN = int(os.environ.get("TARGET_RATE", "250000"))
NUM_PARTITIONS = int(os.environ.get("NUM_PARTITIONS", "4"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "250"))  # events per batch per partition

# ─── Sample data pools (mimics Vector demo_logs) ────────────
HOSTS = [f"host-{i}.example.com" for i in range(20)]
SERVICES = [
    "web",
    "api",
    "auth",
    "payments",
    "search",
    "notifications",
    "analytics",
    "gateway",
]
METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"]
STATUS_CODES = [
    200,
    200,
    200,
    200,
    200,
    201,
    204,
    301,
    302,
    400,
    401,
    403,
    404,
    500,
    502,
    503,
]
PATHS = [
    "/api/v1/users",
    "/api/v1/orders",
    "/api/v1/products",
    "/api/v1/search",
    "/api/v1/auth/login",
    "/api/v1/auth/logout",
    "/api/v1/health",
    "/api/v2/analytics",
    "/api/v1/notifications",
    "/api/v1/payments",
    "/static/js/app.js",
    "/static/css/main.css",
    "/favicon.ico",
]
SEVERITIES = ["info", "info", "info", "info", "warn", "error"]

# ─── Globals ─────────────────────────────────────────────────
running = True
counter_lock = threading.Lock()
total_sent = 0


def signal_handler(sig, frame):
    global running
    print("\n[!] Shutting down gracefully...")
    running = False


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def generate_event():
    """Generate a single log event matching VectorLogs schema."""
    return {
        "Timestamp": datetime.now(timezone.utc).isoformat(),
        "Host": random.choice(HOSTS),
        "Service": random.choice(SERVICES),
        "Message": f"{random.choice(METHODS)} {random.choice(PATHS)}",
        "Severity": random.choice(SEVERITIES),
        "Method": random.choice(METHODS),
        "StatusCode": random.choice(STATUS_CODES),
        "BytesSent": random.randint(100, 50000),
        "DurationMs": round(random.uniform(0.5, 500.0), 2),
        "SourceType": "python-simulator",
        "VectorHostname": os.environ.get("HOSTNAME", "local-producer"),
    }


def partition_worker(partition_id, events_per_sec_per_partition, credential):
    """Worker thread: sends events to a specific partition."""
    global running, total_sent

    producer = EventHubProducerClient(
        fully_qualified_namespace=EH_NAMESPACE,
        eventhub_name=EH_NAME,
        credential=credential,
    )

    batches_per_sec = events_per_sec_per_partition / BATCH_SIZE
    sleep_between = 1.0 / batches_per_sec if batches_per_sec > 0 else 0.01
    local_sent = 0
    errors = 0

    print(
        f"  [P{partition_id}] Target: {events_per_sec_per_partition:.0f} events/sec, "
        f"{batches_per_sec:.1f} batches/sec, sleep: {sleep_between * 1000:.0f}ms"
    )

    try:
        with producer:
            # Warm up connection with a small batch
            warmup_batch = producer.create_batch(partition_id=str(partition_id))
            warmup_batch.add(EventData(json.dumps(generate_event())))
            producer.send_batch(warmup_batch)
            local_sent += 1
            print(f"  [P{partition_id}] Connection established (warmup OK)")

            while running:
                loop_start = time.time()

                try:
                    batch = producer.create_batch(partition_id=str(partition_id))
                    for _ in range(BATCH_SIZE):
                        event = EventData(json.dumps(generate_event()))
                        try:
                            batch.add(event)
                        except ValueError:
                            # Batch full, send and start new one
                            producer.send_batch(batch)
                            sent_count = batch.size_in_bytes  # approx
                            batch = producer.create_batch(
                                partition_id=str(partition_id)
                            )
                            batch.add(event)

                    producer.send_batch(batch)
                    local_sent += BATCH_SIZE

                    with counter_lock:
                        total_sent += BATCH_SIZE

                except Exception as e:
                    errors += 1
                    err_str = str(e)
                    if (
                        "throttled" in err_str.lower()
                        or "server-busy" in err_str.lower()
                    ):
                        # Throttled — back off as instructed
                        time.sleep(4)
                    else:
                        if errors <= 5:
                            print(f"  [P{partition_id}] Error #{errors}: {e}")
                        time.sleep(1)
                    if errors > 10000:
                        print(
                            f"  [P{partition_id}] Too many errors ({errors}), stopping"
                        )
                        break
                    continue

                # Pace control
                elapsed = time.time() - loop_start
                sleep_time = sleep_between - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

    except Exception as e:
        print(f"  [P{partition_id}] Fatal error: {e}")

    print(f"  [P{partition_id}] Done. Sent: {local_sent:,}, Errors: {errors}")


def main():
    global running, total_sent

    print("=" * 60)
    print("  Event Hub Log Producer (Multi-Partition)")
    print("=" * 60)
    print(f"  Namespace:    {EH_NAMESPACE}")
    print(f"  Event Hub:    {EH_NAME}")
    print(f"  Target:       {TARGET_EVENTS_PER_MIN:,} events/min")
    print(f"  Partitions:   {NUM_PARTITIONS}")
    print(f"  Batch size:   {BATCH_SIZE} per partition")
    print(f"  Auth:         Azure AD service principal")
    print("=" * 60)

    events_per_sec = TARGET_EVENTS_PER_MIN / 60.0
    events_per_sec_per_partition = events_per_sec / NUM_PARTITIONS

    print(f"\n  Total events/sec:     {events_per_sec:,.0f}")
    print(f"  Per partition/sec:    {events_per_sec_per_partition:,.0f}")

    # Create shared credential
    print("\n[1/3] Authenticating with Azure AD...")
    credential = ClientSecretCredential(
        tenant_id=TENANT_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
    )

    # Launch partition workers
    print("[2/3] Launching partition workers...\n")
    threads = []
    for pid in range(NUM_PARTITIONS):
        t = threading.Thread(
            target=partition_worker,
            args=(pid, events_per_sec_per_partition, credential),
            daemon=True,
        )
        t.start()
        threads.append(t)
        time.sleep(0.5)  # stagger startup

    # Monitor loop
    print(f"\n[3/3] Monitoring... (Ctrl+C to stop)\n")
    start_time = time.time()
    last_report = start_time
    last_total = 0

    try:
        while running:
            time.sleep(10)
            now = time.time()
            current_total = total_sent
            elapsed = now - last_report
            period_sent = current_total - last_total
            rate_per_min = (period_sent / elapsed) * 60 if elapsed > 0 else 0
            total_elapsed = now - start_time
            overall_rate = (
                (current_total / total_elapsed) * 60 if total_elapsed > 0 else 0
            )

            print(
                f"[{datetime.now().strftime('%H:%M:%S')}] "
                f"Sent: {current_total:>10,} total | "
                f"Rate: {rate_per_min:>10,.0f}/min (last 10s) | "
                f"Avg: {overall_rate:>10,.0f}/min"
            )

            last_report = now
            last_total = current_total
    except KeyboardInterrupt:
        running = False

    print("\n[!] Stopping workers...")
    running = False
    for t in threads:
        t.join(timeout=10)

    total_elapsed = time.time() - start_time
    overall_rate = (total_sent / total_elapsed) * 60 if total_elapsed > 0 else 0
    print(f"\n{'=' * 60}")
    print(f"  Total sent:     {total_sent:,}")
    print(f"  Total time:     {total_elapsed:.1f}s")
    print(f"  Average rate:   {overall_rate:,.0f}/min")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
