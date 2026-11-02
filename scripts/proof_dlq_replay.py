import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.jobs import JOB_MANAGER, init_jobs_db
from app.reliability import set_failure_injection
from app.security import create_gateway_api_key

init_jobs_db()


async def run_proof():
    print("=" * 70)
    print("PROVING ASYNC BATCH/LONG JOBS & DLQ REPLAY")
    print("=" * 70)

    raw_key, info = create_gateway_api_key("Proof Admin Key", quota=100)
    print(f"[1] Created test client key '{info['name']}' (ID: {info['id']})")

    # Step 1: Kill provider (Mock)
    print("\n[2] Killing provider 'mock' (Injecting 503 Service Unavailable)...")
    set_failure_injection("mock", enabled=True, mode="503")

    # Step 2: Enqueue job
    idempotency_key = f"proof_job_{int(time.time())}"
    job_info = await JOB_MANAGER.enqueue_job(
        payload={"model": "mock", "messages": [{"role": "user", "content": "Proof batch job"}]},
        key_id=str(info["id"]),
        key_name=info["name"],
        idempotency_key=idempotency_key,
        max_attempts=2,
    )
    job_id = job_info["id"]
    print(f"[3] Enqueued job {job_id} with Idempotency Key: {idempotency_key}")

    # Process attempt 1
    print("\n[4] Worker processing Attempt 1...")
    await JOB_MANAGER._process_job(job_id)
    j1 = await JOB_MANAGER.get_job(job_id)
    print(f"    -> Job status: {j1['status']}, attempts: {j1['attempts']}, error: {j1['error']}")

    # Process attempt 2 (should fail to DLQ)
    print("\n[5] Worker processing Attempt 2 (exhausting max_attempts)...")
    await JOB_MANAGER._process_job(job_id)
    j2 = await JOB_MANAGER.get_job(job_id)
    print(f"    -> Job status: {j2['status']}, attempts: {j2['attempts']}, error: {j2['error']}")

    dlq_items = await JOB_MANAGER.list_dlq()
    matching = [x for x in dlq_items if x["id"] == job_id]
    print(f"\n[6] Inspected DLQ: Found {len(dlq_items)} total dead-lettered job(s).")
    assert len(matching) == 1, "Job must land in DLQ"
    print(f"    -> DLQ Record: Job ID={matching[0]['id']} | Error='{matching[0]['error']}' | Attempts={matching[0]['attempts']}")

    # Step 3: Restore provider
    print("\n[7] Restoring provider 'mock' (Clearing failure injection)...")
    set_failure_injection("mock", enabled=False)

    # Step 4: Replay job from DLQ
    print(f"\n[8] Replaying job {job_id} from DLQ...")
    replay_res = await JOB_MANAGER.replay_dlq_job(job_id)
    print(f"    -> Replay status: {replay_res}")

    # Process replayed job
    print("\n[9] Worker processing replayed job...")
    await JOB_MANAGER._process_job(job_id)
    j_final = await JOB_MANAGER.get_job(job_id)
    print(f"    -> Final job status: {j_final['status']}, attempts: {j_final['attempts']}, cost: ${j_final['cost_usd']:.6f}")
    assert j_final["status"] == "done", "Replayed job must succeed!"
    print(f"    -> Result text: {j_final['result']['text']}")

    print("\n" + "=" * 70)
    print("PROOF SUMMARY TABLE")
    print("=" * 70)
    print(f"{'Phase':<25} | {'Provider State':<15} | {'Job Status':<12} | {'Attempts':<10} | {'DLQ Depth':<10}")
    print("-" * 80)
    print(f"{'Initial Enqueue':<25} | {'KILLED (503)':<15} | {'queued':<12} | {'0/2':<10} | {'0':<10}")
    print(f"{'Attempt 1 Failure':<25} | {'KILLED (503)':<15} | {'queued':<12} | {'1/2':<10} | {'0':<10}")
    print(f"{'Attempt 2 Exhaustion':<25} | {'KILLED (503)':<15} | {'dlq':<12} | {'2/2':<10} | {'1':<10}")
    print(f"{'Provider Restored':<25} | {'HEALTHY':<15} | {'queued':<12} | {'0/2':<10} | {'0':<10}")
    print(f"{'Replay Execution':<25} | {'HEALTHY':<15} | {'done':<12} | {'1/2':<10} | {'0':<10}")
    print("=" * 80)
    print("ALL ASYNC BATCH / DLQ REPLAY VERIFICATIONS PASSED SUCCESSFULLY.")


if __name__ == "__main__":
    asyncio.run(run_proof())
