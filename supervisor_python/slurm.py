import datetime
import json
import subprocess
from typing import Optional, List


def slurm_squeue(user: str, run_id: Optional[str] = None) -> List[dict]:
    """Gets slurm jobs in the queue for a particular user

    :param user: The user ID to filter the jobs on.
    :param run_id: The run id. Will filter on this for job names.
    :return: List of dictionaries. The dicts are slurm outputs from squeue.
    """

    command = f"squeue -u {user} --json"
    proc = subprocess.run(command, shell=True, capture_output=True)
    if proc.returncode != 0:
        stderr = proc.stderr.decode()
        raise RuntimeError(f"Error occurred running squeue: {stderr}")

    jobs = [j for j in json.loads(proc.stdout.decode())["jobs"]]

    # Filter on the run id here and only select the "jobs" portion of the return
    if run_id:
        jobs = [j for j in jobs if j["name"].startswith(run_id)]

    # squeue with --json will list non-queued jobs for a few minutes after completion so we need to filter
    # NOTE - running with `squeue --states` has no effect when using `--json`
    jobs = [j for j in jobs if j["job_state"][0] in ["PENDING", "RUNNING"]]

    return jobs


def slurm_sacct(
    start_date: Optional[datetime.datetime] = None,
    run_id: Optional[str] = None,
    end_date: Optional[datetime.datetime] = None,
    state: Optional[List] = None,
) -> List[dict]:
    """Gets slurm jobs for a particular run_id starting after a start date.

    :param run_id: The run id. Will filter on this for job names.
    :param start_date: Jobs that start after this datetime are considered
    :param end_date: Jobs that end before this time. If unsupplied, will take the current time.
    :param state: Jobs with any of these states.
    :return: List of dictionaries. The dicts are slurm outputs.

    Valid states are: running (r), completed (cd), failed (f), timeout (to), resizing (rs), deadline (dl) and node_fail (nf).
    """
    if not end_date:
        end_date = datetime.datetime.now()
    if not start_date:
        start_date = datetime.datetime.now().date()

    start_str = start_date.strftime("%Y-%m-%d%H:%M:%S")
    end_str = end_date.strftime("%Y-%m-%d%H:%M:%S")
    command = f"sacct -S {start_str} -E {end_str} -X  --json"
    proc = subprocess.run(command, shell=True, capture_output=True)
    if proc.returncode != 0:
        stderr = proc.stderr.decode()
        raise RuntimeError(f"Error occurred running sacct: {stderr}")

    jobs = [j for j in json.loads(proc.stdout.decode())["jobs"]]

    # Filter on the run id here and only select the "jobs" portion of the return
    if run_id:
        jobs = [j for j in jobs if j["name"].startswith(run_id)]

    # Further filter by State if it's supplied. We filter manually rather than use sacct because sacct doesn't work
    if state:
        jobs = [j for j in jobs if j["state"]["current"][0] in state]

    return jobs


def filter_latest_slurm_jobs(jobs: List[dict]) -> List[dict]:
    """Gets only the most recent of each job version from a list of jobs

    :param jobs: The slurm Jobs
    :return: The most recent version of each of the supplied jobs
    """
    recent_jobs = {}
    for job in jobs:
        name = job["name"]
        end_time = job["time"]["end"]
        if name not in recent_jobs or end_time > int(recent_jobs[name]["time"]["end"]):
            recent_jobs[name] = job

    return list(recent_jobs.values())
