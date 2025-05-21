import datetime
import subprocess
import argparse
import os
import time

from supervisor_python.slurm import slurm_sacct, filter_latest_slurm_jobs, slurm_squeue


def launch(executable: str, account: str, partition: str, work_dir: str, run_id: str) -> datetime.date:
    """Launches the pipeline

    :param executable: The path of the pipeline executable
    :param account: The account to execute the pipeline under
    :param patition: The parition to execute the pipeline under
    :param work_dir: The working directory for the pipeline
    :param run_id: The ID of the run. Used to identify it and its jobs.
    """
    os.makedirs(work_dir, exist_ok=True)

    command = f"{executable} --account {account} --partition {partition} --work_dir {work_dir} --run_id {run_id}"
    print(f"Running execute command: {command}")
    start = datetime.datetime.now()
    proc = subprocess.run(command, shell=True, capture_output=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Nonzero exit code starting pipeline: {proc.stderr.decode()}")

    # Check that the process has indeed started by getting the output of sacct
    while True:
        jobs = slurm_squeue(run_id=run_id, user=os.environ["USER"])
        if len(jobs) > 0:
            break

        if datetime.datetime.now() - start > datetime.timedelta(minutes=2):
            raise RuntimeError(
                "Expected nonzero job count after initial job submit but found zero. Something went wrong"
            )
        print(f"Waiting for jobs to appear in queue...")
        time.sleep(10)

    return start


def monitor(run_id: str, timeout: datetime.timedelta):
    """A continuous loop that stays awake as long as the pipeline is running.
    Breaks from the loop when no more pipeline jobs are still running or pending.

    :param run_id: The run ID of the pipeline
    :param start: The time that the pipeline began
    :param timeout: The time to monitor for before dying
    """
    start = datetime.datetime.now()
    while True:
        running_jobs = slurm_squeue(run_id=run_id, user=os.environ["USER"])

        if len(running_jobs) == 0:
            print("No running/pending jobs - Pipeline complete!")
            break

        print(f"There are {len(running_jobs)} jobs still pending/running...")

        if datetime.datetime.now() - start >= timeout:
            raise RuntimeError(f"Pipeline has been running for more than {str(timeout)}, quitting")

        time.sleep(30)


def check(run_id: str, start: datetime.datetime) -> None:
    """Evaluates the pipeline's completion

    :param run_id: The run ID of the pipeline
    :param start: The start time of the pipeline
    """
    jobs = filter_latest_slurm_jobs(slurm_sacct(run_id=run_id, start_date=start))
    non_success = [j for j in jobs if "COMPLETED" not in j["state"]["current"]]
    if non_success:
        print(f"Job failure(s) found:")
        for j in non_success:
            print(f"{j['name']} :: {j['state']['current']}")
        raise RuntimeError("Failure Occurred. Please Investigate")
    print("All jobs successful!")


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("-e", "--executable", required=True, help="The pipeline executable")
    parser.add_argument("--account", required=True, help="The account to run the pipeline with")
    parser.add_argument("--partition", required=True, help="The partition to run the pipeline with")
    parser.add_argument(
        "--dir", required=False, help="The working directory. If unsupplied, work in the current directory"
    )

    args = parser.parse_args()
    dir = args.dir or os.getcwd()
    run_id = datetime.datetime.now().strftime("%Y%m%d%H%M")

    # Launch the pipeline job
    start = launch(
        executable=args.executable, account=args.account, partition=args.partition, work_dir=dir, run_id=run_id
    )

    # Monitor the jobs
    monitor(run_id=run_id, timeout=datetime.timedelta(minutes=20))

    # Check if pass or fail
    check(run_id=run_id, start=start)

    print("Ending Supervisor")


if __name__ == "__main__":
    main()
