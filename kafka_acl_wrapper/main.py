from . import cli
import signal
import sys

def handler(signum, frame):
    print("Timeout reached, exiting...")
    sys.exit(1)

def kafka_acl_wrapper():
    # Set the signal handler and a 60-second alarm
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(60)

    try:
        cli.app()
    finally:
        # Disable the alarm
        signal.alarm(0)

if __name__ == "__main__":
    kafka_acl_wrapper()
