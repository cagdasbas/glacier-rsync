# !/usr/bin/env python

import logging
import signal
import sys

from glacier_rsync.argparser import ArgParser
from glacier_rsync.backup_util import BackupUtil

FORCE_STOP_LIMIT = 3
global stop_request_count


def main():
	args = ArgParser().get_args()
	logging.getLogger(__name__)
	logging.basicConfig(
		format="%(asctime)s - %(module)s.%(funcName)s:%(lineno)d - %(levelname)s - %(message)s",
		level=getattr(logging, args.log_level, None))

	global stop_request_count
	stop_request_count = 0
	backup_util = BackupUtil(args)

	def signal_handler(sig, frame):
		global stop_request_count
		stop_request_count += 1
		if stop_request_count < FORCE_STOP_LIMIT:
			logging.info(f"Stop is requested, grsync will exit when current upload is complete.")
			logging.info(f"Press ctrl+c {FORCE_STOP_LIMIT} times for force exit.")
			backup_util.stop()
		else:
			logging.info(f"Force stop is requested. Exiting...")
			backup_util.close()
			sys.exit(0)

	signal.signal(signal.SIGINT, signal_handler)
	signal.signal(signal.SIGTERM, signal_handler)

	backup_util.backup()


if __name__ == "__main__":
	main()
