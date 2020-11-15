import logging
import os
import shutil
import sqlite3
import sys
from contextlib import ExitStack

import boto3
from botocore.exceptions import ClientError


class BackupUtil:
	def __init__(self, args):
		self.src = args.src
		self.compress_algo = args.compress
		self.remove_compressed = args.remove_compressed
		self.desc = args.desc

		self.vault = args.vault
		self.region = args.region

		self.glacier = boto3.client("glacier", region_name=self.region)

		self.db_file = args.db
		try:
			self.conn = sqlite3.connect(self.db_file)
			logging.info("connected to glacier rsync db")
		except sqlite3.Error as e:
			logging.error(f"Cannot create glacier rsync db: {str(e)}")
			raise ValueError(f"Cannot create glacier rsync db: {str(e)}")

		cur = self.conn.cursor()
		try:
			cur.execute(
				"create table if not exists sync_history (id integer primary key, path text, file_size integer, mtime float, archive_id text, location text, checksum text, timestamp text);")
			self.conn.commit()
		except sqlite3.OperationalError as e:
			logging.error(f"DB error. Cannot mark the file as backed up: {str(e)})")
			sys.exit(2)
		finally:
			cur.close()
			self.__cleanup()
		logging.debug("init is done")

	def backup(self):
		"""
		Interface function to find files and apply logic
		"""
		file_list = []
		if os.path.isdir(self.src):  # if the source is a directory find all the files
			for root, dirs, files in os.walk(self.src):
				for file in files:
					file_list.append(os.path.abspath(os.path.join(root, file)))
		else:
			file_list.append(self.src)  # if the source is a file just process it

		logging.debug(f"number of files to backup: {len(file_list)}")
		for file in file_list:
			if not self._check_if_backed_up(file):  # True if already backed up
				logging.debug(f"{file} will be backed up")
				compressed_file = self._compress(file)  # compress the file if specified
				logging.debug(f"{file} is compressed as {compressed_file}")
				archive = self._backup(compressed_file)
				self._mark_backed_up(file, archive)
				self._remove_file(compressed_file)
			else:
				logging.debug(f"{file} is already backed up, skipping...")

	def _check_if_backed_up(self, path):
		"""
		Check if file is already backed up
		:param file: full file path
		:return: True if file is backed up, False if file is not backed up
		"""
		file_size, mtime = self.__get_stats(path)  # file size and mtime should match. if not it will be backed up again
		cur = self.conn.cursor()
		try:
			cur.execute(
				f"select * from sync_history where path='{path}' and file_size={file_size} and mtime={mtime}")
			rows = cur.fetchall()
		except sqlite3.OperationalError as e:
			logging.error(f"DB error. Cannot mark the file as backed up: {str(e)})")
			sys.exit(3)
		finally:
			cur.close()
			self.__cleanup()
		return len(rows) > 0

	def _compress(self, file):
		"""
		Compress given file with given algorithm
		:param file: input file path
		:return: compressed file path. If no compression is selected, the same file path
		"""
		if self.compress_algo is None:  # do nothing and let the util process the original file
			return file

		if self.compress_algo == "gzip":
			try:
				import gzip
			except ImportError:
				msg = "cannot import gzip. Please install required libraries!"
				logging.error(msg)
				raise ValueError(msg)

			compressed_file = f"{file}.gz"
			self.__compress_stream(open(file, "rb"), gzip.open(compressed_file, "wb"))

			return compressed_file
		elif self.compress_algo == "zstd":
			try:
				import zstandard as zstd
			except ImportError:
				msg = "cannot import zstd. Please install required libraries!"
				logging.error(msg)
				raise ValueError(msg)

			compressed_file = f"{file}.zstd"
			cctx = zstd.ZstdCompressor()
			self.__compress_stream(open(file, "rb"), cctx.stream_writer(open(compressed_file, "wb")))

			return compressed_file
		else:
			return None

	def __compress_stream(self, input, output):
		"""
		Wrapper for redirecting the input file to compressor output
		:param input: input stream
		:param output: output compressor stream
		"""
		with ExitStack() as stack:
			f_in = stack.enter_context(input)
			f_out = stack.enter_context(output)
			shutil.copyfileobj(f_in, f_out)

	def _backup(self, src_file):
		"""
		Send the file to glacier
		:param src_file: Absolute path of the file to be backed up
		:return: archive information
		"""
		if src_file is None:  # only happens if unsupported compression algorithm
			return None
		try:
			object_data = open(src_file, "rb")
		# possible FileNotFoundError/IOError exception
		except Exception as e:
			logging.error(e)
			return None
		try:
			archive = self.glacier.upload_archive(vaultName=self.vault, body=object_data)  # actual work
		except ClientError as e:
			logging.error(e)
			return None
		finally:
			object_data.close()

		# Return dictionary of archive information
		return archive

	def _remove_file(self, path):
		"""
		Delete the given file. File will not be deleted if compression is off because compressed_file == file
		:param path: absolute path of the file
		"""
		if self.compress_algo is None:
			return
		elif self.remove_compressed:
			os.remove(path)
			logging.info(f"{path} is removed")

	def _mark_backed_up(self, path, archive):
		"""
		Mark the given file as archived in db with associated information
		:param path: absolute path of the file
		:param archive_id: glacier archive id
		"""
		if archive is None:
			logging.error(f"{path} cannot be backed up")
			return
		archive_id = archive['archiveId']
		location = archive['location']
		checksum = archive['checksum']
		timestamp = archive['ResponseMetadata']['HTTPHeaders']['date']

		file_size, mtime = self.__get_stats(path)
		cur = self.conn.cursor()
		try:
			cur.execute(
				f"insert into sync_history (path, file_size, mtime, archive_id, location, checksum, timestamp) "
				f"values ('{path}', {file_size}, {mtime}, '{archive_id}', '{location}', '{checksum}', '{timestamp}')"
			)
			self.conn.commit()
		except sqlite3.OperationalError as e:
			logging.error(f"DB error. Cannot mark the file as backed up: {str(e)})")
			sys.exit(1)  # cannot continue if cannot mark
		finally:
			cur.close()
			self.__cleanup()

	def __get_stats(self, path):
		"""
		Get the stats of given file
		:param path: absolute path of the file
		:return: tuple(file size, modified time)
		"""
		return os.path.getsize(path), os.path.getmtime(path)

	def __cleanup(self):
		self.conn.close()
