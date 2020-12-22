import binascii
import hashlib
import logging
import os
import sqlite3
import sys

import boto3
from botocore.exceptions import ClientError

from glacier_rsync.file_cache import FileCache


class BackupUtil:
	def __init__(self, args):
		self.continue_running = True

		self.src = args.src
		self.compress = args.compress
		self.desc = args.desc
		self.part_size = args.part_size

		self.vault = args.vault
		self.region = args.region

		self.glacier = boto3.client("glacier", region_name=self.region)

		self.db_file = args.db
		try:
			self.conn = sqlite3.connect(self.db_file, isolation_level=None)
			self.conn.execute('pragma journal_mode=wal')
			logging.info("connected to glacier rsync db")
		except sqlite3.Error as e:
			logging.error(f"Cannot create glacier rsync db: {str(e)}")
			raise ValueError(f"Cannot create glacier rsync db: {str(e)}")

		cur = self.conn.cursor()
		try:
			cur.execute(
				"create table if not exists sync_history (id integer primary key, path text, file_size integer, "
				"mtime float, archive_id text, location text, checksum text, compression text, timestamp text);")
			self.conn.commit()
		except sqlite3.OperationalError as e:
			logging.error(f"DB error. Cannot mark the file as backed up: {str(e)})")
			sys.exit(2)
		finally:
			cur.close()
		logging.debug("init is done")

	def stop(self):
		"""
		Set break condition for file list loop
		Utility will exit as soon as current upload is complete.
		"""
		self.continue_running = False

	def close(self):
		"""
		Close database connection
		"""
		self.conn.commit()
		self.conn.close()

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

		logging.info(f"number of files to backup: {len(file_list)}")
		for file_index, file in enumerate(file_list):
			if not self.continue_running:
				logging.info(f"Exiting early...")
				break

			is_backed_up, file_size, mtime = self._check_if_backed_up(file)
			if not is_backed_up:  # True if already backed up
				logging.info(f"{file_index + 1}/{len(file_list)} - {file} will be backed up")

				part_size = self.decide_part_size(file_size)  # decide part size for each file
				logging.debug(f"part size is {part_size}")

				file_object, compressed_file_object = self._compress(file)  # compress the file if specified

				desc = f'grsync|{file}|{file_size}|{mtime}|{self.desc}'
				archive = self._backup(compressed_file_object, desc, part_size)

				if archive is not None:
					logging.info(f"{file} is backed up successfully")
				else:
					logging.error(f"Error backing up {file}")

				file_object.close()
				self._mark_backed_up(file, archive)
			else:
				logging.info(f"{file_index + 1}/{len(file_list)} - {file} is already backed up, skipping...")

		logging.info("All files are processed.")
		self.close()

	def _check_if_backed_up(self, path):
		"""
		Check if file is already backed up
		:param path: full file path
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
		return len(rows) > 0, file_size, mtime

	def _compress(self, file):
		"""
		Compress given file with given algorithm
		:param file: input file path
		:return: compressed file path. If no compression is selected, the same file path
		"""

		file_object = open(file, 'rb')
		compression = False

		if self.compress:
			try:
				import zstandard as zstd
			except ImportError:
				msg = "cannot import zstd. Please install `zstandard' package!"
				logging.error(msg)
				raise ValueError(msg)
			compression = True

		return file_object, FileCache(file_object, compression=compression)

	def calculate_tree_hash(self, part, part_size):
		"""
		Calculate hash of single part
		:param part: data chunk
		:param part_size: size of the chunk
		:return: calculated hash
		"""
		checksums = []
		upper_bound = min(len(part), part_size)
		step = 1024 * 1024  # 1 MB
		for chunk_pos in range(0, upper_bound, step):
			chunk = part[chunk_pos: chunk_pos + step]
			checksums.append(hashlib.sha256(chunk).hexdigest())
			del chunk
		return self.calculate_total_tree_hash(checksums)

	@staticmethod
	def calculate_total_tree_hash(checksums):
		"""
		Calculate hash of a list
		:param checksums: list(checksum) -> a list of checksum
		:return: total calculated hash
		"""
		tree = checksums[:]
		while len(tree) > 1:
			parent = []
			for i in range(0, len(tree), 2):
				if i < len(tree) - 1:
					part1 = binascii.unhexlify(tree[i])
					part2 = binascii.unhexlify(tree[i + 1])
					parent.append(hashlib.sha256(part1 + part2).hexdigest())
				else:
					parent.append(tree[i])
			tree = parent
		return tree[0]

	def _backup(self, src_file_object, description, part_size):
		"""
		Send the file to glacier
		:param src_file_object: FileCache object
		:param description: Archive description including grsync meta
		:param part_size: Part size for multipart upload
		:return: archive information
		"""
		if src_file_object is None:  # only happens if unsupported compression algorithm
			return None
		try:
			response = self.glacier.initiate_multipart_upload(
				vaultName=self.vault,
				partSize=str(part_size),
				archiveDescription=description
			)
			upload_id = response['uploadId']

			byte_pos = 0
			list_of_checksums = []
			while True:
				chunk = src_file_object.read(part_size)
				if chunk is None:
					break
				range_header = "bytes {}-{}/*".format(
					byte_pos, byte_pos + len(chunk) - 1
				)
				byte_pos += len(chunk)
				response = self.glacier.upload_multipart_part(
					vaultName=self.vault,
					uploadId=upload_id,
					range=range_header,
					body=chunk,
				)
				checksum = response["checksum"]
				list_of_checksums.append(checksum)

			total_tree_hash = self.calculate_total_tree_hash(list_of_checksums)
			archive = self.glacier.complete_multipart_upload(
				vaultName=self.vault,
				uploadId=upload_id,
				archiveSize=str(byte_pos),
				checksum=total_tree_hash,
			)
		except ClientError as e:
			logging.error(e)
			return None

		# Return dictionary of archive information
		return archive

	def _mark_backed_up(self, path, archive):
		"""
		Mark the given file as archived in db with associated information
		:param path: absolute path of the file
		:param archive: glacier archive information
		"""
		if archive is None:
			logging.error(f"{path} cannot be backed up")
			return
		archive_id = archive['archiveId']
		location = archive['location']
		checksum = archive['checksum']
		timestamp = archive['ResponseMetadata']['HTTPHeaders']['date']
		compression = "plain"
		if self.compress:
			compression = "zstd"

		file_size, mtime = self.__get_stats(path)
		cur = self.conn.cursor()
		try:
			cur.execute(
				f"insert into sync_history "
				f"(path, file_size, mtime, archive_id, location, checksum, compression, timestamp) "
				f"values ('{path}', {file_size}, {mtime}, '{archive_id}', '{location}', "
				f"'{checksum}', '{compression}', '{timestamp}')"
			)
			self.conn.commit()
		except sqlite3.OperationalError as e:
			logging.error(f"DB error. Cannot mark the file as backed up: {str(e)})")
			sys.exit(1)  # cannot continue if cannot mark
		finally:
			cur.close()

	@staticmethod
	def __get_stats(path):
		"""
		Get the stats of given file
		:param path: absolute path of the file
		:return: tuple(file size, modified time)
		"""
		return os.path.getsize(path), os.path.getmtime(path)

	def decide_part_size(self, file_size):
		"""
		Decide Glacier part size
		Number of parts for multipart upload should be smaller than 10000
		:param file_size: size of file to be uploaded
		:return: part size for upload
		"""
		part_size = self.part_size
		while file_size / part_size > 10000:
			part_size *= 2
		return part_size
