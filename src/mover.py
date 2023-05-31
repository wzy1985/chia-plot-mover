import argparse
import copy
import json
import logging
import math
import multiprocessing
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path


class StopEventException(Exception):
	pass


class Mover:

	def __init__(self):
		# Logger
		self.logger = None

		# Runtime
		self.lock_file = None
		self.expiration_time = None
		self.src = []
		self.dst = []
		self.src_list = None
		self.scanner = None
		self.workers = []
		self.stop_event = threading.Event()
		self.mutex = multiprocessing.Lock()

		# 在主线程中设置信号处理程序
		signal.signal(signal.SIGINT, self.signal_handler)
		signal.signal(signal.SIGTERM, self.signal_handler)
		return

	def __del__(self):
		self.lockfile_delete()
		return

	# 开始运行
	def run(self):
		# Lockfile
		self.lockfile_create()

		# 检查是否已经加载配置
		if not self.lock_file:
			self.log("Configuration not loaded, exiting...", logging.ERROR)
			return

		# 创建扫描线程
		self.scanner = threading.Thread(target=self.scanner_thread)
		self.scanner.start()
		self.log("Scanner thread started.")

		# 创建工作线程
		for dst in self.dst:
			worker_thread = threading.Thread(target=self.worker_thread, args=(dst,))
			worker_thread.start()
			self.workers.append(worker_thread)
		self.log("Worker threads started.")

		# 等待结束
		for worker in self.workers:
			worker.join()
		self.log("Worker threads finished.")

		self.scanner.join()
		self.log("Scanner thread finished.")

		self.log("Exiting main thread.")
		return

	# 设置日志
	def set_logger(self, logger):
		self.logger = logger
		return self

	# 设置配置
	def set_config(self, config_file):
		def validate_src_dst(items, name) -> list:
			if len(items) == 0:
				raise Exception(f"{name} must be configured with at least one item each.")
			result = []
			for k, value in enumerate(items):
				item_name = value.get('name')
				if not item_name or not item_name.strip():
					raise Exception(f'{name}[{k}].name must be filled in.')
				item_dir = Path(value.get('dir', '')).resolve()
				if not item_dir.is_dir():
					raise Exception(f'{name}[{k}].dir directory {item_dir} does not exist or is not a directory!')
				result.append({
					'name': item_name,
					'dir': item_dir,
				})
			return result

		try:
			config_path = Path(config_file)
			with config_path.open() as f:
				config = json.load(f)
			self.log("Config loaded from {}.".format(config_path.resolve()), logging.INFO)
			# Validate and update configuration
			self.lock_file = Path(config.get('main').get('lock_file', '/var/run/mover-manager.lock'))
			self.expiration_time = int(time.mktime(
				time.strptime(config.get('main').get('expiration_time', '0'), '%Y-%m-%d %H:%M:%S')
			) if config.get('main').get('expiration_time') else 0)
			self.src = validate_src_dst(config.get('source', []), 'source')
			self.dst = validate_src_dst(config.get('destination', []), 'destination')
		except FileNotFoundError:
			self.log("Config file {} not found.".format(config_path.resolve()), logging.ERROR)
			exit(1)
		except json.JSONDecodeError:
			self.log("Config file {} is not a valid JSON file.".format(config_path.resolve()), logging.ERROR)
			exit(1)
		except Exception as e:
			self.log("Config file {} is not valid: {}".format(config_path.resolve(), e), logging.ERROR)
			exit(1)
		return self

	# 扫描线程
	def scanner_thread(self):

		# self.src_list = [
		# 	[Path('/home/plotter/plot/plot-1/plot-1.plot'), 108, 'ext4'],
		# 	[Path('/home/plotter/plot/plot-1/plot-2.plot'), 108, 'ext4'],
		# 	[Path('/Users/zhaoyuan/Downloads/chia/plot/in2/028.plot'), 108, 'ext4'],
		# 	[Path('/Users/zhaoyuan/Downloads/chia/plot/in2/888.plot'), 108, 'ext4'],
		# ]

		while True:
			try:
				with self.mutex:
					src_list = copy.deepcopy(self.src_list) if self.src_list is not None else []
				# 扫描新增文件
				append_list = []
				for src in self.src:
					for v in sorted(src['dir'].glob('*.plot')):
						if v.is_file() and not any(sublist['file'] == v for sublist in src_list):
							append_list.append({
								'file': v,
								'size': v.stat().st_size,
								'mtime': v.stat().st_mtime,
								'fs': self.get_filesystem_by_path(v),
								'status': 'idle'
							})
						self.check_stop_event()
				# 扫描待删除文件
				remove_list = []
				for src in src_list:
					if not src['file'].is_file():
						remove_list.append(src)
					self.check_stop_event()
				# 更新源列表
				with self.mutex:
					if self.src_list is None:
						self.src_list = []
					for src in append_list:
						if not any(sublist['file'] == src['file'] for sublist in self.src_list):
							self.log("Found new plot file: {}".format(src['file'].resolve()), logging.INFO,
									 method='Scanner')
							self.src_list.append(src)
						self.check_stop_event()
					for src in remove_list:
						if any(sublist['file'] == src['file'] for sublist in self.src_list):
							self.log("Remove invalid plot file: {}".format(src['file'].resolve()), logging.INFO,
									 method='Scanner')
							self.src_list.remove(src)
						self.check_stop_event()
				# Sleep
				for sec in range(10):
					time.sleep(1)
					self.check_stop_event()
			except StopEventException:
				self.log("Stop event received, exiting scanner thread.", logging.INFO, method='Scanner')
				return
			except Exception as e:
				self.log("Exception in scanner thread: {}".format(e), logging.ERROR, method='Scanner')
				self.log(traceback.format_exc(), logging.DEBUG, method='Scanner')

	# 工作线程
	def worker_thread(self, dst):
		try:
			while True:
				time.sleep(1)
				self.check_stop_event()
				# 生成一个待传任务
				pending = None
				src_item = None
				if self.src_list is None:
					continue
				pending_list = [item for item in dst['dir'].glob('.*.plot.moving') if item.is_file()]
				while True:
					if len(pending_list) > 0:
						pending = pending_list.pop(0)
						with self.mutex:
							for (k, item) in enumerate(self.src_list):
								if item['file'].name == pending.name[1:-7]:
									src_item = item
									break
						if src_item:
							break
						else:
							pending.unlink()
							pending = None
					else:
						break
				if not pending:
					# 获取一个相对空闲的源
					'''
					fs_statistics = {
						'fs1': [传输中数量, [待传对象,待传对象,...]],
						'fs2': [传输中数量, [待传对象,待传对象,...]],
						'fs3': [传输中数量, [待传对象,待传对象,...]],
						...
					}
					'''
					fs_statistics = {}
					with self.mutex:
						for item in self.src_list:
							fs = item['fs']
							statistics = fs_statistics.get(fs, [0, []])
							if item['status'] == 'idle':
								statistics[1].append(item)
							else:
								statistics[0] += 1
							fs_statistics[fs] = statistics
					if len(fs_statistics) == 0:
						continue
					# 过滤掉空列表项
					fs_statistics = {key: value for key, value in fs_statistics.items() if value[1]}
					if len(fs_statistics) == 0:
						continue
					# 找到传输中数量最小的项
					min_item = min(fs_statistics.items(), key=lambda item: item[1][0])
					# 获取列表的头部第一个值
					src_item = min_item[1][1][0]
					print(src_item)
					pending = dst['dir'] / ('.' + src_item['file'].name + '.moving')
				if not pending:
					continue
				# 准备磁盘空间
				free_space = shutil.disk_usage(dst['dir']).free
				if pending.is_file():
					free_space += pending.stat().st_size
				if free_space < src_item['size']:
					for expired_file in (item for item in dst['dir'].glob('*.plot') if
										 item.is_file() and item.stat().st_mtime < self.expiration_time):
						self.log("Removing expired file: {}, mtime: {}".format(
							expired_file.resolve(),
							time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expired_file.stat().st_mtime))
						), logging.INFO)
						free_space += expired_file.stat().st_size
						expired_file.unlink()
						if free_space >= src_item['size']:
							break
				if free_space < src_item['size']:
					# 如果磁盘已满并且没有可清理的文件，则清理掉正在传输的文件
					if pending.suffix == '.plot.moving':
						pending.unlink()
					continue
				# 开始传输
				with self.mutex:
					try:
						src_item = next(item for item in self.src_list if item['file'].name == pending.name[1:-7])
						src_item['status'] = 'moving'
						src_file = src_item['file']
					except StopIteration:
						continue
				self.file_move(src_file, pending)
				pending.rename(pending.parent / (pending.name[1:-7]))
				with self.mutex:
					self.src_list.remove(src_item)
		except StopEventException:
			self.log("Stop event received, exiting worker thread.", logging.INFO, method='Worker')
			return
		except Exception as e:
			self.log("Exception in worker thread: {}".format(e), logging.ERROR, method='Worker')
			self.log(traceback.format_exc(), logging.DEBUG, method='Worker')

	def file_move(self, src, dst, buffer_size=1024 ** 2):
		try:
			if dst.is_file():
				# 文件续传
				self.log("Resume copying file: {} -> {}/".format(src.resolve(), dst.parent), method='Worker')
			else:
				self.log("Start copying file: {} -> {}/".format(src.resolve(), dst.parent), method='Worker')
			time_begin = datetime.now()
			src_stat = os.stat(src.resolve())
			with open(src.resolve(), 'rb') as src_file, open(dst.resolve(), 'ab') as dst_file:
				src_file.seek(dst_file.tell())
				while True:
					buf = src_file.read(buffer_size)
					if buf:
						dst_file.write(buf)
					else:
						break
					self.check_stop_event()
				# 复制文件属性和权限
				shutil.copystat(src.resolve(), dst.resolve())
				# 设置目标文件所有者和所属组
				# os.chown(dst.resolve(), src_stat.st_uid, src_stat.st_gid)
				# 删除源文件
				self.log("Remove source file: {}".format(src.resolve()), level=logging.DEBUG, method='Worker')
				src_path = src.resolve()
				src.unlink()
				time_complete = datetime.now()
				use_time = self.usetime_to_text(time_begin, time_complete)
				copy_speed = src_stat.st_size / 1024 ** 2 / (time_complete - time_begin).total_seconds()
				self.log("File copying completed: {} -> {}/ , time used: {} ({} Mb/s)".format(
					src_path, dst.parent, use_time, format(copy_speed, '0,.0f')
				), method='Worker')
		except StopEventException:
			self.log("File copying stopped: {} -> {}".format(src.resolve(), dst.resolve()), method='Worker')
		except Exception as e:
			self.log("File copying failed: {} -> {}".format(src.resolve(), dst.resolve()), level=logging.ERROR,
					 method='Worker')
			self.log("Error message: {}".format(e), level=logging.ERROR, method='Worker')
		return

	# 将时间差转换为文本
	def usetime_to_text(self, begin, end):
		total = end - begin
		days = total.days
		hours = math.floor(total.seconds / 3600)
		mins = math.floor((total.seconds % 3600) / 60)
		seconds = math.floor((total.seconds % 3600) % 60)
		tmsg = str(seconds) + 's'
		if days or hours or mins:
			tmsg = str(mins) + 'm' + tmsg
			if days or hours:
				tmsg = str(hours) + 'h' + tmsg
				if days:
					tmsg = str(days) + 'd' + tmsg
		return tmsg

	def get_filesystem_by_path(self, path):
		sys_info = os.uname()
		if sys_info.sysname == 'Linux':
			cmd = ['findmnt', '-n', '-r', '-o', 'SOURCE', '-T', str(path)]
			result = subprocess.check_output(cmd)
			filesystem = result.decode().strip()
		elif sys_info.sysname == 'Darwin':  # macOS
			cmd = ['df', str(path)]
			result = subprocess.check_output(cmd)
			filesystem = result.decode().split('\n')[1].split()[0]
		else:
			raise Exception(f'Unsupported operating system: {sys_info.sysname}')
		return filesystem

	# 日志函数
	def log(self, message, level=logging.INFO, method='Main'):
		if method:
			message = "{} - {}".format(method, message)
		if self.logger:
			self.logger.log(level, message)
		else:
			# 显示毫秒
			print("{} - {} - {}".format(
				datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
				logging.getLevelName(level),
				message
			))

	# 信号处理函数
	def signal_handler(self, signum, frame):
		self.log("Signal received, signal: {}".format(signum), method='main')
		self.stop_event.set()

	# 检查是否收到停止信号
	def check_stop_event(self, is_raise=True):
		if self.stop_event.is_set():
			if is_raise:
				self.log("Stop event received, exiting...", method='main')
				raise StopEventException()
			else:
				return True
		return False

	# 创建进程锁文件
	def lockfile_create(self):
		try:
			self.log("Creating lock file: {}".format(self.lock_file.resolve()), method='main')
			with open(self.lock_file.resolve(), 'w') as f:
				f.write(str(os.getpid()))
		except Exception as e:
			self.log("{}\n{}".format(sys.exc_info(), traceback.format_exc()), method='main', level=logging.ERROR)
			exit(1)

	# 删除进程锁文件
	def lockfile_delete(self):
		try:
			self.log("Deleting lock file: {}".format(self.lock_file.resolve()), method='main')
			self.lock_file.unlink()
		except Exception as e:
			pass


if __name__ == "__main__":
	# 参数解析
	parser = argparse.ArgumentParser(description='Chia Plots Mover')
	parser.add_argument('-f', '--config', required=True, help='Config file')
	args = parser.parse_args()

	# Logger
	logger = logging.getLogger('chia-plot-mover')
	logger.setLevel(logging.INFO)
	# logger.setLevel(logging.DEBUG)
	# STDOUT
	stdout_handler = logging.StreamHandler(sys.stdout)
	stdout_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
	logger.addHandler(stdout_handler)
	# FILE
	# file = logging.FileHandler(self.config['main']['log_dir'] + '/chia-plot-mover.log', 'a+', encoding="utf-8")
	# file.setFormatter(formatter)
	# logger.addHandler(file)
	logger = logger
	logger.info("chia-plot-mover was startup, PID: " + str(os.getpid()))

	# Main
	mover = Mover()
	mover.set_config(args.config)
	mover.set_logger(logger)
	mover.run()
