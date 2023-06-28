import argparse
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
import atexit


class StopEventException(Exception):
    pass


class Mover:

    def __init__(self):
        # Config
        self.conf_lock_file = None
        self.conf_expiration_time = None
        self.conf_log_level = None
        self.conf_log_file = None
        self.conf_src = []
        self.conf_dst = []

        # Runtime
        self.logger = None
        self.log_message_queue = []
        self.mutex = multiprocessing.Lock()
        self.workers = {}
        self.stop_event = threading.Event()
        self.filesystem_cache = {}

        # 在主线程中设置信号处理程序
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        atexit.register(self._cleanup)

        self._log("Chia-Plot-Mover was startup, PID: " + str(os.getpid()))
        return

    def _cleanup(self):
        self._lockfile_delete()
        self._log("Chia-Plot-Mover was shutdown, PID: " + str(os.getpid()))
        while True:
            try:
                data = self.log_message_queue.pop(0)
                print("{} - {} - {}".format(
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    logging.getLevelName(data[0]),
                    data[1]
                ))
            except:
                break
        return

    # 开始运行
    def run(self):
        # Lockfile
        self._lockfile_create()

        # 检查是否已经加载配置
        if not self.conf_lock_file:
            self._log("Configuration not loaded, exiting...", logging.ERROR)
            return

        # 创建工作线程
        for dst in self.conf_dst:
            self.workers[dst['dir'].resolve()] = {
                'thread': threading.Thread(target=self._worker_thread, args=(dst,)),
                'lock': threading.Lock(),
                'conf_dst': dst,
                'device': self._get_filesystem_by_path(dst['dir']),
                'estimated_available_capacity': 0,
                'status': 'init',  # init, idle, busy
                'task': None,
            }
            self.workers[dst['dir'].resolve()]['thread'].start()

        # 创建扫描线程
        scanner = threading.Thread(target=self._scanner_thread)
        scanner.start()

        # 等待结束
        scanner.join()
        for worker in self.workers.values():
            worker['thread'].join()

        self._log("Exiting main thread.")
        return

    # 设置配置
    def set_config(self, config_file):
        def validate_src_dst(items, name):
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
            self._log("Config loaded: {}".format(config_path.resolve()), logging.INFO)
            # Validate and update configuration
            self.conf_lock_file = Path(config.get('main').get('lock_file', '/var/run/mover.lock'))
            self.conf_expiration_time = int(time.mktime(
                time.strptime(config.get('main').get('expiration_time', '0'), '%Y-%m-%d %H:%M:%S')
            ) if config.get('main').get('expiration_time') else 0)
            self.conf_log_level = config.get('main').get('log_level', 'INFO').upper()
            conf_log_file = config.get('main').get('log_file')
            if conf_log_file:
                self.conf_log_file = Path(conf_log_file)
            self._init_logger()
            self.conf_src = validate_src_dst(config.get('source', []), 'source')
            self.conf_dst = validate_src_dst(config.get('destination', []), 'destination')
        except FileNotFoundError:
            self._log("Config file {} not found.".format(config_path.resolve()), logging.ERROR)
            exit(1)
        except json.JSONDecodeError:
            self._log("Config file {} is not a valid JSON file.".format(config_path.resolve()), logging.ERROR)
            exit(1)
        except Exception as e:
            self._log("Config file {} is not valid: {}".format(config_path.resolve(), e), logging.ERROR)
            exit(1)
        return self

    # 设置日志
    def _init_logger(self):
        logger = logging.getLogger('chia-plot-mover')
        # Log Level
        log_level = logging.getLevelName(self.conf_log_level)
        if not isinstance(log_level, int):
            log_level = logging.INFO
        logger.setLevel(log_level)
        self._log("Log level set to {}".format(logging.getLevelName(log_level)))
        # Handler
        handler = None
        if self.conf_log_file is not None:
            try:
                # 创建文件
                if not self.conf_log_file.exists():
                    self.conf_log_file.touch()
                handler = logging.FileHandler(self.conf_log_file.resolve(), 'a+', encoding="utf-8")
            except:
                self._log("Log file {} creation failed, using STDOUT.".format(self.conf_log_file.resolve()),
                          logging.ERROR)
                pass
        if handler is None:
            handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(handler)
        self.logger = logger
        return

    def _scan_incomplete_files(self):
        incomplete_files = {}
        for dst in self.conf_dst:
            for file in dst['dir'].glob('.*.plot.moving'):
                if file.is_file():
                    # 发现待续传文件
                    self._log(f"Found incomplete file: {file}", logging.INFO, method='Scanner')
                    incomplete_files[file] = {
                        'file': file,
                        'size': file.stat().st_size,
                        'last_modified': file.stat().st_mtime,
                        'conf_dst': dst,
                    }
        return incomplete_files

    # 扫描来源文件
    def _scan_source_files(self):
        source_files = {}
        for src in self.conf_src:
            for file in sorted(src['dir'].glob('*.plot')):
                if file.is_file():
                    source_files[file] = {
                        'file': file,
                        'size': file.stat().st_size,
                        'last_modified': file.stat().st_mtime,
                        'device': self._get_filesystem_by_path(src['dir']),
                        'conf_src': src,
                    }
        return source_files

    def _resume(self):
        # 扫描全部待续传文件
        incomplete_files = self._scan_incomplete_files()
        # 扫描全部来源文件
        source_files = self._scan_source_files()
        # 续传
        for dst_file, dst_item in incomplete_files.items():
            src_item = None
            # 寻找file对应的source_file
            for src_file in source_files.keys():
                if src_file.name == dst_file.name[1:-7]:
                    src_item = source_files[src_file]
                    break
            # 删除无法续传的文件
            if src_item is None:
                dst_file.unlink()
                continue
            # 续传未完成的文件
            dst_dir = dst_item['conf_dst']['dir'].resolve()
            with self.mutex:
                if self.workers[dst_dir]['status'] == 'idle':
                    self.workers[dst_dir]['status'] = 'busy'
                    self.workers[dst_dir]['task'] = src_item
                    # 发送续传任务
                    self._log(f"Send Resume task {dst_file} -> {self.workers[dst_dir]['conf_dst']['name']}",
                              logging.DEBUG, method='Scanner')

    def _scanner_thread(self):
        self._log("Scanner thread started.", logging.INFO, method='Scanner')
        try:
            # 等待工作线程准备完毕
            while True:
                ready_count = 0
                for worker in self.workers.values():
                    if worker['status'] == 'idle':
                        ready_count += 1
                if ready_count == len(self.workers):
                    break
                self._check_stop_event()
                time.sleep(0.1)
            # 续传
            self._resume()
            # 扫描线程主逻辑开始
            history_src_files = []
            sleep = 0
            while True:
                # 延迟
                for _ in range(sleep):
                    self._check_stop_event()
                    time.sleep(1)
                with self.mutex:
                    # 从最空闲的来源设备选取一个文件
                    dst_device_busy_list = []
                    src_moving_list = []
                    src_device_busyness_statistics = {}
                    for worker in self.workers.values():
                        if worker['status'] == 'busy':
                            src_device = worker['task']['device']
                            dst_device_busy_list.append(worker['device'])
                            src_moving_list.append(worker['task']['file'].resolve())
                            src_device_busyness_statistics[src_device] = src_device_busyness_statistics.get(src_device,
                                                                                                            0) + 1
                    src_items = []
                    for item in [v for v in self._scan_source_files().values() if
                                 v['file'].resolve() not in src_moving_list]:
                        item['sort'] = src_device_busyness_statistics.get(item['device'], 0)
                        src_items.append(item)
                        if item['file'].resolve() not in history_src_files:
                            self._log(f"Found new file: {item['file']}", logging.INFO, method='Scanner')
                    if len(src_items) == 0:
                        sleep = 10
                        continue
                    history_src_files = [v['file'].resolve() for v in src_items]
                    src_items.sort(key=lambda x: x['sort'])
                    src_item = src_items[0]
                    # 选取一个空闲的目标设备
                    workers = [v for v in self.workers.values() if
                               v['status'] == 'idle' and v['device'] not in dst_device_busy_list
                               and v['estimated_available_capacity'] > src_item['size']]
                    if len(workers) == 0:
                        sleep = 10
                        continue
                    worker = workers[0]
                    # 发送任务
                    self._log(f"Send task: {src_item['file']} -> {worker['conf_dst']['name']}", logging.DEBUG,
                              method='Scanner')
                    worker['status'] = 'busy'
                    worker['task'] = src_item
                    sleep = 0
        except StopEventException:
            self._log("Stop event received, exiting scanner thread.", logging.INFO, method='Scanner')
            return
        except Exception as e:
            self._log("Exception in scanner thread: {}".format(e), logging.ERROR, method='Scanner')
            self._log(traceback.format_exc(), logging.DEBUG, method='Scanner')
            pass

    # 工作线程
    def _worker_thread(self, dst):
        self._log("[{}] Worker thread started.".format(dst['name']), logging.INFO, method='Worker')
        worker = self.workers[dst['dir'].resolve()]
        with self.mutex:
            worker['status'] = 'idle'
        # 工作线程主逻辑开始
        sleep = 0
        last_check_time = None
        while True:
            try:
                # 延迟
                for _ in range(sleep):
                    self._check_stop_event()
                    time.sleep(1)
                sleep = 1
                # 检查磁盘空间
                if last_check_time is None or time.time() - last_check_time > 60:
                    current_available_capacity = shutil.disk_usage(dst['dir']).free
                    estimated_available_capacity = current_available_capacity + sum(
                        (v.stat().st_size for v in dst['dir'].glob('*.plot') if
                         v.is_file() and v.stat().st_mtime < self.conf_expiration_time))
                    with self.mutex:
                        worker['estimated_available_capacity'] = estimated_available_capacity
                    last_check_time = time.time()
                # 检查是否有任务
                with self.mutex:
                    if worker['status'] != 'busy' or worker['task'] is None:
                        continue
                    src_item = worker['task']
                # 如果磁盘空间不足，删除过期文件
                if current_available_capacity < src_item['size']:
                    for expired_file in (v for v in dst['dir'].glob('*.plot') if
                                         v.is_file() and v.stat().st_mtime < self.conf_expiration_time):
                        self._log("[{}] Removing expired file: {}, mtime: {}".format(
                            dst['name'],
                            expired_file.resolve(),
                            time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expired_file.stat().st_mtime))
                        ), logging.INFO, method='Worker')
                        expired_file.unlink()
                        current_available_capacity = shutil.disk_usage(dst['dir']).free
                        if current_available_capacity >= src_item['size']:
                            break
                    if current_available_capacity < src_item['size']:
                        self._log("[{}] Not enough disk space, skip moving: {}".format(dst['name'], src_item['file']),
                                  logging.DEBUG, method='Worker')
                        with self.mutex:
                            worker['estimated_available_capacity'] = current_available_capacity + sum(
                                (v.stat().st_size for v in dst['dir'].glob('*.plot') if
                                 v.is_file() and v.stat().st_mtime < self.conf_expiration_time))
                            worker['task'] = None
                            worker['status'] = 'idle'
                        continue
                # 开始移动
                dst_file = dst['dir'] / ('.' + src_item['file'].name + '.moving')
                self._file_move(src_item['file'], dst_file, dst['name'])
                dst_file.rename(dst_file.parent / (dst_file.name[1:-7]))
                # 更新磁盘空间
                current_available_capacity -= src_item['size']
                # 更新worker状态
                with self.mutex:
                    worker['estimated_available_capacity'] -= src_item['size']
                    worker['task'] = None
                    worker['status'] = 'idle'
            except StopEventException:
                self._log("[{}] Stop event received, exiting worker thread.".format(dst['name']),
                          logging.INFO, method='Worker')
                return
            except Exception as e:
                self._log("[{}] Exception in worker thread: {}".format(dst['name'], e), logging.ERROR,
                          method='Worker')
                self._log(traceback.format_exc(), logging.DEBUG, method='Worker')
                with self.mutex:
                    worker['task'] = None
                    worker['status'] = 'idle'
                continue

    def _file_move(self, src, dst, worker_name, buffer_size=1024 ** 2 * 8):
        try:
            if dst.is_file():
                # 文件续传
                self._log("[{}] Resume copying file: {}".format(worker_name, src.resolve()), method='Worker')
            else:
                self._log("[{}] Start copying file: {}".format(worker_name, src.resolve()), method='Worker')
            time_begin = datetime.now()
            move_size = 0
            with open(src.resolve(), 'rb') as src_file, open(dst.resolve(), 'ab') as dst_file:
                src_file.seek(dst_file.tell())
                while True:
                    buf = src_file.read(buffer_size)
                    if buf:
                        dst_file.write(buf)
                        move_size += len(buf)
                    else:
                        break
                    self._check_stop_event()
            # 复制文件属性和权限
            shutil.copystat(src.resolve(), dst.resolve())
            # 设置目标文件所有者和所属组
            # os.chown(dst.resolve(), src_stat.st_uid, src_stat.st_gid)
            # 删除源文件
            self._log("[{}] Remove source file: {}".format(worker_name, src.resolve()), level=logging.DEBUG,
                      method='Worker')
            src_path = src.resolve()
            src.unlink()
            time_complete = datetime.now()
            use_time = self._usetime_to_text(time_begin, time_complete)
            copy_speed = move_size / (1024 ** 2) / (time_complete - time_begin).total_seconds()
            self._log("[{}] File copying completed: {} , time used: {} ({} Mb/s)".format(
                worker_name, src_path, use_time, format(copy_speed, '0,.0f')
            ), method='Worker')
        except StopEventException:
            self._log("[{}] File copying stopped: {}".format(worker_name, src.resolve()), method='Worker')
            # if dst.is_file():
            # 	self._log("[{}] Remove incomplete file: {}".format(worker_name, dst.resolve()), level=logging.DEBUG,
            # 			 method='Worker')
            # 	dst.unlink()
            raise StopEventException
        except Exception as e:
            self._log("[{}] File copying failed: {}".format(worker_name, src.resolve()),
                      level=logging.ERROR,
                      method='Worker')
            self._log("[{}] Error message: {}".format(worker_name, e), level=logging.ERROR, method='Worker')
            raise e
        return

    # 将时间差转换为文本
    def _usetime_to_text(self, begin, end):
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

    def _get_filesystem_by_path(self, path):
        if str(path) not in self.filesystem_cache:
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
            self.filesystem_cache[str(path)] = filesystem
        return self.filesystem_cache[str(path)]

    # 日志函数
    def _log(self, message, level=logging.INFO, method='Main'):
        if self.logger:
            while True:
                try:
                    data = self.log_message_queue.pop(0)
                    self.logger.log(level=data[0], msg=data[1])
                except:
                    break
            self.logger.log(level=level, msg="{} - {}".format(method, message))
        else:
            self.log_message_queue.append((level, "{} - {}".format(method, message)))
        return

    # 信号处理函数
    def _signal_handler(self, signum, frame):
        self._log("Signal received, signal: {}".format(signum))
        self.stop_event.set()

    # 检查是否收到停止信号
    def _check_stop_event(self, is_raise=True):
        if self.stop_event.is_set():
            if is_raise:
                raise StopEventException()
            else:
                return True
        return False

    # 创建进程锁文件
    def _lockfile_create(self):
        try:
            self._log("Creating lock file: {}".format(self.conf_lock_file.resolve()))
            with open(self.conf_lock_file.resolve(), 'w') as f:
                f.write(str(os.getpid()))
        except Exception as e:
            self._log("{}\n{}".format(sys.exc_info(), traceback.format_exc()), level=logging.ERROR)
            exit(1)

    # 删除进程锁文件
    def _lockfile_delete(self):
        try:
            self._log("Deleting lock file: {}".format(self.conf_lock_file.resolve()))
            self.conf_lock_file.unlink()
        except Exception as e:
            pass


if __name__ == "__main__":
    # 参数解析
    parser = argparse.ArgumentParser(description='Chia Plots Mover')
    parser.add_argument('-f', '--config', required=True, help='Config file')
    args = parser.parse_args()

    # Main
    mover = Mover()
    mover.set_config(args.config)
    mover.run()
