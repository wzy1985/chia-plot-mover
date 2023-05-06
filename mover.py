# coding:utf8
import json
import sys
import os
import getopt
import subprocess
import threading
import time
import psutil
import multiprocessing
import logging
import math
import hashlib
import types
import signal
import copy
import datetime


class Mover:
    config = {}

    dest_info = {}

    session = {}

    mutex = multiprocessing.Lock()

    logger = None

    is_main_proc = True

    def __init__(self, config_file):
        self.load_config(config_file)
        self.logger_init()
        self.lockfile_create()
        self.session_load()
        self.run()
        return

    def __del__(self):
        self.lockfile_delete()
        return

    def logger_init(self):
        logger = logging.getLogger('chia-plot-mover')
        logger.setLevel(logging.INFO)
        # STDOUT
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(stdout_handler)
        # FILE
        # file = logging.FileHandler(self.config['main']['log_dir'] + '/chia-plot-mover.log', 'a+', encoding="utf-8")
        # file.setFormatter(formatter)
        # logger.addHandler(file)
        self.logger = logger
        self.logger.info("chia-plot-mover was startup, PID: " + str(os.getpid()))

    def load_config(self, config_file):
        try:
            with open(config_file, mode='r') as fp:
                config = json.loads(fp.read())
            # Parsing configuration
            # -------------- main --------------
            _main = {}
            # Optional: session_file (default: /tmp/chia-plot-mover.session)
            if 'session_file' not in config['main'] or not config['main']['session_file'].strip():
                _main['session_file'] = '/tmp/chia-plot-mover.session'
            else:
                _main['session_file'] = config['main']['session_file'].strip()
            # Optional: lock_file (default: /var/run/chia-plot-mover.lock)
            if 'lock_file' not in config['main'] or not config['main']['lock_file'].strip():
                _main['lock_file'] = '/var/run/chia-plot-mover.lock'
            else:
                _main['lock_file'] = config['main']['lock_file'].strip()
            # Optional: dest_file_expiration_time (default: 0)
            if 'dest_file_expiration_time' not in config['main'] or not config['main']['dest_file_expiration_time'].strip():
                _main['dest_file_expiration_time'] = 0
            else:
                _main['dest_file_expiration_time'] = time.mktime(time.strptime(config['main']['dest_file_expiration_time'].strip(), '%Y-%m-%d %H:%M:%S'))
            self.config['main'] = _main
            # -------------- source --------------
            _source = []
            for k in range(len(config['source'])):
                value = config['source'][k]
                # Required: name
                if 'name' not in value or not value['name'].strip():
                    raise Exception('source[' + str(k) + '].name: This field is required.')
                name = value['name'].rstrip('/')
                # Required: dir
                path = os.path.abspath(value['dir'])
                if not os.path.isdir(path):
                    raise Exception('source[' + str(k) + '].dir: The directory ' + value['dir'] + ' does not exist or is not a directory.')
                _source.append({
                    'name': name,
                    'dir': path,
                })
            if len(_source) == 0:
                raise Exception('At least 1 source must be configured.')
            self.config['source'] = _source
            # -------------- dest --------------
            _dest = []
            for k in range(len(config['dest'])):
                value = config['dest'][k]
                # Required: name
                if 'name' not in value or not value['name'].strip():
                    raise Exception('dest[' + str(k) + '].name: This field is required.')
                name = value['name'].strip()
                # Required: dir
                path = os.path.abspath(value['dir'])
                if not os.path.isdir(path):
                    raise Exception('dest[' + str(k) + '].dir: The directory ' + value['dir'] + ' does not exist or is not a directory.')
                _dest.append({
                    'name': name,
                    'dir': path,
                })
            if len(_dest) == 0:
                raise Exception('At least 1 destination must be configured.')
            self.config['dest'] = _dest
        except Exception as e:
            print('line %d, Configuration error: %s' % (e.__traceback__.tb_lineno, str(e).strip("'")))
            exit(2)
        return

    # Main process
    def run(self):
        # Scanning destination
        self.scan_dest()
        while True:
            # Refreshing session
            self.session_refresh()
            # Auto move
            self.auto_move()
            # Sleep
            time.sleep(10)

    def session_load(self):
        if os.path.isfile(self.config['main']['session_file']):
            with self.mutex:
                with open(self.config['main']['session_file'], mode='r') as fp:
                    self.session = json.loads(fp.read())
        if 'source' not in self.session:
            self.session['source'] = []
        if 'filesystem_available' not in self.session:
            self.session['filesystem_available'] = {}
        self.session['workers'] = {}

    def session_set(self, key, value=None, update_fn=None):
        with self.mutex:
            if isinstance(update_fn, types.FunctionType):
                self.session[key] = update_fn(self.session[key])
            else:
                self.session[key] = value
            with open(self.config['main']['session_file'], mode='w') as fp:
                fp.write(json.dumps(self.session))

    def scan_dest(self):
        result = {}
        for k in range(len(self.config['dest'])):
            dest = self.config['dest'][k]
            result[dest['dir']] = []
            for filename in os.listdir(dest['dir']):
                path = os.path.join(dest['dir'], filename)
                stat = os.stat(path)
                result[dest['dir']].append((path, stat))
        self.dest_info = result

    # Refreshing session
    def session_refresh(self):
        # Updating real-time statistics of the file system
        fs_available = {}
        fp = subprocess.Popen(
            "df -m | awk '{print $1,$4}'",
            shell=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            universal_newlines=True
        )
        fp.stdout.readline()
        while True:
            line = fp.stdout.readline()
            if not line:
                break
            (filesystem, available) = line.strip().split(' ')
            fs_available[filesystem] = int(available)
        self.session_set('filesystem_available', fs_available)
        # Scanning all source files
        tmp = {}
        for k in range(len(self.config['source'])):
            source = self.config['source'][k]
            tmp[k] = []
            for filename in os.listdir(source['dir']):
                file = os.path.join(source['dir'], filename)
                if not os.path.isfile(file):
                    continue
                basename, suffix = os.path.splitext(filename)
                if basename[:1] == '.' or suffix != '.plot':
                    continue
                tmp[k].append((file, math.ceil(os.path.getsize(file)/1024/1024)))
        # Cross-merging source lists
        source_list = copy.deepcopy(self.session['source'])
        source_list_map = list(map(lambda v: v[0], source_list))
        j = 0
        while True:
            is_continue = False
            for i in range(len(tmp)):
                try:
                    if tmp[i][j][0] not in source_list_map:
                        self.logger.info('Found new plot: %s' % tmp[i][j][0])
                        source_list.append(tmp[i][j])
                    is_continue = True
                except:
                    pass
            if not is_continue:
                break
            j += 1
        # Remove data from session.source that has already been completed or does not exist
        for k in range(len(source_list)):
            file = source_list[k][0]
            if not os.path.isfile(file):
                source_list[k] = None
        source_list = list(filter(None, source_list))
        # Update session
        self.session_set('source', source_list)
        return

    def auto_move(self):
        source_list = copy.deepcopy(self.session['source'])
        fs_available = copy.deepcopy(self.session['filesystem_available'])
        # Retrieve all sources waiting to be transmitted
        workers_source = list(map(lambda v: v['source'][0], self.session['workers'].values()))
        for k in range(len(source_list)):
            if source_list[k][0] in workers_source:
                source_list[k] = None
        source_list = list(filter(None, source_list))
        if len(source_list) == 0:
            return
        # Check for the existence of temporary files for incomplete transfers due to transmission errors
        workers_dest = list(map(lambda v: v['dest'], self.session['workers'].values()))
        lost = []
        for k in range(len(self.config['dest'])):
            dest = self.config['dest'][k]
            for filename in os.listdir(dest['dir']):
                file = os.path.join(dest['dir'], filename)
                if not os.path.isfile(file):
                    continue
                basename, suffix = os.path.splitext(filename)
                if basename[:1] == '.' and suffix == '.moving':
                    dest_file = os.path.join(dest['dir'], filename[1:-7])
                    if dest_file not in workers_dest:
                        lost.append(dest_file)
        # If there are unfinished tasks, prioritize resuming transfers
        if len(lost) > 0:
            dest_file = lost.pop(0)
            for source in source_list:
                (source_file, *_) = source
                if os.path.basename(source_file) == os.path.basename(dest_file):
                    self.logger.info(
                        'recovery task in mover. plot: %s , path: %s/ -> %s/' %
                        (os.path.basename(source_file), os.path.dirname(source_file), os.path.dirname(dest_file))
                    )
                    threading.Thread(target=self.move_worker, args=(source, dest_file), daemon=True).start()
                    return
        # Count available destinations
        fs_used = []
        for _, worker in self.session['workers'].items():
            (_, size) = worker['source']
            fs = worker['filesystem']
            fs_available[fs] -= size
            if fs not in fs_used:
                fs_used.append(fs)
        # Generate task destinations
        source = source_list.pop(0)
        (source_file, source_size) = source
        # Prioritize generating move tasks
        for k in range(len(self.config['dest'])):
            dest = self.config['dest'][k]
            fs = get_filesystem_by_path(dest['dir'])
            if fs in fs_used:
                continue
            if fs_available[fs] < source_size:
                continue
            dest_file = os.path.join(dest['dir'], os.path.basename(source_file))
            self.logger.info(
                'new task in mover. plot: %s , path: %s/ -> %s/' %
                (os.path.basename(source_file), os.path.dirname(source_file), os.path.dirname(dest_file))
            )
            threading.Thread(target=self.move_worker, args=(source, dest_file), daemon=True).start()
            return
        # If the destination disk is full, generate replacement tasks
        if self.config['main']['dest_file_expiration_time'] > 0:
            for (dir, flist) in self.dest_info.items():
                for k in range(len(flist)):
                    (path, stat) = flist[k]
                    if stat.st_mtime > self.config['main']['dest_file_expiration_time']:
                        continue
                    fs = get_filesystem_by_path(dir)
                    if fs in fs_used:
                        continue
                    # Delete first
                    timestruct = time.localtime(stat.st_mtime)
                    self.logger.info(
                        'remove expiration plot: %s , mtime: %s' %
                        (path, time.strftime('%Y-%m-%d %H:%M:%S', timestruct))
                    )
                    os.remove(path)
                    del self.dest_info[dir][k]
                    fs_available[fs] += stat.st_size / 1024 / 1024
                    # If there is still not enough space after deletion, continue deleting in a loop
                    if source_size > (fs_available[fs] + stat.st_size/1024/1024):
                        continue
                    # Move afterwards
                    dest_file = os.path.join(dir, os.path.basename(source_file))
                    self.logger.info(
                        'new task in mover. plot: %s , path: %s/ -> %s/' %
                        (os.path.basename(source_file), os.path.dirname(source_file), os.path.dirname(dest_file))
                    )
                    threading.Thread(target=self.move_worker, args=(source, dest_file), daemon=True).start()
                    return
        return

    def move_worker(self, source, dest):
        def update_fn(workers):
            ident = threading.get_ident()
            workers[ident] = {
                'ident': ident,
                'source': source,
                'dest': dest,
                'filesystem': get_filesystem_by_path(os.path.dirname(dest))
            }
            return workers
        self.session_set('workers', update_fn=update_fn)
        begin_time = datetime.datetime.now()
        # Copy file
        (source_file, source_size) = source
        copy_once_size = 4 * 1024 * 1024
        if not os.path.isfile(dest):
            dest_tmp = os.path.join(os.path.dirname(dest), '.' + os.path.basename(dest) + '.moving')
            with open(source_file, 'rb') as f1, open(dest_tmp, 'ab') as f2:
                f1.seek(f2.tell())
                while True:
                    blob = f1.read(copy_once_size)
                    if blob == b'':
                        break
                    f2.write(blob)
        else:
            dest_tmp = dest
        end_copy_time = datetime.datetime.now()
        # Quickly verify file consistency
        try:
            size1 = os.path.getsize(source_file)
            size2 = os.path.getsize(dest_tmp)
            if size1 != size2:
                raise Exception('File move verification failed. The source and target files are of different sizes.')
            verify_count = 200
            verify_once_size = 1 * 1024 * 1024
            hash1 = hashlib.md5()
            hash2 = hashlib.md5()
            with open(source_file, 'rb') as f1, open(dest_tmp, 'rb') as f2:
                for i in range(verify_count):
                    pos = int(size1 / verify_count * i)
                    f1.seek(pos)
                    hash1.update(f1.read(verify_once_size))
                    f2.seek(pos)
                    hash2.update(f2.read(verify_once_size))
            hex1 = hash1.hexdigest()
            hex2 = hash2.hexdigest()
            if hex1 != hex2:
                raise Exception('File move verification failed. The source and target files have differences in content.')
            end_verify_time = datetime.datetime.now()
            # Copy completed
            if dest_tmp != dest:
                os.rename(dest_tmp, dest)
            os.remove(source_file)
            # Update session
            def update_fn(workers):
                workers.pop(threading.get_ident())
                return workers
            self.session_set('workers', update_fn=update_fn)
            # Output log
            copy_usetime_txt = usetime_to_text(begin_time, end_copy_time)
            copy_speed = source_size / (end_copy_time - begin_time).total_seconds()
            verify_usetime_txt = usetime_to_text(end_copy_time, end_verify_time)
            total_usetime_txt = usetime_to_text(begin_time, end_verify_time)
            self.logger.info(
                'file move finished. plot: %s , path: %s/ -> %s/ , copy use: %s (%s Mb/s), verify use: %s, total use: %s' %
                (os.path.basename(source_file), os.path.dirname(source_file), os.path.dirname(dest),
                 copy_usetime_txt, format(copy_speed, '0,.0f'), verify_usetime_txt, total_usetime_txt)
            )
        except Exception:
            os.remove(dest_tmp)
            self.logger.warning(
                'file move finished, but verify fail, tmp file has deleted! plot: %s , path: %s/ -> %s/' %
                (os.path.basename(source_file), os.path.dirname(source_file), os.path.dirname(dest))
            )
            pass
        return

    def lockfile_create(self):
        if os.path.isfile(self.config['main']['lock_file']):
            with open(self.config['main']['lock_file'], mode='r') as fp:
                pid = fp.read()
            try:
                procinfo = psutil.Process(pid)
                if procinfo:
                    self.logger.error('There is already a Mover process running with PID=' + pid)
                    exit(2)
            except Exception:
                pass
        try:
            with open(self.config['main']['lock_file'], mode='w') as fp:
                fp.write(str(os.getpid()))
        except Exception as e:
            self.logger.error(str(e).strip("'"))
            exit(2)

    def lockfile_delete(self):
        try:
            os.remove(self.config['main']['lock_file'])
        except:
            pass



def main():
    config_file = ''
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'h', ['config='])
    except getopt.GetoptError:
        print(__file__ + ' --config=<config file>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print(__file__ + ' --config=<config file>')
            sys.exit()
        elif opt == '--config':
            config_file = arg
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGTERM, quit)
    Mover(config_file)


def quit(signum, frame):
    print ('You choose to stop me. signum:', signum)
    sys.exit()


def get_filesystem_by_path(path):
    fp = subprocess.Popen(
        'df -m ' + path + " | awk '{print $1}'",
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        universal_newlines=True
    )
    fp.stdout.readline()
    return fp.stdout.readline().strip()


def usetime_to_text(begin, end):
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


if __name__ == "__main__":
    main()

