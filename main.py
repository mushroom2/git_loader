import os
import json
import re
import asyncio
import time
from asyncio import Queue, QueueEmpty
from fake_useragent import FakeUserAgent
import aiohttp
from subprocess import Popen, PIPE
from time import sleep
from colorama import init, Fore
import hashlib

init(autoreset=True)

__author__ = 'Light_Mu$hr00m'
__version__ = "0.0.3"

with open('config.json') as f:
    CONFIG = json.load(f)

default_git_files_maybe_dangerous = [
    ["config"],
    ["hooks", "applypatch-msg.sample"],
    ["hooks", "applypatch-msg"],
    ["hooks", "commit-msg.sample"],
    ["hooks", "commit-msg"],
    ["hooks", "fsmonitor-watchman.sample"],
    ["hooks", "fsmonitor-watchman"],
    ["hooks", "post-update.sample"],
    ["hooks", "post-update"],
    ["hooks", "pre-applypatch.sample"],
    ["hooks", "pre-applypatch"],
    ["hooks", "pre-commit.sample"],
    ["hooks", "pre-commit"],
    ["hooks", "pre-merge-commit.sample"],
    ["hooks", "pre-merge-commit"],
    ["hooks", "pre-push.sample"],
    ["hooks", "pre-push"],
    ["hooks", "pre-rebase.sample"],
    ["hooks", "pre-rebase"],
    ["hooks", "pre-receive.sample"],
    ["hooks", "pre-receive"],
    ["hooks", "prepare-commit-msg.sample"],
    ["hooks", "prepare-commit-msg"],
    ["hooks", "update.sample"],
    ["hooks", "update"],
]

default_git_files = [
    ["COMMIT_EDITMSG"],
    ["description"],
    ["FETCH_HEAD"],
    ["HEAD"],
    ["index"],
    ["info", "exclude"],
    ["logs", "HEAD"],
    ["logs", "refs", "remotes", "origin", "HEAD"],
    ["logs", "refs", "stash"],
    ["ORIG_HEAD"],
    ["packed-refs"],
    ["refs", "remotes", "origin", "HEAD"],
    # git stash
    ["refs", "stash"],
    # pack
    ["objects", "info", "alternates"],
    ["objects", "info", "http-alternates"],
    ["objects", "info", "packs"],
]

BRANCH_LIST = []
HEAD_PATH = None


class GitObject(object):
    def __init__(self, _hash):
        self._hash = _hash
        self.downloaded = False
        self.parsed = False

    def __hash__(self):
        return self._hash

    @property
    def url(self):
        git_url = '/'.join((CONFIG['source'], CONFIG['default_git_folder']))
        return '/'.join((git_url, 'objects', self.folder, self.filename))

    @property
    def folder(self):
        return self._hash[:2]

    @property
    def filename(self):
        return self._hash[2:]

    @property
    def path(self):
        return os.path.join(CONFIG['result_path'], CONFIG['default_git_folder'], 'objects', self.folder, self.filename)

    @property
    async def is_blob(self):
        os.chdir(CONFIG['result_path'])
        proc = await asyncio.create_subprocess_shell(f'git cat-file -t {self._hash}', stdout=PIPE)
        stdout = await proc.stdout.read()
        return stdout == b'blob\n'

    async def parse_handle(self):
        os.chdir(CONFIG['result_path'])
        proc = await asyncio.create_subprocess_shell(f'git cat-file -p {self._hash}', stdout=PIPE, stderr=PIPE)
        stdout, err = await proc.communicate()
        if err:
            print(Fore.RED + '[!]' + err.decode('utf8'))
            if os.path.exists(self.path):
                os.remove(self.path)
        parsed_objects = re.findall('[0-9a-f]{40}', stdout.decode('utf8'))
        return parsed_objects

    async def parse(self):
        parsed_objects = []
        if not self.parsed and not await self.is_blob:
            parsed_objects = await self.parse_handle()
            self.parsed = True
        return list(set(parsed_objects))


class ObjectsList(list):
    def __contains__(self, item: GitObject):
        return item._hash in (_._hash for _ in self)


class GitGrabber(object):
    def __init__(self):
        self.user_agent = FakeUserAgent(use_cache_server=False)
        self.branch_list = []
        self.head_path = None
        self.parse_queue = Queue()
        self.download_queue = Queue()
        self.git_url = '/'.join((CONFIG['source'], CONFIG['default_git_folder']))
        self.downloaded_objects = ObjectsList()
        self.parse_empty_count = 0
        self.fsck_count = 0
        self.downloaded = False
        self.fsck_hash = None
        self.timeout_error_counter = 0

    def do_fsck(self):
        os.chdir(CONFIG['result_path'])
        proc = Popen(('git', 'fsck', '--full'), stdout=PIPE, stderr=PIPE)
        stdout, err = proc.communicate()
        output = stdout + err
        _hash = hashlib.md5(output).hexdigest()
        parsed_objects = list(set(re.findall('[0-9a-f]{40}', output.decode('utf8'))))
        return parsed_objects, _hash

    def save_object(self, name, content):
        path = f'/objects/{name[:2]}'
        fn = name[2:]
        self.save_file(path, fn, content)

    async def get_branch_list(self, session):
        branch_names = CONFIG['branch_names']
        print(branch_names)
        if os.path.exists(os.path.join(CONFIG['result_path'], '.git', 'FETCH_HEAD')):
            with open(os.path.join(CONFIG['result_path'], '.git', 'FETCH_HEAD'), 'r') as fetch_head_file:
                for line in fetch_head_file:
                    branch_from_fetch = re.sub(r"^.+ '|'.*", '', line).strip()
                    if branch_from_fetch:
                        branch_names.append(branch_from_fetch)

        # todo separate on local and origin?
        if os.path.exists(os.path.join(CONFIG['result_path'], '.git', 'packed-refs')):
            with open(os.path.join(CONFIG['result_path'], '.git', 'packed-refs'), 'r') as packed_refs_file:
                for line in packed_refs_file:
                    if '/' in line:
                        brench_from_pack = line.split('/')[-1].strip()
                        if brench_from_pack:
                            branch_names.append(brench_from_pack)

        print(branch_names)
        branch_names = list(set(branch_names))
        find_branch_tasks = []
        for branch_name in branch_names:
            find_branch_tasks.append(asyncio.ensure_future(self.find_branch(session, branch_name)))
        await asyncio.gather(*find_branch_tasks)

    async def fetch_and_save(self, session, url, path, filename, obj=None):
        async with session.get(url) as resp:
            if obj:
                self.downloaded_objects.append(obj)
            await resp.read()
            status = resp.status
            if status != 200:
                print(f'{Fore.YELLOW}[!] Request {url} got {status}')
                return

            content = await resp.read()
            if obj:
                self.save_object(obj._hash, content)
                print(f'[i] Downloaded {obj._hash}')
                self.parse_queue.put_nowait(obj)
            else:
                self.save_file(path, filename, content)

    async def find_branch(self, session, branch_name):
        head_file_url = '/'.join((self.git_url, 'refs', 'heads', branch_name))
        async with session.get(head_file_url) as head_resp:
            if head_resp.status != 200:
                print(f'{Fore.YELLOW}[!] {branch_name} {head_resp.status}')
                return
            head_content = await head_resp.read()
            if re.findall('[0-9a-f]{40}', head_content.decode('utf8')) and len(head_content) < 60:
                print(f'{Fore.GREEN}[i] Branch found: [{branch_name}]')
                self.download_queue.put_nowait(GitObject(head_content.decode('utf8').strip()))
                print(f'{Fore.GREEN}[i] {head_content.decode("utf8").strip()} added to download queue')
                self.save_file(
                    os.path.join('refs', 'heads'),
                    branch_name, head_content)
                self.branch_list.append(branch_name)

    async def get_main_head(self, session):
        with open(os.path.join(CONFIG['result_path'], '.git', 'HEAD')) as f:
            head_file_content = f.read()
            head_path = head_file_content.split(' ')[-1].strip()
            head_obj_url = '/'.join((CONFIG['source'], '.git', head_path))
        async with session.get(head_obj_url) as resp:
            object_name = await resp.text()
            head_obj_file_url = '/'.join((self.git_url, 'objects', object_name[:2], object_name[2:]))
            save_head_path = head_path.split('/')
            self.save_file(os.path.join(*save_head_path[:-1]), save_head_path[-1], object_name)

        async with session.get(head_obj_file_url) as head_obj_resp:
            content = await head_obj_resp.read()
            self.save_object(object_name.strip(), content)
            head_obj = GitObject(object_name.strip())
            head_obj.downloaded = True
            self.downloaded_objects.append(head_obj)
            self.parse_queue.put_nowait(head_obj)

    async def download_pack(self, session, pack_hash, is_idx=False):
        pack_filename = f'pack-{pack_hash}.{"idx" if is_idx else "pack"}'
        pack_url = '/'.join((self.git_url, 'objects', 'pack', pack_filename))
        async with session.get(pack_url) as pack_req:
            content = await pack_req.read()
            print(f'{Fore.GREEN} pack {"index" if is_idx else ""} downloaded with status {pack_hash}')
            if pack_req.status == 200:
                with open(
                        os.path.join(
                            CONFIG['result_path'], '.git', 'objects', 'pack', pack_filename), 'wb'
                ) as f:
                    f.write(content)

    async def get_known_packs(self, session):
        pack_tasks = []
        pack_path = os.path.join(CONFIG['result_path'], '.git', 'objects', 'info', 'packs')
        if os.path.exists(pack_path):
            with open(pack_path) as f:
                for pack_lnk in f:
                    if 'pack' in pack_lnk:
                        print(f'{Fore.GREEN} Found pack {pack_hash}')
                        pack_hash = pack_lnk.split(' ')[1].split('.')[0]
                        pack_tasks.append(asyncio.ensure_future(self.download_pack(session, pack_hash, is_idx=False)))
                        pack_tasks.append(asyncio.ensure_future(self.download_pack(session, pack_hash, is_idx=True)))
        await asyncio.gather(*pack_tasks)

    async def download_git_schema(self):
        tasks = []
        head_url = '/'.join((self.git_url, 'HEAD'))
        index_url = '/'.join((self.git_url, 'index'))
        config_url = '/'.join((self.git_url, 'config'))
        remote_head_url = '/'.join((self.git_url, 'FETCH_HEAD'))
        packed_refs_url = '/'.join((self.git_url, 'packed-refs'))
        info_packs_url = '/'.join((self.git_url, "objects", "info", "packs"))

        async with aiohttp.ClientSession(headers={
            'user-agent': self.user_agent.chrome
        }) as session:

            tasks.append(asyncio.ensure_future(self.fetch_and_save(session, head_url, '', 'HEAD')))
            tasks.append(asyncio.ensure_future(self.fetch_and_save(session, index_url, '', 'index')))
            tasks.append(asyncio.ensure_future(self.fetch_and_save(session, config_url, '', 'config')))
            tasks.append(asyncio.ensure_future(self.fetch_and_save(session, remote_head_url, '', 'FETCH_HEAD')))
            tasks.append(asyncio.ensure_future(self.fetch_and_save(session, packed_refs_url, '', 'packed-refs')))
            tasks.append(asyncio.ensure_future(self.fetch_and_save(session, info_packs_url,
                                                                   os.path.join('objects', 'info'), 'packs')))

            await asyncio.gather(*tasks)
            await self.get_main_head(session)
            await self.get_branch_list(session)
            await self.get_known_packs(session)

    async def download_repo(self):
        while not self.downloaded:
            await self.parse_worker()
            await self.download_worker()

    async def parse_coro(self, obj):
        parse_res = await obj.parse()
        print(f'[i] parsed {obj._hash}')
        for to_download in parse_res:
            print(f'[i] new task to download {to_download}')
            if GitObject(to_download) not in self.downloaded_objects:
                self.download_queue.put_nowait(GitObject(to_download))

    async def parse_worker(self):
        tasks = []
        while 1:
            try:
                obj: GitObject = self.parse_queue.get_nowait()
                tasks.append(asyncio.ensure_future(self.parse_coro(obj)))
            except QueueEmpty:
                if tasks:
                    await asyncio.gather(*tasks)
                else:
                    print(Fore.BLUE + '[!] parse queue is empty')
                    self.parse_empty_count += 1
                    if self.parse_empty_count > 3:
                        self.parse_empty_count = 0
                        self.fsck_count += 1
                        print(f'{Fore.GREEN} [i] FSCK #{self.fsck_count} started')
                        new_objects, _hash = self.do_fsck()
                        print(f'{Fore.GREEN} [i] FSCK hashes (new/old) {_hash}, {self.fsck_hash}')
                        if self.fsck_hash != _hash:
                            self.fsck_hash = _hash
                        else:
                            self.downloaded = True

                        if not new_objects:
                            self.downloaded = True
                        else:
                            for to_download in new_objects:
                                obj = GitObject(to_download)
                                if obj not in self.downloaded_objects:
                                    print(f'{Fore.BLUE}[i] new task to download {to_download} from fsck output')
                                    self.download_queue.put_nowait(obj)
                                else:
                                    print(f'{Fore.YELLOW}[!] skip {to_download}'
                                          f' (file has already downloaded) from fsck output')
                break

    async def download_coro(self, session, obj):
        try:
            async with session.head(obj.url) as head_resp:
                self.downloaded_objects.append(obj)
                await head_resp.read()
                status = head_resp.status
                if status != 200:
                    print(f'{Fore.YELLOW}[!] Object {obj._hash} got {status} when download')
                    return
            async with session.get(obj.url) as obj_resp:
                content = await obj_resp.read()
                self.save_object(obj._hash, content)

                print(f'[i] Downloaded {obj._hash}')

                self.parse_queue.put_nowait(obj)
        except asyncio.TimeoutError:
            print(f'{Fore.RED}[!] TIMEOUT ERROR on {obj.url}')
            self.timeout_error_counter += 1
            if self.timeout_error_counter <= 10:
                self.downloaded_objects.remove(obj)
                self.download_queue.put_nowait(obj)

    async def download_worker(self):
        tasks = []
        async with aiohttp.ClientSession(headers={
            'user-agent': self.user_agent.chrome
        }) as session:
            while 1:
                try:
                    obj: GitObject = self.download_queue.get_nowait()
                    if obj not in self.downloaded_objects:
                        tasks.append(asyncio.ensure_future(self.download_coro(session, obj)))
                except QueueEmpty:
                    print(f'{Fore.YELLOW}[!] download queue is empty; {len(tasks)} tasks was started ')
                    if tasks:
                        await asyncio.gather(*tasks)
                    sleep(.1)
                    break

    @staticmethod
    def save_file(path, filename, content):
        repo_path = os.path.join(CONFIG['result_path'], '.git')
        safe_path = os.path.join(repo_path, *path.split('/'))
        os.makedirs(safe_path, exist_ok=True)
        fn = os.path.join(safe_path, filename)
        if isinstance(content, bytes):
            with open(fn, 'wb') as f:
                f.write(content)
        else:
            with open(fn, 'w+') as f:
                f.write(content)

    @staticmethod
    def make_base_tree():
        os.makedirs(os.path.join(CONFIG['result_path'], '.git'), exist_ok=True)


if __name__ == '__main__':
    g = GitGrabber()
    g.make_base_tree()
    start = time.time()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(g.download_git_schema())
    loop.run_until_complete(g.download_repo())
    end = time.time() - start
    print(f'{Fore.GREEN} Done! Downloaded in {end}')

    # version 0.1 1385.0098085403442 seconds
    # version 0.2 212.8488848209381 # 5c1b32ed58f1b8c6b7a8af34f382843b
    # version 0.3 172.6314718723297 # 5c1b32ed58f1b8c6b7a8af34f382843b
    # version 0.3 751.4795854091644 # 5c1b32ed58f1b8c6b7a8af34f382843b ???
    # version 0.2 resource "b" 8402.140508174896
