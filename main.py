import asyncio
import os
import sys
import time
import aiosocks

import asyncssh


class SocksClientConnection:
    def __init__(self, proxy):
        user_pass, host_port = proxy.split('@')
        self.username, self.password = user_pass.split(':')
        self.host, self.port = host_port.split(':')

    async def create_connection(self, session_factory, host, port):
        socks5_addr = aiosocks.Socks5Addr(self.host, int(self.port))
        socks5_auth = aiosocks.Socks5Auth(self.username, self.password)
        return await aiosocks.create_connection(
            session_factory, proxy=socks5_addr, proxy_auth=socks5_auth, dst=(host, port)
        )


async def zip_local(source_dir):
    proc = await asyncio.create_subprocess_shell(
        f'zip -r {source_dir}.zip {source_dir}',
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        print(f'zip ERROR! {stderr.decode()}')
        return None
    return f'{source_dir}.zip'


async def run_scp(source_dir, target_dir, host, port, username, password=None, private_key=None, proxy=None) -> None:
    scp_start = time.monotonic()
    try:
        zip_file = await zip_local(source_dir)
        if zip_file is None:
            raise Exception('zip error!')
        client_keys = [asyncssh.import_private_key(private_key)] if private_key else []
        time_start = time.monotonic()
        tunnel = SocksClientConnection(proxy) if proxy else ()
        async with asyncssh.connect(
                host,
                port=int(port),
                tunnel=tunnel,
                username=username,
                password=password,
                known_hosts=None,
                client_keys=client_keys
        ) as conn:
            print(f'{host}: Start remove {target_dir}')
            await conn.run(f'mkdir -p {target_dir}')
            await conn.run(f'rm -rf {target_dir}')
            await conn.run(f'mkdir -p {target_dir}')
            print(f'{host}: Start upload {source_dir} to {target_dir} ...')

            await conn.run(f'mkdir -p {target_dir}/tmp')
            await asyncssh.scp(srcpaths=zip_file, dstpath=(conn, f'{target_dir}/tmp'), recurse=True, preserve=True)
            print(f'{host}: ok! cost: {time.monotonic() - time_start:.2f} s')
            print(f'{host}: Start unzip {zip_file} ...')
            time_start = time.monotonic()
            await conn.run(f'unzip -o {target_dir}/tmp/{zip_file} -d {target_dir}')
            print(f'{host}: unzip ok! cost: {time.monotonic() - time_start:.2f} s')
            print(f'{host}: Start remove {target_dir}/tmp')
            await conn.run(f'rm -rf {target_dir}/tmp')
            print(f'{host}: remove ok!')
    except Exception as e:
        print(f'{host}: ERROR! {e} {type(e)}')
    finally:
        print(f'{host}: scp ok! cost: {time.monotonic() - scp_start:.2f} s')


async def run(source_dir, target_dir, host_raw, username, password=None, private_key=None, proxy=None):
    tasks = []
    if not any((password, private_key)):
        raise ValueError('password and private_key must have one')
    host_raw_list = []
    if ',' in host_raw:
        host_raw_list = host_raw.split(',')
    else:
        host_raw_list.append(host_raw)
    for raw in host_raw_list:
        host, port = raw.split(':')
        print(f'Scping: {host}:{port}')
        # await run_scp(source_dir, target_dir, host, port, username, password, private_key)
        # print(f'scp success: {host}:{port}')
        tasks.append(run_scp(source_dir, target_dir, host, port, username, password, private_key, proxy))
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == '__main__':
    try:
        source_dir = os.environ.get('INPUT_SOURCE')
        target_dir = os.environ.get('INPUT_TARGET')
        host_raw = os.environ.get('INPUT_HOST')
        username = os.environ.get('INPUT_USERNAME')
        password = os.environ.get('INPUT_PASSWORD')
        private_key = os.environ.get('INPUT_KEY')
        proxy = os.environ.get('INPUT_PROXY')
        asyncio.run(run(source_dir, target_dir, host_raw, username, password, private_key, proxy))
    except (OSError, asyncssh.Error) as exc:
        sys.exit(f'SSH connection failed: ' + str(exc))
