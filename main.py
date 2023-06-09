import asyncio
import os
import sys

import asyncssh


async def run_scp(source_dir, target_dir, host, port, username, password=None, private_key=None) -> None:
    client_keys = [asyncssh.import_private_key(private_key)] if private_key else []
    async with asyncssh.connect(
            host,
            port=int(port),
            username=username,
            password=password,
            known_hosts=None,
            client_keys=client_keys
    ) as conn:
        await asyncssh.scp(srcpaths=source_dir, dstpath=(conn, f'{target_dir}/'))
        print('ok!')


async def run(source_dir, target_dir, host_raw, username, password=None, private_key=None):
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
        print(f'scping: {host}:{port}')
        await run_scp(source_dir, target_dir, host, port, username, password, private_key)
        print(f'scp success: {host}:{port}')
        # tasks.append(run_scp(source_dir, target_dir, host, port, username, password, private_key))
    # await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == '__main__':
    try:
        source_dir = os.environ.get('INPUT_SOURCE')
        target_dir = os.environ.get('INPUT_TARGET')
        host_raw = os.environ.get('INPUT_HOST')
        username = os.environ.get('INPUT_USERNAME')
        password = os.environ.get('INPUT_PASSWORD')
        private_key = os.environ.get('INPUT_KEY')
        asyncio.run(run(source_dir, target_dir, host_raw, username, password, private_key))
    except (OSError, asyncssh.Error) as exc:
        sys.exit(f'SSH connection failed: ' + str(exc))
