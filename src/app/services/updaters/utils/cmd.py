import asyncio
from typing import Optional


async def run_cmd(cmd: str) -> tuple[bytes, bytes, Optional[int]]:
    proc = await asyncio.create_subprocess_shell(
        cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )

    stdout, stderr = await proc.communicate()
    return stdout, stderr, proc.returncode
