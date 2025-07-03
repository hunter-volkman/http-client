import asyncio
from viam.module.module import Module

try:
    from models.mission import Mission
except ModuleNotFoundError:
    # when running as local module with run.sh
    from .models.mission import Mission


if __name__ == '__main__':
    asyncio.run(Module.run_from_registry())