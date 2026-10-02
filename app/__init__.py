"""本地数据管道编排服务。"""

from app.api import create_app
from app.orchestrator import Orchestrator

orchestrator = Orchestrator()
orchestrator.start()

app = create_app(orchestrator)

__all__ = ["app", "Orchestrator", "create_app", "orchestrator"]
