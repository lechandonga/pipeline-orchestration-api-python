"""本地模拟任务执行器，用可配置的等待与失败注入代替真实外部系统。"""

from __future__ import annotations

import threading
import time


class SimulatedFailure(Exception):
    pass


class CancellationRequested(Exception):
    pass


class LocalTaskExecutor:
    """按 simulation 配置模拟任务单次尝试。

    fail_times: 前 N 次尝试抛 SimulatedFailure；duration_seconds: 尝试耗时。
    等待过程中收到取消信号则抛 CancellationRequested，不留下半完成结果。
    """

    def execute(
        self,
        task_name: str,
        attempt: int,
        simulation: dict[str, float | int] | None,
        cancel_event: threading.Event,
    ) -> None:
        duration = float(simulation.get("duration_seconds", 0.0)) if simulation else 0.0
        fail_times = int(simulation.get("fail_times", 0)) if simulation else 0

        if duration > 0.0:
            if cancel_event.wait(timeout=duration):
                raise CancellationRequested(f"任务 '{task_name}' 在第 {attempt} 次尝试中被取消")

        if attempt <= fail_times:
            raise SimulatedFailure(
                f"任务 '{task_name}' 第 {attempt} 次尝试模拟失败（共注入 {fail_times} 次失败）"
            )

    def sleep_cancellable(self, delay: float, cancel_event: threading.Event) -> bool:
        """重试退避等待，返回 True 表示等待期间被取消。"""
        return cancel_event.wait(timeout=max(delay, 0.0))


executor = LocalTaskExecutor()
