from collections.abc import Callable
from datetime import datetime

from apscheduler.executors.asyncio import AsyncIOExecutor
from apscheduler.executors.pool import ProcessPoolExecutor, ThreadPoolExecutor
from apscheduler.job import Job
from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from redis.asyncio import Redis

from app.core.logger import log
from app.utils.cron_util import CronUtil

scheduler = AsyncIOScheduler()
scheduler.configure(
    jobstores={
        "default": MemoryJobStore(),
    },
    executors={
        "default": AsyncIOExecutor(),
        "threadpool": ThreadPoolExecutor(max_workers=10),
        "processpool": ProcessPoolExecutor(max_workers=1),
    },
    job_defaults={
        "coalesce": True,
        "max_instances": 5,
    },
    timezone="Asia/Shanghai",
)


class SchedulerUtil:
    """
    定时任务调度器工具类（简化版）
    """

    redis_instance: Redis | None = None

    @classmethod
    async def init_scheduler(cls, redis: Redis | None = None) -> None:
        """
        应用启动时初始化定时任务。
        """
        if redis:
            cls.redis_instance = redis
        scheduler.start()
        log.info("✅ 定时任务调度器已启动")

    @classmethod
    def start(cls, paused: bool = False) -> None:
        scheduler.start(paused=paused)

    @classmethod
    async def shutdown(cls, wait: bool = False):
        return scheduler.shutdown(wait=wait)

    @classmethod
    def pause(cls) -> None:
        scheduler.pause()

    @classmethod
    def resume(cls) -> None:
        scheduler.resume()

    @classmethod
    def is_running(cls) -> bool:
        return scheduler.running

    @classmethod
    def get_scheduler_state(cls) -> str:
        if scheduler.state == 0:
            return "停止"
        if scheduler.state == 1:
            return "运行中"
        if scheduler.state == 2:
            return "暂停"
        return "未知"

    @classmethod
    def get_job(cls, job_id: str | int, jobstore: str | None = None) -> Job | None:
        return scheduler.get_job(str(job_id), jobstore)

    @classmethod
    def get_jobs(cls, jobstore: str | None = None) -> list[Job]:
        return scheduler.get_jobs(jobstore)

    @classmethod
    def get_all_jobs(cls) -> list[Job]:
        return scheduler.get_jobs()

    @classmethod
    def add_job(
        cls,
        func: Callable,
        trigger,
        job_id: str | None = None,
        name: str | None = None,
        args: list | None = None,
        kwargs: dict | None = None,
        jobstore: str = "default",
        executor: str = "default",
        **options,
    ) -> Job:
        """
        添加任务到调度器

        参数:
        - func: 任务执行函数
        - trigger: 触发器
        - job_id: 任务ID
        - name: 任务名称
        - args: 位置参数
        - kwargs: 关键字参数
        - jobstore: 存储器
        - executor: 执行器
        """
        job = scheduler.add_job(
            func=func,
            trigger=trigger,
            id=str(job_id) if job_id else None,
            name=name,
            args=args or [],
            kwargs=kwargs or {},
            jobstore=jobstore,
            executor=executor,
            **options,
        )
        log.info(f"任务 {job_id} 添加成功")
        return job

    @classmethod
    def remove_job(cls, job_id: str | int, jobstore: str | None = None) -> None:
        scheduler.remove_job(str(job_id), jobstore)
        log.info(f"任务 {job_id} 已移除")

    @classmethod
    def clear_jobs(cls) -> None:
        scheduler.remove_all_jobs()
        log.info("所有任务已清空")

    @classmethod
    def pause_job(cls, job_id: str | int, jobstore: str | None = None) -> Job | None:
        job = scheduler.pause_job(str(job_id), jobstore)
        log.info(f"任务 {job_id} 已暂停")
        return job

    @classmethod
    def resume_job(cls, job_id: str | int, jobstore: str | None = None) -> Job | None:
        job = scheduler.resume_job(str(job_id), jobstore)
        log.info(f"任务 {job_id} 已恢复")
        return job

    @classmethod
    def modify_job(cls, job_id: str | int, jobstore: str | None = None, **changes) -> Job | None:
        return scheduler.modify_job(str(job_id), jobstore, **changes)

    @classmethod
    def add_cron_job(
        cls,
        func: Callable,
        cron_expr: str,
        job_id: str | None = None,
        name: str | None = None,
        args: list | None = None,
        kwargs: dict | None = None,
        jobstore: str = "default",
        executor: str = "default",
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Job:
        """
        创建Cron定时任务

        参数:
        - func: 任务执行函数
        - cron_expr: Cron表达式 (秒 分 时 日 月 周 年)
        - job_id: 任务ID
        - name: 任务名称
        - args: 位置参数
        - kwargs: 关键字参数
        - jobstore: 存储器
        - executor: 执行器
        - start_date: 开始时间
        - end_date: 结束时间
        """
        if not cron_expr:
            raise ValueError("Cron触发器缺少参数")

        fields = cron_expr.strip().split()
        if len(fields) not in (6, 7):
            raise ValueError("无效的 Cron 表达式")
        if not CronUtil.validate_cron_expression(cron_expr):
            raise ValueError(f"Cron表达式不正确: {cron_expr}")

        parsed_fields = [field if field != "?" else "*" for field in fields]
        if len(fields) == 6:
            parsed_fields.append("*")

        second, minute, hour, day, month, day_of_week, year = tuple(parsed_fields)

        if (
            second == "*"
            and minute == "*"
            and hour == "*"
            and day == "*"
            and month == "*"
            and day_of_week in ("*", "?")
        ):
            raise ValueError("Cron表达式不允许每秒执行，请至少指定秒数")

        trigger = CronTrigger(
            second=second,
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week,
            year=year,
            start_date=start_date,
            end_date=end_date,
            timezone="Asia/Shanghai",
        )
        return cls.add_job(
            func=func,
            trigger=trigger,
            job_id=job_id,
            name=name,
            args=args,
            kwargs=kwargs,
            jobstore=jobstore,
            executor=executor,
        )

    @classmethod
    def add_interval_job(
        cls,
        func: Callable,
        interval_args: str,
        job_id: str | None = None,
        name: str | None = None,
        args: list | None = None,
        kwargs: dict | None = None,
        jobstore: str = "default",
        executor: str = "default",
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        **options,
    ) -> Job:
        """
        创建间隔执行任务

        参数:
        - func: 任务执行函数
        - interval_args: 间隔参数 (秒 分 时 天 周)
        - job_id: 任务ID
        - name: 任务名称
        - args: 位置参数
        - kwargs: 关键字参数
        - jobstore: 存储器
        - executor: 执行器
        - start_date: 开始时间
        - end_date: 结束时间
        """
        if not interval_args:
            raise ValueError("interval触发器缺少参数")

        fields = interval_args.strip().split()
        if len(fields) != 5:
            raise ValueError("无效的 interval 表达式，格式: 秒 分 时 天 周")

        second, minute, hour, day, week = tuple(
            int(field) if field != "*" else 0 for field in fields
        )
        trigger = IntervalTrigger(
            weeks=week,
            days=day,
            hours=hour,
            minutes=minute,
            seconds=second,
            start_date=start_date,
            end_date=end_date,
            timezone="Asia/Shanghai",
        )
        return cls.add_job(
            func=func,
            trigger=trigger,
            job_id=job_id,
            name=name,
            args=args,
            kwargs=kwargs,
            jobstore=jobstore,
            executor=executor,
            **options,
        )

    @classmethod
    def add_date_job(
        cls,
        func: Callable,
        run_date: datetime | str,
        job_id: str | None = None,
        name: str | None = None,
        args: list | None = None,
        kwargs: dict | None = None,
        jobstore: str = "default",
        executor: str = "default",
    ) -> Job:
        """
        创建指定时间执行任务

        参数:
        - func: 任务执行函数
        - run_date: 执行时间
        - job_id: 任务ID
        - name: 任务名称
        - args: 位置参数
        - kwargs: 关键字参数
        - jobstore: 存储器
        - executor: 执行器
        """
        trigger = DateTrigger(run_date=run_date, timezone="Asia/Shanghai")
        return cls.add_job(
            func=func,
            trigger=trigger,
            job_id=job_id,
            name=name,
            args=args,
            kwargs=kwargs,
            jobstore=jobstore,
            executor=executor,
        )

    @classmethod
    def run_job_now(cls, job_id: str | int, jobstore: str | None = None) -> Job | None:
        """
        立即执行任务（创建临时任务）

        参数:
        - job_id: 任务ID
        - jobstore: 存储器
        """
        job = cls.get_job(job_id=job_id, jobstore=jobstore)
        if not job:
            return None

        temp_job_id = f"{job_id}_run_now_{datetime.now().timestamp()}"
        trigger = DateTrigger(run_date=datetime.now(), timezone="Asia/Shanghai")
        temp_job = scheduler.add_job(
            func=job.func,
            trigger=trigger,
            args=job.args,
            kwargs=job.kwargs,
            id=temp_job_id,
            name=f"{job.name}(立即执行)",
            jobstore=jobstore or "default",
            executor=job.executor,
            max_instances=1,
        )
        log.info(f"任务 {job_id} 已触发立即执行，临时任务 ID: {temp_job_id}")
        return temp_job

    @classmethod
    def get_job_status(cls, job_id: str | int) -> str:
        """
        获取单个任务的当前状态。
        """
        job = cls.get_job(job_id=str(job_id))
        if not job:
            return "未知"

        if job.next_run_time is None:
            return "暂停中"

        if scheduler.state == 0:
            return "已停止"

        return "运行中"

    @classmethod
    def print_jobs(cls, jobstore: str | None = None) -> str:
        """
        打印调度器任务信息

        参数:
        - jobstore: 存储器别名，None 表示所有存储器

        返回:
        - str: 格式化的任务信息
        """
        import io

        output = io.StringIO()
        scheduler.print_jobs(jobstore=jobstore, out=output)
        return output.getvalue()
