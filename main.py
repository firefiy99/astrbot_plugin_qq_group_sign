"""AstrBot QQ 群自动签到插件。

创作者：小星萤
仅支持提供 NapCat 群打卡扩展接口的 aiocqhttp 适配器。
"""

import asyncio
import inspect
import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.star import Context, Star
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

PLUGIN_NAME = "astrbot_plugin_qq_group_sign"
DATA_DIR = Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME
STATE_PATH = DATA_DIR / "state.json"


class QQGroupSignPlugin(Star):
    """每天在白名单 QQ 群中调用 NapCat 群打卡接口。"""

    def __init__(self, context: Context, config: Optional[AstrBotConfig] = None):
        super().__init__(context)
        self.config = config or {}
        self.enabled = bool(self.config.get("enabled", True))
        self.platform_id = str(self.config.get("platform_id", "") or "").strip()
        self.sign_hour = self._bounded_int("sign_hour", 8, 0, 23)
        self.sign_minute = self._bounded_int("sign_minute", 0, 0, 59)
        self.retry_window_minutes = self._bounded_int(
            "retry_window_minutes", 10, 0, 120
        )
        self.group_whitelist = self._parse_group_ids(
            self.config.get("group_whitelist", "")
        )
        self._bot: Any = None
        self._scheduler_task: Optional[asyncio.Task] = None
        self._closed = False
        self._state_lock = asyncio.Lock()
        self._run_lock = asyncio.Lock()
        self._state: dict[str, Any] = {"groups": {}}

    def _bounded_int(self, key: str, default: int, minimum: int, maximum: int) -> int:
        try:
            value = int(self.config.get(key, default))
        except (TypeError, ValueError):
            value = default
        return max(minimum, min(maximum, value))

    @staticmethod
    def _parse_group_ids(value: object) -> set[str]:
        """接受英文逗号、中文逗号、分号、空格或换行分隔的群号。"""
        return {item for item in re.findall(r"\d+", str(value or "")) if item}

    async def initialize(self):
        if self._scheduler_task and not self._scheduler_task.done():
            return
        self._closed = False
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        await self._load_state()
        self._bot = await self._configured_bot()
        self._scheduler_task = asyncio.create_task(
            self._scheduler_loop(), name="qq_group_sign_scheduler"
        )
        if not self.group_whitelist:
            logger.warning(
                "[qq_group_sign] 群白名单为空，出于安全考虑不会对任何群执行签到"
            )
        logger.info(
            f"[qq_group_sign] 插件已启动，签到时间 "
            f"{self.sign_hour:02d}:{self.sign_minute:02d}，"
            f"白名单群数 {len(self.group_whitelist)}"
        )

    async def terminate(self):
        self._closed = True
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
        self._scheduler_task = None
        self._bot = None
        logger.info("[qq_group_sign] 插件已停止")

    async def _load_state(self):
        if not STATE_PATH.is_file():
            return
        try:
            data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("groups"), dict):
                groups = {
                    str(group_id): record
                    for group_id, record in data["groups"].items()
                    if isinstance(record, dict)
                }
                self._state = {"groups": groups}
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning(f"[qq_group_sign] 状态文件读取失败，将使用空状态: {exc}")

    async def _save_state(self):
        async with self._state_lock:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            temp_path = STATE_PATH.with_suffix(".tmp")
            content = json.dumps(self._state, ensure_ascii=False, indent=2)
            try:
                temp_path.write_text(content, encoding="utf-8")
                temp_path.replace(STATE_PATH)
            except OSError as exc:
                logger.error(f"[qq_group_sign] 状态文件保存失败: {exc}")

    @staticmethod
    async def _platform_client(platform) -> Any:
        """从 AstrBot 平台实例中提取 aiocqhttp 客户端。"""
        if platform is None:
            return None
        meta = getattr(platform, "meta", None)
        try:
            meta_obj = meta() if callable(meta) else meta
            if isinstance(meta_obj, dict):
                platform_name = str(meta_obj.get("name", "") or "").lower()
            else:
                platform_name = str(getattr(meta_obj, "name", "") or "").lower()
        except Exception:
            platform_name = ""
        if platform_name and platform_name != "aiocqhttp":
            return None
        get_client = getattr(platform, "get_client", None)
        client = get_client() if callable(get_client) else None
        if inspect.isawaitable(client):
            client = await client
        return client or getattr(platform, "bot", None)

    async def _configured_bot(self) -> Any:
        """按配置选择 QQ 平台；留空时使用第一个 aiocqhttp 平台。"""
        if self.platform_id:
            platform = self.context.get_platform_inst(self.platform_id)
            if inspect.isawaitable(platform):
                platform = await platform
            client = await self._platform_client(platform)
            if client is None:
                logger.error(
                    f"[qq_group_sign] 平台 {self.platform_id} 不存在或不是 aiocqhttp"
                )
            return client
        manager = getattr(self.context, "platform_manager", None)
        get_insts = getattr(manager, "get_insts", None)
        platforms = get_insts() if callable(get_insts) else []
        if inspect.isawaitable(platforms):
            platforms = await platforms
        for platform in platforms or []:
            client = await self._platform_client(platform)
            if client is not None:
                return client
        return None

    async def _bot_for_event(self, event: AstrMessageEvent) -> Any:
        """手动指令优先使用指定平台，否则使用指令所在平台。"""
        if self.platform_id:
            return await self._configured_bot()
        platform = self.context.get_platform_inst(event.get_platform_id())
        if inspect.isawaitable(platform):
            platform = await platform
        return await self._platform_client(platform)

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.event_message_type(filter.EventMessageType.ALL)
    async def remember_aiocqhttp_bot(self, event: AstrMessageEvent):
        """平台启动较晚时，从消息事件刷新定时任务使用的客户端。"""
        if self._bot is None:
            self._bot = await self._bot_for_event(event)

    async def _scheduler_loop(self):
        while not self._closed:
            try:
                await self._scheduled_tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception(
                    f"[qq_group_sign] 定时检查异常，将在下一分钟重试: {exc}"
                )
            now = datetime.now()
            await asyncio.sleep(max(1, 60 - now.second))

    async def _scheduled_tick(self):
        if not self.enabled or not self.group_whitelist:
            return
        now = datetime.now()
        target = now.replace(
            hour=self.sign_hour, minute=self.sign_minute, second=0, microsecond=0
        )
        # 签到时间接近午夜时，重试窗口可能延续到次日。
        if now < target:
            previous_target = target - timedelta(days=1)
            if now <= previous_target + timedelta(
                minutes=self.retry_window_minutes, seconds=59
            ):
                target = previous_target
            else:
                return
        if now > target + timedelta(minutes=self.retry_window_minutes, seconds=59):
            return
        await self._sign_whitelist(
            manual=False, schedule_date=target.strftime("%Y-%m-%d")
        )

    def _signed_today(self, group_id: str, today: str) -> bool:
        record = self._state.get("groups", {}).get(group_id, {})
        return record.get("success_date") == today

    async def _sign_whitelist(
        self, manual: bool, schedule_date: Optional[str] = None
    ) -> dict[str, tuple[bool, str]]:
        """为白名单群签到；自动执行时按计划日期避免重复。"""
        async with self._run_lock:
            today = schedule_date or datetime.now().strftime("%Y-%m-%d")
            results: dict[str, tuple[bool, str]] = {}
            for group_id in sorted(self.group_whitelist, key=int):
                if not manual and self._signed_today(group_id, today):
                    results[group_id] = (True, "今日已成功，已跳过")
                    continue
                ok, message = await self._call_group_sign(group_id)
                results[group_id] = (ok, message)
                now_text = datetime.now().isoformat(sep=" ", timespec="seconds")
                record = self._state.setdefault("groups", {}).setdefault(group_id, {})
                record["last_attempt_at"] = now_text
                record["last_success"] = ok
                record["last_message"] = message[:500]
                if ok:
                    record["success_date"] = today
                    record["last_success_at"] = now_text
                await self._save_state()
                if ok:
                    logger.info(f"[qq_group_sign] 群 {group_id} 签到成功: {message}")
                else:
                    logger.warning(f"[qq_group_sign] 群 {group_id} 签到失败: {message}")
            return results

    async def _call_group_sign(self, group_id: str) -> tuple[bool, str]:
        if self._bot is None:
            self._bot = await self._configured_bot()
        if self._bot is None:
            return False, "未找到可用的 aiocqhttp/NapCat 客户端，请检查 platform_id 和平台连接状态"

        gid: Any = int(group_id) if group_id.isdigit() else group_id
        errors: list[str] = []

        # NapCat 同时保留 set_group_sign 与 send_group_sign 两个动作名。
        call_action = getattr(self._bot, "call_action", None)
        if callable(call_action):
            for action in ("set_group_sign", "send_group_sign"):
                try:
                    result = call_action(action, group_id=gid)
                    if inspect.isawaitable(result):
                        result = await result
                    ok, message = self._parse_action_result(result)
                    if ok:
                        return True, f"{action}: {message}"
                    errors.append(f"{action}: {message}")
                except Exception as exc:
                    errors.append(f"{action}: {exc}")

        # 某些客户端会把 OneBot action 暴露为同名方法。
        for action in ("set_group_sign", "send_group_sign"):
            method = getattr(self._bot, action, None)
            if not callable(method):
                continue
            try:
                try:
                    result = method(group_id=gid)
                except TypeError:
                    result = method(gid)
                if inspect.isawaitable(result):
                    result = await result
                ok, message = self._parse_action_result(result)
                if ok:
                    return True, f"{action}: {message}"
                errors.append(f"{action}: {message}")
            except Exception as exc:
                errors.append(f"{action}: {exc}")

        if not errors:
            return False, "当前 QQ 适配器未暴露 NapCat 群打卡接口"
        return False, "；".join(errors)

    @staticmethod
    def _parse_action_result(result: Any) -> tuple[bool, str]:
        """兼容返回 None、字典或对象形式的 OneBot 响应。"""
        if result is None:
            return True, "调用完成"
        if isinstance(result, bool):
            return result, "调用成功" if result else "接口返回 False"
        if isinstance(result, dict):
            status = str(result.get("status", "")).lower()
            retcode = result.get("retcode", 0)
            if status == "failed" or retcode not in (None, 0, "0"):
                message = (
                    result.get("wording")
                    or result.get("message")
                    or f"retcode={retcode}"
                )
                return False, str(message)
            return True, str(result.get("message") or "调用成功")
        status = str(getattr(result, "status", "")).lower()
        retcode = getattr(result, "retcode", 0)
        if status == "failed" or retcode not in (None, 0, "0"):
            message = (
                getattr(result, "wording", "")
                or getattr(result, "message", "")
                or f"retcode={retcode}"
            )
            return False, str(message)
        return True, "调用成功"

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("群签到状态", alias={"自动群签到状态"})
    async def sign_status(self, event: AstrMessageEvent):
        """查看自动群签到配置及最近执行结果。"""
        today = datetime.now().strftime("%Y-%m-%d")
        lines = [
            "🗓️ QQ 群自动签到状态",
            f"总开关：{'开启' if self.enabled else '关闭'}",
            f"执行时间：{self.sign_hour:02d}:{self.sign_minute:02d}",
            f"失败重试窗口：{self.retry_window_minutes} 分钟",
            f"白名单：{', '.join(sorted(self.group_whitelist, key=int)) or '空'}",
            f"QQ 客户端：{'已就绪' if self._bot is not None else '尚未获取'}",
            "",
            "今日结果：",
        ]
        if not self.group_whitelist:
            lines.append("· 无白名单群")
        for group_id in sorted(self.group_whitelist, key=int):
            record = self._state.get("groups", {}).get(group_id, {})
            if record.get("success_date") == today:
                lines.append(f"· {group_id}：✅ 已签到")
            elif record.get("last_attempt_at", "").startswith(today):
                lines.append(
                    f"· {group_id}：❌ {record.get('last_message', '执行失败')}"
                )
            else:
                lines.append(f"· {group_id}：⏳ 今日尚未执行")
        yield event.plain_result("\n".join(lines))

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("群签到", alias={"手动群签到"})
    async def sign_current_group(self, event: AstrMessageEvent):
        """手动为指令所在的白名单群执行签到，并返回平台结果。"""
        group_id = str(event.get_group_id() or "")
        if not group_id:
            yield event.plain_result("⚠️ /群签到 只能在 QQ 群聊中使用")
            return
        if group_id not in self.group_whitelist:
            yield event.plain_result(
                f"⚠️ 当前群 {group_id} 不在自动签到白名单中，未执行签到"
            )
            return

        bot = await self._bot_for_event(event)
        if bot is not None:
            self._bot = bot
        async with self._run_lock:
            ok, message = await self._call_group_sign(group_id)
            now_text = datetime.now().isoformat(sep=" ", timespec="seconds")
            record = self._state.setdefault("groups", {}).setdefault(group_id, {})
            record["last_attempt_at"] = now_text
            record["last_success"] = ok
            record["last_message"] = message[:500]
            if ok:
                record["success_date"] = datetime.now().strftime("%Y-%m-%d")
                record["last_success_at"] = now_text
            await self._save_state()

        if ok:
            logger.info(f"[qq_group_sign] 管理员手动为群 {group_id} 签到成功: {message}")
            yield event.plain_result(
                "✅ QQ 群签到成功\n"
                f"群号：{group_id}\n"
                f"接口结果：{message}"
            )
        else:
            logger.warning(f"[qq_group_sign] 管理员手动为群 {group_id} 签到失败: {message}")
            yield event.plain_result(
                "❌ QQ 群签到失败\n"
                f"群号：{group_id}\n"
                f"失败原因：{message}"
            )

    @filter.platform_adapter_type(filter.PlatformAdapterType.AIOCQHTTP)
    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("立即群签到", alias={"测试群签到"})
    async def sign_now(self, event: AstrMessageEvent):
        """立即为全部白名单群执行一次签到，用于测试配置。"""
        if not self.group_whitelist:
            yield event.plain_result("⚠️ 群白名单为空，未执行任何签到")
            return
        bot = await self._bot_for_event(event)
        if bot is not None:
            self._bot = bot
        results = await self._sign_whitelist(manual=True)
        lines = ["🗓️ 手动群签到结果"]
        for group_id, (ok, message) in results.items():
            lines.append(f"· {group_id}：{'✅' if ok else '❌'} {message}")
        yield event.plain_result("\n".join(lines))

    @filter.command("群签到帮助")
    async def sign_help(self, event: AstrMessageEvent):
        """显示插件帮助。"""
        yield event.plain_result(
            "📖 QQ 群自动签到\n"
            "每天在设定时间为白名单 QQ 群执行群打卡。\n\n"
            "/群签到 - 管理员为当前白名单群手动签到\n"
            "/群签到状态 - 管理员查看配置和结果\n"
            "/立即群签到 - 管理员立即测试全部白名单群\n"
            "/群签到帮助 - 显示本帮助\n\n"
            "请在 AstrBot WebUI 插件配置中设置时间和群白名单。\n"
            "仅支持 aiocqhttp + NapCat，QQ 官方机器人不支持此接口。"
        )
