"""DingTalk/DingDing channel implementation using Stream Mode."""

import asyncio
import json
import mimetypes
import os
import time
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import httpx
from loguru import logger

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import DingTalkConfig

try:
    from dingtalk_stream import (
        AckMessage,
        CallbackHandler,
        CallbackMessage,
        Credential,
        DingTalkStreamClient,
    )
    from dingtalk_stream.chatbot import ChatbotMessage

    DINGTALK_AVAILABLE = True
except ImportError:
    DINGTALK_AVAILABLE = False
    # Fallback so class definitions don't crash at module level
    CallbackHandler = object  # type: ignore[assignment,misc]
    CallbackMessage = None  # type: ignore[assignment,misc]
    AckMessage = None  # type: ignore[assignment,misc]
    ChatbotMessage = None  # type: ignore[assignment,misc]


class NanobotDingTalkHandler(CallbackHandler):
    """
    Standard DingTalk Stream SDK Callback Handler.
    Parses incoming messages and forwards them to the Nanobot channel.
    """

    def __init__(self, channel: "DingTalkChannel"):
        super().__init__()
        self.channel = channel

    async def process(self, message: CallbackMessage):
        """Process incoming stream message with support for files and media."""
        try:
            chatbot_msg = ChatbotMessage.from_dict(message.data)

            sender_id = chatbot_msg.sender_staff_id or chatbot_msg.sender_id
            sender_name = chatbot_msg.sender_nick or "Unknown"

            conversation_type = message.data.get("conversationType")
            conversation_id = (
                message.data.get("conversationId")
                or message.data.get("openConversationId")
            )

            # Handle different message types
            content = ""
            media_paths = []

            msg_type = chatbot_msg.message_type

            if msg_type == "text":
                # Existing text handling
                content = chatbot_msg.text.content.strip() if chatbot_msg.text else ""
                if not content:
                    content = message.data.get("text", {}).get("content", "").strip()

            elif msg_type in ("picture", "image"):
                # Handle image messages
                content = "[Image sent]"
                # Extract download code from message data
                image_dict = message.data.get("content", {}) or message.data.get("image", {})
                download_code = image_dict.get("downloadCode") or image_dict.get("download_code")
                if download_code:
                    file_path, content_text = await self.channel._download_and_save_media(
                        "image", {"download_code": download_code}, conversation_id
                    )
                    if file_path:
                        media_paths.append(file_path)
                    if content_text:
                        content = content_text

            elif msg_type == "richText":
                # Handle rich text with embedded images
                content_parts = []
                rich_text_content = message.data.get("content", {}) or message.data.get("richText", {})
                rich_text_list = rich_text_content.get("richTextList") or rich_text_content.get("rich_text_list", [])

                for item in rich_text_list:
                    if isinstance(item, dict):
                        text = item.get("text")
                        if text:
                            content_parts.append(text)
                        # Check for embedded images
                        image_dict = item.get("image", {})
                        download_code = image_dict.get("downloadCode") or image_dict.get("download_code")
                        if download_code:
                            file_path, _ = await self.channel._download_and_save_media(
                                "image", {"download_code": download_code}, conversation_id
                            )
                            if file_path:
                                media_paths.append(file_path)
                                content_parts.append(f"[Image: {os.path.basename(file_path)}]")

                content = "\n".join(content_parts) if content_parts else "[Rich text message]"

            elif msg_type == "file":
                # Handle file attachments
                file_dict = message.data.get("content", {}) or message.data.get("file", {})
                download_code = file_dict.get("downloadCode") or file_dict.get("download_code")
                filename = file_dict.get("fileName") or file_dict.get("filename", "")

                if download_code:
                    file_path, content_text = await self.channel._download_and_save_media(
                        "file", {"download_code": download_code, "filename": filename}, conversation_id
                    )
                    if file_path:
                        media_paths.append(file_path)
                    content = content_text or f"[File: {filename}]"
                else:
                    content = f"[File: {filename or 'unknown'}]"

            elif msg_type in ("audio", "video"):
                # Handle audio/video messages
                media_dict = message.data.get("content", {}) or message.data.get(msg_type, {})
                download_code = media_dict.get("downloadCode") or media_dict.get("download_code")
                duration = media_dict.get("duration")

                if download_code:
                    file_path, content_text = await self.channel._download_and_save_media(
                        msg_type, {"download_code": download_code}, conversation_id
                    )
                    if file_path:
                        media_paths.append(file_path)
                    content = content_text or f"[{msg_type.capitalize()} message"
                    if duration:
                        content += f" ({duration}s)"
                    content += "]"
                else:
                    content = f"[{msg_type.capitalize()} message]"

            else:
                # Unknown or unsupported type
                logger.warning(
                    "Received unsupported message type: {} from {}",
                    msg_type, sender_name
                )
                return AckMessage.STATUS_OK, "OK"

            # Check for DingTalk Docs links in text content
            if content and "dingtalk.com" in content.lower():
                docs_links = self._extract_dingtalk_docs_links(content)
                if docs_links:
                    # Add metadata about docs links
                    logger.info("Found {} DingTalk Docs links", len(docs_links))

            # Log the received message
            logger.info(
                "Received DingTalk {} message from {} ({}): {} with {} media files",
                msg_type, sender_name, sender_id,
                content[:100] if content else "[no text]",
                len(media_paths)
            )

            # Forward to Nanobot via _on_message
            task = asyncio.create_task(
                self.channel._on_message(
                    content,
                    sender_id,
                    sender_name,
                    conversation_type,
                    conversation_id,
                    media_paths,
                )
            )
            self.channel._background_tasks.add(task)
            task.add_done_callback(self.channel._background_tasks.discard)

            return AckMessage.STATUS_OK, "OK"

        except Exception as e:
            logger.error("Error processing DingTalk message: {}", e)
            # Return OK to avoid retry loop from DingTalk server
            return AckMessage.STATUS_OK, "Error"

    def _extract_dingtalk_docs_links(self, content: str) -> list[str]:
        """Extract DingTalk Docs URLs from message content."""
        import re
        # Match dingtalk.com URLs (docs or other DingTalk URLs)
        pattern = r'https?://[^\s]*dingtalk\.com/[^\s]*'
        return re.findall(pattern, content)


class DingTalkChannel(BaseChannel):
    """
    DingTalk channel using Stream Mode.

    Uses WebSocket to receive events via `dingtalk-stream` SDK.
    Uses direct HTTP API to send messages (SDK is mainly for receiving).

    Supports both private (1:1) and group chats.
    Group chat_id is stored with a "group:" prefix to route replies back.
    """

    name = "dingtalk"
    _IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp"}
    _AUDIO_EXTS = {".amr", ".mp3", ".wav", ".ogg", ".m4a", ".aac"}
    _VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}

    def __init__(self, config: DingTalkConfig, bus: MessageBus):
        super().__init__(config, bus)
        self.config: DingTalkConfig = config
        self._client: Any = None
        self._http: httpx.AsyncClient | None = None

        # Access Token management for sending messages
        self._access_token: str | None = None
        self._token_expiry: float = 0

        # Hold references to background tasks to prevent GC
        self._background_tasks: set[asyncio.Task] = set()

    async def start(self) -> None:
        """Start the DingTalk bot with Stream Mode."""
        try:
            if not DINGTALK_AVAILABLE:
                logger.error(
                    "DingTalk Stream SDK not installed. Run: pip install dingtalk-stream"
                )
                return

            if not self.config.client_id or not self.config.client_secret:
                logger.error("DingTalk client_id and client_secret not configured")
                return

            self._running = True
            self._http = httpx.AsyncClient()

            logger.info(
                "Initializing DingTalk Stream Client with Client ID: {}...",
                self.config.client_id,
            )
            credential = Credential(self.config.client_id, self.config.client_secret)
            self._client = DingTalkStreamClient(credential)

            # Register standard handler
            handler = NanobotDingTalkHandler(self)
            self._client.register_callback_handler(ChatbotMessage.TOPIC, handler)

            logger.info("DingTalk bot started with Stream Mode")

            # Reconnect loop: restart stream if SDK exits or crashes
            while self._running:
                try:
                    await self._client.start()
                except Exception as e:
                    logger.warning("DingTalk stream error: {}", e)
                if self._running:
                    logger.info("Reconnecting DingTalk stream in 5 seconds...")
                    await asyncio.sleep(5)

        except Exception as e:
            logger.exception("Failed to start DingTalk channel: {}", e)

    async def stop(self) -> None:
        """Stop the DingTalk bot."""
        self._running = False
        # Close the shared HTTP client
        if self._http:
            await self._http.aclose()
            self._http = None
        # Cancel outstanding background tasks
        for task in self._background_tasks:
            task.cancel()
        self._background_tasks.clear()

    async def _get_access_token(self) -> str | None:
        """Get or refresh Access Token."""
        if self._access_token and time.time() < self._token_expiry:
            return self._access_token

        url = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
        data = {
            "appKey": self.config.client_id,
            "appSecret": self.config.client_secret,
        }

        if not self._http:
            logger.warning("DingTalk HTTP client not initialized, cannot refresh token")
            return None

        try:
            resp = await self._http.post(url, json=data)
            resp.raise_for_status()
            res_data = resp.json()
            self._access_token = res_data.get("accessToken")
            # Expire 60s early to be safe
            self._token_expiry = time.time() + int(res_data.get("expireIn", 7200)) - 60
            return self._access_token
        except Exception as e:
            logger.error("Failed to get DingTalk access token: {}", e)
            return None

    @staticmethod
    def _is_http_url(value: str) -> bool:
        return urlparse(value).scheme in ("http", "https")

    def _guess_upload_type(self, media_ref: str) -> str:
        ext = Path(urlparse(media_ref).path).suffix.lower()
        if ext in self._IMAGE_EXTS: return "image"
        if ext in self._AUDIO_EXTS: return "voice"
        if ext in self._VIDEO_EXTS: return "video"
        return "lfile"

    def _guess_filename(self, media_ref: str, upload_type: str) -> str:
        name = os.path.basename(urlparse(media_ref).path)
        return name or {"image": "image.jpg", "voice": "audio.amr", "video": "video.mp4"}.get(upload_type, "file.bin")

    async def _read_media_bytes(
        self,
        media_ref: str,
    ) -> tuple[bytes | None, str | None, str | None]:
        if not media_ref:
            return None, None, None

        if self._is_http_url(media_ref):
            if not self._http:
                return None, None, None
            try:
                resp = await self._http.get(media_ref, follow_redirects=True)
                if resp.status_code >= 400:
                    logger.warning(
                        "DingTalk media download failed status={} ref={}",
                        resp.status_code,
                        media_ref,
                    )
                    return None, None, None
                content_type = (resp.headers.get("content-type") or "").split(";")[0].strip()
                filename = self._guess_filename(media_ref, self._guess_upload_type(media_ref))
                return resp.content, filename, content_type or None
            except Exception as e:
                logger.error("DingTalk media download error ref={} err={}", media_ref, e)
                return None, None, None

        try:
            if media_ref.startswith("file://"):
                parsed = urlparse(media_ref)
                local_path = Path(unquote(parsed.path))
            else:
                local_path = Path(os.path.expanduser(media_ref))
            if not local_path.is_file():
                logger.warning("DingTalk media file not found: {}", local_path)
                return None, None, None
            data = await asyncio.to_thread(local_path.read_bytes)
            content_type = mimetypes.guess_type(local_path.name)[0]
            return data, local_path.name, content_type
        except Exception as e:
            logger.error("DingTalk media read error ref={} err={}", media_ref, e)
            return None, None, None

    async def _upload_media(
        self,
        token: str,
        data: bytes,
        media_type: str,
        filename: str,
        content_type: str | None,
    ) -> str | None:
        if not self._http:
            return None
        url = f"https://oapi.dingtalk.com/media/upload?access_token={token}&type={media_type}"
        mime = content_type or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        files = {"media": (filename, data, mime)}

        try:
            resp = await self._http.post(url, files=files)
            text = resp.text
            result = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
            if resp.status_code >= 400:
                logger.error("DingTalk media upload failed status={} type={} body={}", resp.status_code, media_type, text[:500])
                return None
            errcode = result.get("errcode", 0)
            if errcode != 0:
                logger.error("DingTalk media upload api error type={} errcode={} body={}", media_type, errcode, text[:500])
                return None
            sub = result.get("result") or {}
            media_id = result.get("media_id") or result.get("mediaId") or sub.get("media_id") or sub.get("mediaId")
            if not media_id:
                logger.error("DingTalk media upload missing media_id body={}", text[:500])
                return None
            return str(media_id)
        except Exception as e:
            logger.error("DingTalk media upload error type={} err={}", media_type, e)
            return None

    async def _send_batch_message(
        self,
        token: str,
        chat_id: str,
        msg_key: str,
        msg_param: dict[str, Any],
    ) -> bool:
        if not self._http:
            logger.warning("DingTalk HTTP client not initialized, cannot send")
            return False

        headers = {"x-acs-dingtalk-access-token": token}
        if chat_id.startswith("group:"):
            # Group chat
            url = "https://api.dingtalk.com/v1.0/robot/groupMessages/send"
            payload = {
                "robotCode": self.config.client_id,
                "openConversationId": chat_id[6:],  # Remove "group:" prefix,
                "msgKey": msg_key,
                "msgParam": json.dumps(msg_param, ensure_ascii=False),
            }
        else:
            # Private chat
            url = "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend"
            payload = {
                "robotCode": self.config.client_id,
                "userIds": [chat_id],
                "msgKey": msg_key,
                "msgParam": json.dumps(msg_param, ensure_ascii=False),
            }

        try:
            resp = await self._http.post(url, json=payload, headers=headers)
            body = resp.text
            if resp.status_code != 200:
                logger.error("DingTalk send failed msgKey={} status={} body={}", msg_key, resp.status_code, body[:500])
                return False
            try: result = resp.json()
            except Exception: result = {}
            errcode = result.get("errcode")
            if errcode not in (None, 0):
                logger.error("DingTalk send api error msgKey={} errcode={} body={}", msg_key, errcode, body[:500])
                return False
            logger.debug("DingTalk message sent to {} with msgKey={}", chat_id, msg_key)
            return True
        except Exception as e:
            logger.error("Error sending DingTalk message msgKey={} err={}", msg_key, e)
            return False

    async def _send_markdown_text(self, token: str, chat_id: str, content: str) -> bool:
        return await self._send_batch_message(
            token,
            chat_id,
            "sampleMarkdown",
            {"text": content, "title": "Nanobot Reply"},
        )

    async def _send_media_ref(self, token: str, chat_id: str, media_ref: str) -> bool:
        media_ref = (media_ref or "").strip()
        if not media_ref:
            return True

        upload_type = self._guess_upload_type(media_ref)
        if upload_type == "image" and self._is_http_url(media_ref):
            ok = await self._send_batch_message(
                token,
                chat_id,
                "sampleImageMsg",
                {"photoURL": media_ref},
            )
            if ok:
                return True
            logger.warning("DingTalk image url send failed, trying upload fallback: {}", media_ref)

        data, filename, content_type = await self._read_media_bytes(media_ref)
        if not data:
            logger.error("DingTalk media read failed: {}", media_ref)
            return False

        filename = filename or self._guess_filename(media_ref, upload_type)
        file_type = Path(filename).suffix.lower().lstrip(".")
        if not file_type:
            guessed = mimetypes.guess_extension(content_type or "")
            file_type = (guessed or ".bin").lstrip(".")
        if file_type == "jpeg":
            file_type = "jpg"

        media_id = await self._upload_media(
            token=token,
            data=data,
            media_type=upload_type,
            filename=filename,
            content_type=content_type,
        )
        if not media_id:
            return False

        if upload_type == "image":
            # Verified in production: sampleImageMsg accepts media_id in photoURL.
            ok = await self._send_batch_message(
                token,
                chat_id,
                "sampleImageMsg",
                {"photoURL": media_id},
            )
            if ok:
                return True
            logger.warning("DingTalk image media_id send failed, falling back to file: {}", media_ref)

        return await self._send_batch_message(
            token,
            chat_id,
            "sampleFile",
            {"mediaId": media_id, "fileName": filename, "fileType": file_type},
        )

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through DingTalk."""
        token = await self._get_access_token()
        if not token:
            return

        if msg.content and msg.content.strip():
            await self._send_markdown_text(token, msg.chat_id, msg.content.strip())

        for media_ref in msg.media or []:
            ok = await self._send_media_ref(token, msg.chat_id, media_ref)
            if ok:
                continue
            logger.error("DingTalk media send failed for {}", media_ref)
            # Send visible fallback so failures are observable by the user.
            filename = self._guess_filename(media_ref, self._guess_upload_type(media_ref))
            await self._send_markdown_text(
                token,
                msg.chat_id,
                f"[Attachment send failed: {filename}]",
            )

    async def _download_and_save_media(
        self,
        media_type: str,
        media_info: dict[str, Any],
        conversation_id: str | None = None,
    ) -> tuple[str | None, str]:
        """
        Download media from DingTalk and save to local storage.

        Args:
            media_type: Type of media ("image", "file", "audio", "video")
            media_info: Dict containing download_code, filename, etc.
            conversation_id: Optional conversation ID for logging

        Returns:
            Tuple of (file_path, content_text)
            - file_path: Absolute path to saved file, or None if failed
            - content_text: Text description to include in message
        """
        from nanobot.config.paths import get_workspace_path

        download_code = media_info.get("download_code")
        if not download_code:
            logger.warning("No download_code found for media type: {}", media_type)
            return None, f"[{media_type.capitalize()} without download code]"

        # Check size limit configuration
        if not self.config.receive_files:
            return None, f"[{media_type.capitalize()} receiving disabled]"

        max_size = self.config.max_file_size_mb * 1024 * 1024

        # Get download URL from DingTalk API
        download_url = await self._get_media_download_url(media_type, download_code)
        if not download_url:
            return None, f"[Failed to get {media_type} download URL]"

        # Download the file
        if not self._http:
            logger.warning("HTTP client not available for download")
            return None, f"[Cannot download {media_type}: no HTTP client]"

        try:
            resp = await self._http.get(
                download_url,
                timeout=self.config.download_timeout_seconds
            )
            if resp.status_code >= 400:
                logger.error(
                    "Failed to download {} status={} code={}",
                    media_type, resp.status_code, download_code
                )
                return None, f"[Failed to download {media_type}: HTTP {resp.status_code}]"

            data = resp.content

            # Check file size limit
            if max_size > 0 and len(data) > max_size:
                logger.warning(
                    "{} exceeds size limit: {} > {} bytes",
                    media_type, len(data), max_size
                )
                size_mb = len(data) / 1024 / 1024
                return None, f"[{media_type.capitalize()} too large ({size_mb:.1f}MB > {self.config.max_file_size_mb}MB limit)]"

            # Determine filename
            filename = media_info.get("filename")
            if not filename:
                # Generate filename from download_code
                ext = self._get_default_extension(media_type)
                filename = f"{download_code[:16]}_{conversation_id or 'unknown'}{ext}"

            # Sanitize filename
            filename = self._sanitize_filename(filename)

            # Save to workspace/dingtalk directory instead of media/dingtalk
            workspace = get_workspace_path(self.config.workspace)
            dingtalk_dir = workspace / "dingtalk"
            dingtalk_dir.mkdir(parents=True, exist_ok=True)
            file_path = dingtalk_dir / filename

            # Handle duplicate filenames
            counter = 1
            while file_path.exists():
                name, ext = os.path.splitext(filename)
                file_path = dingtalk_dir / f"{name}_{counter}{ext}"
                counter += 1

            # Write file
            await asyncio.to_thread(file_path.write_bytes, data)

            # Get file size for message
            size_mb = len(data) / 1024 / 1024

            logger.info(
                "Downloaded {} to {} ({:.2f} MB)",
                media_type, file_path, size_mb
            )

            content_text = f"[{media_type.capitalize()}: {filename} ({size_mb:.2f}MB)]"
            return str(file_path), content_text

        except asyncio.TimeoutError:
            logger.error("Timeout downloading {} code={}", media_type, download_code)
            return None, f"[{media_type.capitalize()} download timed out]"
        except Exception as e:
            logger.error("Error downloading {} code={}: {}", media_type, download_code, e)
            return None, f"[Failed to download {media_type}]"

    async def _get_media_download_url(
        self,
        media_type: str,
        download_code: str
    ) -> str | None:
        """
        Get download URL for media using DingTalk OpenAPI.

        Args:
            media_type: Type of media ("image", "file", "audio", "video")
            download_code: The download code from message

        Returns:
            Download URL string or None if failed
        """
        token = await self._get_access_token()
        if not token:
            logger.error("No access token for media download")
            return None

        # Check if robot_code is configured
        if not self.config.robot_code:
            logger.error("robot_code not configured in DingTalk config")
            return None

        # Use POST request with body params
        # Reference: https://open.dingtalk.com/document/development/download-the-file-content-of-the-robot-receiving-message
        url = "https://api.dingtalk.com/v1.0/robot/messageFiles/download"
        headers = {
            "x-acs-dingtalk-access-token": token,
            "Content-Type": "application/json"
        }

        payload = {
            "downloadCode": download_code,
            "robotCode": self.config.robot_code  # Required: robot's code (different from client_id)
        }

        if not self._http:
            logger.error("HTTP client not available")
            return None

        try:
            resp = await self._http.post(url, json=payload, headers=headers)
            if resp.status_code != 200:
                logger.error(
                    "Failed to get download URL: HTTP {} body={}",
                    resp.status_code, resp.text[:200]
                )
                return None

            result = resp.json()
            download_url = result.get("downloadUrl")

            if not download_url:
                logger.error("No downloadUrl in response: {}", result)
                return None

            return download_url

        except Exception as e:
            logger.error("Error getting download URL: {}", e)
            return None

    def _get_default_extension(self, media_type: str) -> str:
        """Get default file extension for media type."""
        extensions = {
            "image": ".jpg",
            "picture": ".jpg",
            "file": ".bin",
            "audio": ".amr",
            "video": ".mp4",
        }
        return extensions.get(media_type, ".bin")

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for safe filesystem storage."""
        # Remove path separators and dangerous characters
        filename = filename.replace("/", "_").replace("\\", "_")
        filename = filename.replace("..", "_")
        # Keep alphanumeric, spaces, dots, hyphens, underscores
        import re
        filename = re.sub(r'[^\w\s\-_.]', '_', filename)
        # Limit length
        if len(filename) > 200:
            name, ext = os.path.splitext(filename)
            filename = name[:190] + ext
        return filename or "unnamed"

    async def _on_message(
        self,
        content: str,
        sender_id: str,
        sender_name: str,
        conversation_type: str | None = None,
        conversation_id: str | None = None,
        media_paths: list[str] | None = None,
    ) -> None:
        """Handle incoming message (called by NanobotDingTalkHandler).

        Delegates to BaseChannel._handle_message() which enforces allow_from
        permission checks before publishing to the bus.

        Args:
            content: Message text content
            sender_id: Sender's staff ID
            sender_name: Sender's nickname
            conversation_type: Type of conversation (1=private, 2=group)
            conversation_id: Conversation ID
            media_paths: List of local file paths for downloaded media
        """
        try:
            logger.info(
                "DingTalk inbound: {} from {} with {} media attachments",
                content[:100] if content else "[no text]",
                sender_name,
                len(media_paths) if media_paths else 0
            )

            is_group = conversation_type == "2" and conversation_id
            chat_id = f"group:{conversation_id}" if is_group else sender_id

            # Build enhanced metadata
            metadata = {
                "sender_name": sender_name,
                "platform": "dingtalk",
                "conversation_type": conversation_type,
                "media_count": len(media_paths) if media_paths else 0,
            }

            # Add file metadata if media present
            if media_paths:
                file_info = []
                for path in media_paths:
                    try:
                        stat = await asyncio.to_thread(os.stat, path)
                        size_mb = stat.st_size / 1024 / 1024
                        file_info.append({
                            "path": path,
                            "name": os.path.basename(path),
                            "size_bytes": stat.st_size,
                            "size_mb": round(size_mb, 2),
                        })
                    except Exception as e:
                        logger.warning("Failed to stat file {}: {}", path, e)
                metadata["files"] = file_info

            await self._handle_message(
                sender_id=sender_id,
                chat_id=chat_id,
                content=content,
                media=media_paths or [],
                metadata=metadata,
            )

        except Exception as e:
            logger.error("Error publishing DingTalk message: {}", e)
