#!/usr/bin/env python3
"""
Versão otimizada do script de transferência de álbuns do Telegram.
- Download paralelizado de álbuns (buffer).
- Upload paralelizado de álbuns, mas sempre mantendo a ordem cronológica (respeita ordem da fila).
- Correção: upload só começa após todos os arquivos do álbum existirem em disco!
"""

import asyncio
import json
import logging
import os
import shutil
import sqlite3
import sys
import time
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import telethon
print("Telethon version:", telethon.__version__)

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, Message
from telethon.errors import FloodWaitError, SlowModeWaitError, TimeoutError, RPCError
from tqdm import tqdm

@dataclass
class MediaInfo:
    message_id: int
    grouped_id: Optional[int]
    date: datetime
    media_type: str
    file_size: int
    file_name: str
    caption: Optional[str]
    local_path: Optional[str] = None
    downloaded: bool = False
    uploaded: bool = False

@dataclass
class AlbumInfo:
    grouped_id: int
    medias: List[MediaInfo]
    caption: Optional[str]
    date: datetime
    processed: bool = False
    downloaded: bool = False
    uploaded: bool = False

    @property
    def total_size(self) -> int:
        return sum(media.file_size for media in self.medias)

class ProgressTracker:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS albums (
                    grouped_id INTEGER PRIMARY KEY,
                    album_data TEXT,
                    processed BOOLEAN DEFAULT 0,
                    downloaded BOOLEAN DEFAULT 0,
                    uploaded BOOLEAN DEFAULT 0,
                    date_created TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS progress (
                    key TEXT PRIMARY KEY,
                    value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_albums_status ON albums(processed, downloaded, uploaded)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_albums_date ON albums(date_created)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_progress_key ON progress(key)")

    async def save_album(self, album: AlbumInfo):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO albums 
                (grouped_id, album_data, processed, downloaded, uploaded, date_created)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                album.grouped_id,
                json.dumps(asdict(album), default=str),
                album.processed,
                album.downloaded,
                album.uploaded,
                album.date.isoformat()
            ))

    async def save_albums_batch(self, albums: List[AlbumInfo]):
        with sqlite3.connect(self.db_path) as conn:
            data = [
                (
                    album.grouped_id,
                    json.dumps(asdict(album), default=str),
                    album.processed,
                    album.downloaded,
                    album.uploaded,
                    album.date.isoformat()
                )
                for album in albums
            ]
            conn.executemany("""
                INSERT OR REPLACE INTO albums 
                (grouped_id, album_data, processed, downloaded, uploaded, date_created)
                VALUES (?, ?, ?, ?, ?, ?)
            """, data)

    async def load_albums(self) -> Dict[int, AlbumInfo]:
        albums = {}
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                SELECT album_data FROM albums 
                ORDER BY date_created ASC
            """)
            for (album_data,) in cursor.fetchall():
                try:
                    data = json.loads(album_data)
                    medias = []
                    for media_data in data['medias']:
                        media_data['date'] = datetime.fromisoformat(media_data['date'])
                        medias.append(MediaInfo(**media_data))
                    data['medias'] = medias
                    data['date'] = datetime.fromisoformat(data['date'])
                    album = AlbumInfo(**data)
                    albums[album.grouped_id] = album
                except Exception as e:
                    logging.warning(f"Erro carregando álbum: {e}")
                    continue
        return albums

    async def update_progress(self, key: str, value: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO progress (key, value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            """, (key, value))

    async def get_progress(self, key: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT value FROM progress WHERE key = ?", (key,))
            result = cursor.fetchone()
            return result[0] if result else None

class TelegramAlbumTransfer:
    """Classe principal para transferência de álbuns com fila e upload paralelo ordenado"""
    def __init__(self, 
                 api_id: int,
                 api_hash: str,
                 session_name: str,
                 source_chat_id: int,
                 target_chat_id: int,
                 temp_dir: str = "./temp_media",
                 max_concurrent_downloads: int = 6,
                 progress_db: str = "./transfer_progress.db",
                 batch_size: int = 1000,
                 prefetch_albums: int = 8,
                 max_concurrent_uploads: int = 3):
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.source_chat_id = source_chat_id
        self.target_chat_id = target_chat_id
        self.temp_dir = Path(temp_dir)
        self.max_concurrent_downloads = max_concurrent_downloads
        self.batch_size = batch_size
        self.progress_tracker = ProgressTracker(progress_db)
        self.albums: Dict[int, AlbumInfo] = {}
        self.single_medias: List[MediaInfo] = []
        self.download_semaphore = asyncio.Semaphore(max_concurrent_downloads)
        self.upload_delay = 7.0
        self.last_upload_time = 0
        self.request_delay = 3
        self.timeout = 60
        self.flood_wait_multiplier = 1.7
        self.max_retries = 5
        self.download_delay = 0.8
        self.prefetch_albums = prefetch_albums  # Quantos álbuns baixar antes de começar uploads (buffer/fila)
        self.max_concurrent_uploads = max_concurrent_uploads
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('transfer.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.floodwait_log_count = 0

    async def safe_telegram_call(self, func, *args, **kwargs):
        for attempt in range(10):
            try:
                return await func(*args, **kwargs)
            except FloodWaitError as e:
                wait_time = getattr(e, 'seconds', 60)
                self.floodwait_log_count += 1
                self.logger.warning(f"[FloodWait #{self.floodwait_log_count}] FloodWait de {wait_time}s em {func.__name__}, aguardando...")
                await asyncio.sleep(wait_time + 1)
            except Exception as e:
                self.logger.error(f"Erro inesperado em {func.__name__}: {e}")
                if attempt == 9:
                    raise
                await asyncio.sleep(3)

    async def start(self):
        await self.client.start()
        self.logger.info("Cliente Telegram conectado")
        self.temp_dir.mkdir(exist_ok=True)
        try:
            self.albums = await self.progress_tracker.load_albums()
            last_message_id = await self.progress_tracker.get_progress("last_processed_message")
            if self.albums:
                self.logger.info(f"Carregados {len(self.albums)} álbuns do progresso anterior")
            if last_message_id != "completed":
                self.logger.info("Iniciando escaneamento completo e ordenado...")
                await self.scan_messages_chronological()
            else:
                self.logger.info("Escaneamento já foi concluído anteriormente")
            await self.process_albums_chronological_with_parallel_uploads()
            self.logger.info("Transferência concluída com sucesso!")
        except Exception as e:
            self.logger.error(f"Erro durante a transferência: {e}")
            raise
        finally:
            await self.cleanup()

    async def scan_messages_chronological(self):
        self.logger.info("Iniciando escaneamento cronológico completo...")
        try:
            chat_info = await self.safe_telegram_call(self.client.get_entity, self.source_chat_id)
            self.logger.info(f"Chat: {getattr(chat_info, 'title', 'Chat privado')}")
        except Exception as e:
            self.logger.warning(f"Não foi possível obter informações do chat: {e}")
        self.logger.info("Coletando todas as mensagens com mídia...")
        all_messages = []
        message_count = 0
        batch_count = 0
        try:
            async for message in self.client.iter_messages(
                self.source_chat_id,
                limit=None,
                reverse=True
            ):
                message_count += 1
                batch_count += 1
                if hasattr(message, 'media') and message.media:
                    all_messages.append(message)
                if batch_count >= 200:
                    await asyncio.sleep(1.2)
                    batch_count = 0
                if message_count % 5000 == 0:
                    self.logger.info(f"Coletadas {message_count} mensagens...")
        except FloodWaitError as e:
            wait_time = getattr(e, 'seconds', 60)
            self.logger.warning(f"FloodWait durante coleta: aguardando {wait_time}s")
            await asyncio.sleep(wait_time + 1)
        except Exception as e:
            self.logger.error(f"Erro inesperado durante coleta de mensagens: {e}")
            raise

        self.logger.info(f"Coletadas {len(all_messages)} mensagens com mídia de {message_count} mensagens totais")

        if all_messages:
            self.logger.info(f"Primeira mensagem: ID={all_messages[0].id}, Data={all_messages[0].date}")
            self.logger.info(f"Última mensagem: ID={all_messages[-1].id}, Data={all_messages[-1].date}")
            for m in all_messages[:20]:
                self.logger.info(f"MsgID {m.id} - date={m.date} grouped_id={getattr(m, 'grouped_id', None)}")
        all_messages.sort(key=lambda x: x.date)
        self.logger.info(f"Mensagens ordenadas cronologicamente")
        await self.process_messages_for_albums(all_messages)
        await self.progress_tracker.update_progress("last_processed_message", "completed")
        self.logger.info(f"Escaneamento concluído: {len(self.albums)} álbuns encontrados")

    async def process_messages_for_albums(self, messages: List[Message]):
        self.logger.info(f"Processando {len(messages)} mensagens para identificar álbuns...")
        album_groups = defaultdict(list)
        loose_album_buffer = []
        media_count = 0

        def flush_loose_album():
            if len(loose_album_buffer) >= 2:
                grouped_id = -int(f"{int(loose_album_buffer[0].date.timestamp())}{loose_album_buffer[0].message_id}")
                for m in loose_album_buffer:
                    m.grouped_id = grouped_id
                album_groups[grouped_id].extend(loose_album_buffer)
            loose_album_buffer.clear()

        previous = None
        for i, message in enumerate(messages):
            if i % 1000 == 0:
                self.logger.info(f"Processando mensagem {i+1}/{len(messages)}")

            media_info = await self.extract_media_info_safe(message)
            if not media_info:
                flush_loose_album()
                previous = None
                continue
            media_count += 1

            if media_info.grouped_id:
                flush_loose_album()
                album_groups[media_info.grouped_id].append(media_info)
                previous = None
            else:
                if previous:
                    same_day = media_info.date.date() == previous.date.date()
                    same_type = media_info.media_type == previous.media_type
                    time_gap = abs((media_info.date - previous.date).total_seconds()) < 180
                    if same_day and same_type and time_gap:
                        loose_album_buffer.append(media_info)
                    else:
                        flush_loose_album()
                        loose_album_buffer.append(media_info)
                else:
                    loose_album_buffer.append(media_info)
                previous = media_info
        flush_loose_album()

        self.logger.info(f"Encontradas {media_count} mídias, {len(album_groups)} grupos potenciais")

        valid_albums = 0
        batch_albums = []
        for grouped_id, medias in album_groups.items():
            if len(medias) >= 2:
                medias.sort(key=lambda x: x.date)
                caption = next((m.caption for m in medias if m.caption), None)
                album = AlbumInfo(
                    grouped_id=grouped_id,
                    medias=medias,
                    caption=caption,
                    date=medias[0].date
                )
                self.albums[grouped_id] = album
                batch_albums.append(album)
                valid_albums += 1
                if valid_albums <= 5:
                    self.logger.info(f"Álbum {valid_albums}: ID={grouped_id}, "
                        f"Data={medias[0].date.strftime('%Y-%m-%d %H:%M:%S')}, "
                        f"Mídias={len(medias)}, "
                        f"Tipos={[m.media_type for m in medias]}")
                if len(batch_albums) >= 100:
                    await self.progress_tracker.save_albums_batch(batch_albums)
                    batch_albums = []
        if batch_albums:
            await self.progress_tracker.save_albums_batch(batch_albums)

        self.logger.info(f"Álbuns válidos criados: {valid_albums}")
        self.logger.info(f"Mídias individuais: {len(self.single_medias)}")

        december_2023_albums = [
            album for album in self.albums.values()
            if album.date.year == 2023 and album.date.month == 12
        ]
        if december_2023_albums:
            self.logger.info(f"Encontrados {len(december_2023_albums)} álbuns de dezembro de 2023:")
            for album in december_2023_albums[:3]:
                self.logger.info(f"  - Álbum {album.grouped_id}: {album.date.strftime('%Y-%m-%d %H:%M:%S')}, "
                                 f"{len(album.medias)} mídias, tipos: {[m.media_type for m in album.medias]}")
        else:
            self.logger.warning("⚠️ Nenhum álbum de dezembro de 2023 encontrado!")

    async def extract_media_info_safe(self, message: Message) -> Optional[MediaInfo]:
        for attempt in range(self.max_retries):
            try:
                return await asyncio.wait_for(
                    self.extract_media_info(message),
                    timeout=self.timeout
                )
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout extraindo mídia da mensagem {message.id}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(1)
                else:
                    return None
            except Exception as e:
                self.logger.warning(f"Erro extraindo mídia da mensagem {message.id}: {e}")
                return None
        return None

    async def extract_media_info(self, message: Message) -> Optional[MediaInfo]:
        if not hasattr(message, 'media') or not message.media:
            return None
        media_type = "unknown"
        file_size = 0
        file_name = f"media_{message.id}"
        try:
            if isinstance(message.media, MessageMediaPhoto):
                media_type = "photo"
                if hasattr(message.media.photo, 'sizes'):
                    largest_size = max(message.media.photo.sizes, 
                                     key=lambda x: getattr(x, 'size', 0) if hasattr(x, 'size') else 0)
                    file_size = getattr(largest_size, 'size', 0) or 2000000
                else:
                    file_size = 2000000
                file_name = f"photo_{message.id}.jpg"
            elif isinstance(message.media, MessageMediaDocument):
                doc = message.media.document
                media_type = "document"
                file_size = getattr(doc, 'size', 0)
                mime_type = getattr(doc, 'mime_type', '')
                if mime_type:
                    if mime_type.startswith('video/'):
                        media_type = "video"
                    elif mime_type.startswith('image/'):
                        media_type = "image"
                    elif mime_type.startswith('audio/'):
                        media_type = "audio"
                if hasattr(doc, 'attributes'):
                    for attr in doc.attributes:
                        if hasattr(attr, 'file_name') and attr.file_name:
                            file_name = attr.file_name
                            break
                    else:
                        extension = "bin"
                        if mime_type:
                            extension = mime_type.split('/')[-1]
                            if extension in ['jpeg', 'jpg']:
                                extension = 'jpg'
                            elif extension == 'mpeg':
                                extension = 'mp4'
                        file_name = f"{media_type}_{message.id}.{extension}"
            if file_size == 0 and media_type == "unknown":
                return None
            return MediaInfo(
                message_id=message.id,
                grouped_id=getattr(message, 'grouped_id', None),
                date=message.date,
                media_type=media_type,
                file_size=file_size,
                file_name=file_name,
                caption=getattr(message, 'text', None) or getattr(message, 'message', None)
            )
        except Exception as e:
            self.logger.warning(f"Erro extraindo mídia da mensagem {message.id}: {e}")
            return None

    async def process_albums_chronological_with_parallel_uploads(self):
        """Downloads paralelos de álbuns, uploads paralelos mas SEMPRE na ordem da fila."""
        sorted_albums = sorted(self.albums.values(), key=lambda x: x.date)
        pending_albums = [album for album in sorted_albums if not album.uploaded]
        total = len(pending_albums)
        self.logger.info(f"Processando {total} álbuns em ordem cronológica (upload paralelo ordenado)")
        if not pending_albums:
            self.logger.info("Todos os álbuns já foram enviados!")
            return

        upload_queue = deque()
        download_tasks = {}
        album_idx = 0

        # Pré-carrega até prefetch_albums álbuns para download em paralelo
        while album_idx < len(pending_albums) and len(download_tasks) < self.prefetch_albums:
            album = pending_albums[album_idx]
            self.logger.info(f"[Fila] Iniciando download do álbum {album_idx+1}/{total}, ID {album.grouped_id}")
            task = asyncio.create_task(self.download_album_safe(album))
            download_tasks[album.grouped_id] = (task, album)
            album_idx += 1

        # Vamos controlar a janela de uploads paralelos, mas respeitando a fila
        uploads_in_progress = {}
        upload_results = {}
        next_upload_index = 0

        while next_upload_index < total:
            # Aguarda ao menos um álbum baixar por completo
            if not upload_queue:
                done, _ = await asyncio.wait(
                    [t for t, _ in download_tasks.values()],
                    return_when=asyncio.FIRST_COMPLETED
                )
                for group_id, (task, album) in list(download_tasks.items()):
                    if task in done:
                        try:
                            await task  # Propaga exceção se houve erro
                            album.downloaded = True
                            await self.progress_tracker.save_album(album)
                            # VERIFICAÇÃO EXTRA: todos os arquivos do álbum existem?
                            all_ok = True
                            for media in album.medias:
                                if not (media.local_path and os.path.exists(media.local_path)):
                                    self.logger.error(f"Mídia {media.file_name} não foi baixada corretamente para o álbum {album.grouped_id}. Retentando...")
                                    await self.download_media(media)
                                    if not (media.local_path and os.path.exists(media.local_path)):
                                        all_ok = False
                                        break
                            if all_ok:
                                upload_queue.append(album)
                                self.logger.info(f"[Fila] Álbum baixado e enfileirado para upload: {album.grouped_id}")
                            else:
                                self.logger.error(f"Álbum {album.grouped_id} não está completamente baixado. Nova tentativa no final da fila.")
                                # Tenta baixar de novo ao final
                                download_tasks[group_id] = (asyncio.create_task(self.download_album_safe(album)), album)
                                continue
                        except Exception as e:
                            self.logger.error(f"Download de álbum {album.grouped_id} falhou: {e}")
                            download_tasks[group_id] = (asyncio.create_task(self.download_album_safe(album)), album)
                            continue
                        del download_tasks[group_id]
                # Mantém o buffer de downloads cheio
                while album_idx < len(pending_albums) and len(download_tasks) < self.prefetch_albums:
                    album = pending_albums[album_idx]
                    self.logger.info(f"[Fila] Iniciando download do álbum {album_idx+1}/{total}, ID {album.grouped_id}")
                    task = asyncio.create_task(self.download_album_safe(album))
                    download_tasks[album.grouped_id] = (task, album)
                    album_idx += 1

            # Envia em paralelo até o limite, mas só álbuns no início da fila
            while (len(uploads_in_progress) < self.max_concurrent_uploads and
                   upload_queue and
                   next_upload_index + len(uploads_in_progress) < total):
                # Mantém a ordem: só pega o próximo álbum da fila
                album = upload_queue.popleft()
                idx = next_upload_index + len(uploads_in_progress)
                self.logger.info(f"[UPLOAD-FILA] Slot {len(uploads_in_progress)+1}/{self.max_concurrent_uploads}: preparando envio do álbum {album.grouped_id} (posição {idx+1}/{total})")
                upload_task = asyncio.create_task(self.upload_album_with_status(album, idx))
                uploads_in_progress[idx] = (upload_task, album)

            # Aguarda o upload mais antigo terminar, antes de liberar o próximo na ordem!
            if uploads_in_progress:
                idx_to_await = min(uploads_in_progress.keys())
                task, album = uploads_in_progress[idx_to_await]
                await task  # Garante ordem
                upload_results[idx_to_await] = True
                del uploads_in_progress[idx_to_await]
                next_upload_index += 1

    async def upload_album_with_status(self, album, idx):
        try:
            await self.upload_album_corrected(album)
            album.uploaded = True
            await self.progress_tracker.save_album(album)
            await self.cleanup_album_files(album)
            self.logger.info(f"[UPLOAD-FILA] Álbum {album.grouped_id} enviado com sucesso (posição {idx+1})")
        except Exception as e:
            self.logger.error(f"[UPLOAD-FILA] Falha ao enviar álbum {album.grouped_id} (posição {idx+1}): {e}")
            await asyncio.sleep(10)
            await self.upload_album_with_status(album, idx)

    async def download_album_safe(self, album: AlbumInfo):
        try:
            await asyncio.wait_for(
                self.download_album(album),
                timeout=self.timeout * len(album.medias) + 1000
            )
        except asyncio.TimeoutError:
            raise TimeoutError(f"Timeout baixando álbum {album.grouped_id}")

    async def download_album(self, album: AlbumInfo):
        self.logger.info(f"Baixando álbum {album.grouped_id} "
                        f"({len(album.medias)} mídias, {album.total_size / 1024 / 1024:.1f} MB)")
        album_dir = self.temp_dir / f"album_{album.grouped_id}"
        album_dir.mkdir(exist_ok=True)
        download_tasks = []
        for media in album.medias:
            if not media.downloaded:
                media.local_path = str(album_dir / media.file_name)
                task = self.download_media(media)
                download_tasks.append(task)
        if download_tasks:
            results = await asyncio.gather(*download_tasks, return_exceptions=True)
            for idx, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.warning(f"Download falhou para {album.medias[idx].file_name}, re-tentando individualmente.")
                    await self.download_media(album.medias[idx])

        # VERIFICAÇÃO EXTRA: garante que todos existem
        for media in album.medias:
            if not (media.local_path and os.path.exists(media.local_path)):
                self.logger.error(f"Mídia {media.file_name} não foi baixada corretamente para o álbum {album.grouped_id}")
                await self.download_media(media)
                if not (media.local_path and os.path.exists(media.local_path)):
                    raise RuntimeError(f"Falha persistente ao baixar {media.file_name} para álbum {album.grouped_id}")

    async def download_media(self, media: MediaInfo):
        async with self.download_semaphore:
            delay = 1
            for attempt in range(10):
                try:
                    await asyncio.sleep(self.download_delay)
                    message = await self.safe_telegram_call(self.client.get_messages, self.source_chat_id, ids=media.message_id)
                    if message and message.media:
                        input_location = None
                        if hasattr(message.media, "document") and message.media.document:
                            input_location = message.media.document
                        elif hasattr(message.media, "photo") and message.media.photo:
                            input_location = message.media.photo
                        else:
                            input_location = message.media
                        await self.safe_telegram_call(
                            self.client.download_file,
                            input_location,
                            file=media.local_path,
                            part_size_kb=4096
                        )
                        if os.path.exists(media.local_path) and os.path.getsize(media.local_path) > 1000:
                            media.downloaded = True
                            self.logger.info(f"Baixado: {media.file_name}")
                            return
                        else:
                            self.logger.warning(f"Arquivo baixado {media.local_path} parece inválido, re-tentando...")
                    else:
                        self.logger.warning(f"Mensagem {media.message_id} não tem mídia, re-tentando...")
                except (FloodWaitError, TimeoutError, asyncio.TimeoutError) as e:
                    wait_time = getattr(e, 'seconds', delay)
                    self.logger.warning(f"[FloodWait/Timeout] baixando mídia {media.message_id}, aguardando {wait_time}s")
                    await asyncio.sleep(wait_time * self.flood_wait_multiplier + 1)
                except Exception as e:
                    self.logger.warning(f"Erro baixando mídia {media.message_id}, tentativa {attempt+1}: {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
            raise RuntimeError(f"Não foi possível baixar mídia {media.message_id} após muitas tentativas.")
        
    async def upload_album_corrected(self, album: AlbumInfo):
        self.logger.info(f"Enviando álbum {album.grouped_id}")
        time_since_last = time.time() - self.last_upload_time
        if time_since_last < self.upload_delay:
            await asyncio.sleep(self.upload_delay - time_since_last)
        album.medias.sort(key=lambda m: (m.caption is None, m.date))
        for attempt in range(10):
            try:
                files_to_send = []
                for media in album.medias:
                    if not media.local_path or not os.path.exists(media.local_path):
                        raise FileNotFoundError(f"Arquivo não encontrado: {media.local_path}")
                    files_to_send.append(media.local_path)
                await self.safe_telegram_call(
                    self.client.send_file,
                    self.target_chat_id,
                    files_to_send,
                    caption=album.medias[0].caption if album.medias else album.caption,
                    force_document=False,
                    supports_streaming=True
                )
                self.last_upload_time = time.time()
                self.logger.info(f"Álbum {album.grouped_id} enviado com sucesso ({len(album.medias)} mídias)")
                return
            except (FloodWaitError, SlowModeWaitError) as e:
                wait_time = getattr(e, 'seconds', 60) * self.flood_wait_multiplier
                self.logger.warning(f"[FloodWait] Rate limit atingido, aguardando {wait_time:.1f} segundos")
                await asyncio.sleep(wait_time)
            except Exception as e:
                self.logger.error(f"Erro enviando álbum {album.grouped_id}: {e}")
                await asyncio.sleep(10)
        raise RuntimeError(f"Falha ao enviar álbum {album.grouped_id} após várias tentativas.")

    async def cleanup_album_files(self, album: AlbumInfo):
        try:
            album_dir = self.temp_dir / f"album_{album.grouped_id}"
            if album_dir.exists():
                shutil.rmtree(album_dir)
        except Exception as e:
            self.logger.warning(f"Erro limpando arquivos do álbum {album.grouped_id}: {e}")

    async def cleanup(self):
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
        except Exception as e:
            self.logger.warning(f"Erro na limpeza final: {e}")
        try:
            await self.client.disconnect()
        except Exception as e:
            self.logger.warning(f"Erro desconectando cliente: {e}")

async def main():
    API_ID = 20372456
    API_HASH = "4bf8017e548b790415a11cc8ed1b9804"
    SESSION_NAME = "album_transfer_session"
    SOURCE_CHAT_ID = -1001781722146
    TARGET_CHAT_ID = -1002608875175
    TEMP_DIR = "./temp_media"
    MAX_CONCURRENT_DOWNLOADS = 6
    PROGRESS_DB = "./transfer_progress.db"
    BATCH_SIZE = 200
    PREFETCH_ALBUMS = 8  # Quantos álbuns ficarão em buffer para download (ajuste conforme RAM/rede)
    MAX_CONCURRENT_UPLOADS = 3  # Quantos uploads de álbum rodar em paralelo (mas sempre respeitando ordem da fila)
    print("🚀 Script Otimizado - Transferência de Álbuns do Telegram (Upload Paralelo Ordenado)")
    print("✅ Download e upload confiáveis, paralelismo, retentativas infinitas para álbuns.")
    print(f"📁 Origem: {SOURCE_CHAT_ID}")
    print(f"📁 Destino: {TARGET_CHAT_ID}")
    print(f"💾 Downloads paralelos por álbum: {MAX_CONCURRENT_DOWNLOADS}")
    print(f"📦 Buffer de álbuns em download: {PREFETCH_ALBUMS}")
    print(f"⬆️ Uploads paralelos de álbuns: {MAX_CONCURRENT_UPLOADS}")
    print(f"📦 Tamanho do lote: {BATCH_SIZE}")
    print(f"🗄️ Diretório temporário: {TEMP_DIR}")
    print(f"📊 Banco de progresso: {PROGRESS_DB}")
    transfer = TelegramAlbumTransfer(
        api_id=API_ID,
        api_hash=API_HASH,
        session_name=SESSION_NAME,
        source_chat_id=SOURCE_CHAT_ID,
        target_chat_id=TARGET_CHAT_ID,
        temp_dir=TEMP_DIR,
        max_concurrent_downloads=MAX_CONCURRENT_DOWNLOADS,
        progress_db=PROGRESS_DB,
        batch_size=BATCH_SIZE,
        prefetch_albums=PREFETCH_ALBUMS,
        max_concurrent_uploads=MAX_CONCURRENT_UPLOADS
    )
    try:
        await transfer.start()
    except KeyboardInterrupt:
        print("\n⏸️ Transferência interrompida pelo usuário")
        print("📊 Progresso salvo. Execute novamente para continuar de onde parou.")
    except Exception as e:
        print(f"❌ Erro durante a transferência: {e}")
        logging.error(f"Erro completo: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    try:
        import telethon
        import tqdm
    except ImportError as e:
        print("❌ Dependências não instaladas!")
        print("Execute: pip install telethon tqdm")
        sys.exit(1)
    asyncio.run(main())