#!/usr/bin/env python3
"""
Versão otimizada do script de transferência de álbuns do Telegram.
Sistema de 3 filas: Download (8), Upload (3), Envio (1), com ordem absoluta e sem ultrapassagem.
"""

import asyncio
import json
import logging
import os
import shutil
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
import telethon
print("Telethon version:", telethon.__version__)
print(f"Data/Hora Atual (UTC): {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Usuário: Clown171")

from telethon import TelegramClient
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument, Message, PeerChannel
from telethon.errors import FloodWaitError, SlowModeWaitError, TimeoutError, RPCError

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
    def __init__(self, 
                 api_id: int,
                 api_hash: str,
                 session_name: str,
                 source_chat_id: int,
                 target_chat_id: int,
                 temp_dir: str = "./temp_media",
                 max_download_queue: int = 8,
                 max_upload_queue: int = 3,
                 progress_db: str = "./transfer_progress.db",
                 batch_size: int = 1000):
        self.client = TelegramClient(session_name, api_id, api_hash)
        self.source_chat_id = source_chat_id
        self.target_chat_id = target_chat_id
        self.temp_dir = Path(temp_dir)
        self.max_download_queue = max_download_queue
        self.max_upload_queue = max_upload_queue
        self.batch_size = batch_size
        self.progress_tracker = ProgressTracker(progress_db)
        self.albums: Dict[int, AlbumInfo] = {}
        self.upload_delay = 7.0
        self.last_upload_time = 0
        self.request_delay = 3
        self.timeout = 60
        self.flood_wait_multiplier = 1.7
        self.max_retries = 5
        self.download_delay = 0.8
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
            await self.process_with_three_queues()
            self.logger.info("Transferência concluída com sucesso!")
        except Exception as e:
            self.logger.error(f"Erro durante a transferência: {e}")
            raise
        finally:
            await self.cleanup()

    async def scan_messages_chronological(self):
        self.logger.info("\n=== INICIANDO ESCANEAMENTO CRONOLÓGICO ===")
        self.logger.info(f"Data/Hora UTC: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        MESSAGE_LINK = "https://t.me/c/1781722146/185127"
        
        self.logger.info("\n🔍 Tentando acessar mensagem pelo link...")
        self.logger.info(f"Link: {MESSAGE_LINK}")

        try:
            # Primeiro, vamos obter a entidade do canal
            channel = await self.client.get_entity(PeerChannel(self.source_chat_id))
            if not channel:
                raise ValueError("❌ Canal não encontrado!")

            # Agora vamos pegar a mensagem específica
            message = await self.client.get_messages(channel, ids=185127)
            if not message:
                raise ValueError("❌ Mensagem não encontrada!")

            self.logger.info("✅ Mensagem encontrada!")
            self.logger.info(f"ID: {message.id}")
            self.logger.info(f"Data: {message.date}")
            self.logger.info(f"Texto: {getattr(message, 'text', None)}")
            self.logger.info(f"Grupo ID: {getattr(message, 'grouped_id', None)}")

            if not message.grouped_id:
                raise ValueError("❌ A mensagem não faz parte de um álbum!")

            # Pegar todas as mensagens do mesmo grupo (álbum)
            grouped_messages = []
            async for msg in self.client.iter_messages(
                channel,
                min_id=message.id - 10,
                max_id=message.id + 10
            ):
                if getattr(msg, 'grouped_id', None) == message.grouped_id:
                    grouped_messages.append(msg)

            if not grouped_messages:
                raise ValueError("❌ Não foi possível encontrar as mensagens do álbum!")

            self.logger.info(f"\n✅ Álbum encontrado com {len(grouped_messages)} mensagens")

            # Continua com o processamento das mensagens subsequentes
            all_messages = []
            all_messages.extend(grouped_messages)
            message_count = len(grouped_messages)

            self.logger.info("\n🔄 Coletando mensagens posteriores ao primeiro álbum...")
            
            async for next_message in self.client.iter_messages(
                channel,
                min_id=message.id,
                reverse=True
            ):
                message_count += 1
                
                if hasattr(next_message, 'media') and next_message.media:
                    all_messages.append(next_message)
                    
                    if len(all_messages) % 100 == 0:
                        self.logger.info(f"📊 Progresso: {len(all_messages)} mensagens com mídia")
                
                if message_count % 200 == 0:
                    await asyncio.sleep(1.2)

            self.logger.info(f"\n✅ Coleta concluída: {len(all_messages)} mensagens com mídia")

            # Processamento dos álbuns
            await self.process_messages_for_albums(all_messages)
            
            await self.progress_tracker.update_progress("last_processed_message", "completed")
            self.logger.info(f"\n✅ Escaneamento concluído com sucesso!")
            self.logger.info(f"📊 Total de álbuns encontrados: {len(self.albums)}")

        except Exception as e:
            self.logger.error(f"\n❌ Erro durante o escaneamento: {str(e)}")
            raise

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

    async def process_messages_for_albums(self, messages: List[Message]):
        self.logger.info(f"Processando {len(messages)} mensagens para identificar álbuns...")
        album_groups = defaultdict(list)
        media_count = 0

        for message in messages:
            media_info = await self.extract_media_info_safe(message)
            if media_info:
                media_count += 1
                if media_info.grouped_id:
                    album_groups[media_info.grouped_id].append(media_info)

        self.logger.info(f"Encontradas {media_count} mídias, {len(album_groups)} grupos")

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

                if len(batch_albums) >= 100:
                    await self.progress_tracker.save_albums_batch(batch_albums)
                    batch_albums = []

        if batch_albums:
            await self.progress_tracker.save_albums_batch(batch_albums)

        self.logger.info(f"Álbuns válidos criados: {valid_albums}")

    async def process_with_three_queues(self):
        sorted_albums = sorted(self.albums.values(), key=lambda x: x.date)
        total = len(sorted_albums)

        download_slots = asyncio.Semaphore(self.max_download_queue)
        upload_slots = asyncio.Semaphore(self.max_upload_queue)
        send_slot = asyncio.Semaphore(1)

        download_cursor = 0
        upload_cursor = 0
        send_cursor = 0

        download_condition = asyncio.Condition()
        upload_condition = asyncio.Condition()
        send_condition = asyncio.Condition()

        async def download_worker(idx, album):
            nonlocal download_cursor
            async with download_condition:
                while idx != download_cursor:
                    await download_condition.wait()
            async with download_slots:
                await self.download_album_safe(album)
                album.downloaded = True
                await self.progress_tracker.save_album(album)
            async with download_condition:
                download_cursor += 1
                download_condition.notify_all()

        async def upload_worker(idx, album):
            nonlocal upload_cursor
            while True:
                async with upload_condition:
                    if idx == upload_cursor and album.downloaded:
                        break
                    await upload_condition.wait()
            async with upload_slots:
                await self.upload_album_corrected(album)
                album.uploaded = True
                await self.progress_tracker.save_album(album)
                await self.cleanup_album_files(album)
            async with upload_condition:
                upload_cursor += 1
                upload_condition.notify_all()

        async def send_worker(idx, album):
            nonlocal send_cursor
            while True:
                async with send_condition:
                    if idx == send_cursor and album.uploaded:
                        break
                    await send_condition.wait()
            async with send_slot:
                self.logger.info(f"Álbum {album.grouped_id} FINALIZADO")
            async with send_condition:
                send_cursor += 1
                send_condition.notify_all()

        tasks = []
        for idx, album in enumerate(sorted_albums):
            tasks.append(asyncio.create_task(download_worker(idx, album)))
            tasks.append(asyncio.create_task(upload_worker(idx, album)))
            tasks.append(asyncio.create_task(send_worker(idx, album)))
        await asyncio.gather(*tasks)

    async def extract_media_info_safe(self, message: Message) -> Optional[MediaInfo]:
        try:
            return await asyncio.wait_for(
                self.extract_media_info(message),
                timeout=self.timeout
            )
        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout extraindo mídia da mensagem {message.id}")
            return None
        except Exception as e:
            self.logger.warning(f"Erro extraindo mídia da mensagem {message.id}: {e}")
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

    async def download_media(self, media: MediaInfo):
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
                    caption=album.caption,
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
    MAX_DOWNLOAD_QUEUE = 8
    MAX_UPLOAD_QUEUE = 3
    PROGRESS_DB = "./transfer_progress.db"
    BATCH_SIZE = 200
    print("🚀 Script Otimizado - Três Filas (Download, Upload, Envio)")
    print(f"📁 Origem: {SOURCE_CHAT_ID}")
    print(f"📁 Destino: {TARGET_CHAT_ID}")
    print(f"💾 Downloads paralelos: {MAX_DOWNLOAD_QUEUE}")
    print(f"⬆️ Uploads paralelos: {MAX_UPLOAD_QUEUE}")
    print(f"🗄️ Diretório temporário: {TEMP_DIR}")
    print(f"📊 Banco de progresso: {PROGRESS_DB}")
    transfer = TelegramAlbumTransfer(
        api_id=API_ID,
        api_hash=API_HASH,
        session_name=SESSION_NAME,
        source_chat_id=SOURCE_CHAT_ID,
        target_chat_id=TARGET_CHAT_ID,
        temp_dir=TEMP_DIR,
        max_download_queue=MAX_DOWNLOAD_QUEUE,
        max_upload_queue=MAX_UPLOAD_QUEUE,
        progress_db=PROGRESS_DB,
        batch_size=BATCH_SIZE
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
    except ImportError as e:
        print("❌ Dependências não instaladas!")
        print("Execute: pip install telethon tqdm")
        sys.exit(1)
    asyncio.run(main())