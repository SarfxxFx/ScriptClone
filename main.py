#!/usr/bin/env python3
"""
Versão otimizada do script de transferência de álbuns do Telegram.
Sistema de 3 filas: Download (8), Upload (3), Envio (1), com ordem absoluta e sem ultrapassagem.
CORREÇÃO: Implementação rigorosa de ordem cronológica em todas as filas.
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

@dataclass
class QueuePosition:
    """Classe para rastrear posições nas filas"""
    album_id: int
    original_index: int  # Índice na ordem cronológica original
    download_started: bool = False
    download_completed: bool = False
    upload_started: bool = False
    upload_completed: bool = False
    send_completed: bool = False

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
        
        # Sistema de filas com ordem rigorosa
        self.download_queue = deque()  # Fila de download (máx 8 simultâneos)
        self.upload_queue = deque()    # Fila de upload (máx 3 simultâneos)
        self.send_queue = deque()      # Fila de envio (máx 1)
        
        # Controle de estado das filas
        self.download_active = set()   # IDs dos álbuns sendo baixados
        self.upload_active = set()     # IDs dos álbuns sendo enviados
        self.send_active = None        # ID do álbum sendo enviado (ou None)
        
        # Locks para sincronização
        self.download_lock = asyncio.Lock()
        self.upload_lock = asyncio.Lock()
        self.send_lock = asyncio.Lock()
        
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
            await self.pipeline_strict_order()
            self.logger.info("Transferência concluída com sucesso!")
        except Exception as e:
            self.logger.error(f"Erro durante a transferência: {e}")
            raise
        finally:
            await self.cleanup()

    async def pipeline_strict_order(self):
        """Pipeline com ordem rigorosa - nenhum álbum pode ultrapassar outro"""
        # Ordenar álbuns cronologicamente
        sorted_albums = sorted(self.albums.values(), key=lambda x: x.date)
        total = len(sorted_albums)
        
        self.logger.info(f"Iniciando pipeline com {total} álbuns em ordem cronológica rigorosa")
        self.logger.info("REGRAS: Download(8)->Upload(3)->Envio(1), SEM ultrapassagem")
        
        # Criar posições de fila para cada álbum
        queue_positions = {}
        for i, album in enumerate(sorted_albums):
            queue_positions[album.grouped_id] = QueuePosition(
                album_id=album.grouped_id,
                original_index=i
            )
        
        # Adicionar todos os álbuns na fila de download (ordem cronológica)
        for album in sorted_albums:
            self.download_queue.append(album.grouped_id)
        
        # Iniciar gerenciadores de fila
        await asyncio.gather(
            self.download_manager(sorted_albums, queue_positions),
            self.upload_manager(sorted_albums, queue_positions),
            self.send_manager(sorted_albums, queue_positions),
        )

    async def download_manager(self, sorted_albums: List[AlbumInfo], queue_positions: Dict[int, QueuePosition]):
        """Gerencia a fila de download - máximo 8 simultâneos, ordem rigorosa"""
        albums_dict = {album.grouped_id: album for album in sorted_albums}
        
        while self.download_queue or self.download_active:
            async with self.download_lock:
                # Iniciar novos downloads se há espaço na fila
                while len(self.download_active) < self.max_download_queue and self.download_queue:
                    album_id = self.download_queue.popleft()
                    album = albums_dict[album_id]
                    
                    if not album.downloaded:
                        self.download_active.add(album_id)
                        queue_positions[album_id].download_started = True
                        self.logger.info(f"[DOWNLOAD] Iniciando álbum {album_id} (posição {queue_positions[album_id].original_index})")
                        
                        # Iniciar download assíncrono
                        asyncio.create_task(self.download_worker(album, queue_positions[album_id]))
                    else:
                        # Álbum já foi baixado anteriormente
                        queue_positions[album_id].download_completed = True
                        self.logger.info(f"[DOWNLOAD] Álbum {album_id} já estava baixado")
            
            await asyncio.sleep(0.1)  # Pequeno delay para evitar busy waiting

    async def download_worker(self, album: AlbumInfo, position: QueuePosition):
        """Worker individual para download de um álbum"""
        try:
            await self.download_album_safe(album)
            album.downloaded = True
            await self.progress_tracker.save_album(album)
            
            async with self.download_lock:
                self.download_active.discard(album.grouped_id)
                position.download_completed = True
                
            self.logger.info(f"[DOWNLOAD] Concluído álbum {album.grouped_id} (posição {position.original_index})")
            
            # Tentar mover para fila de upload (respeitando ordem)
            await self.try_move_to_upload(album, position)
            
        except Exception as e:
            self.logger.error(f"[DOWNLOAD] Erro no álbum {album.grouped_id}: {e}")
            async with self.download_lock:
                self.download_active.discard(album.grouped_id)

    async def try_move_to_upload(self, album: AlbumInfo, position: QueuePosition):
        """Tenta mover álbum para fila de upload, respeitando ordem cronológica"""
        async with self.upload_lock:
            # Só pode mover se:
            # 1. Há espaço na fila de upload
            # 2. É o próximo álbum na ordem (não pode ultrapassar)
            if len(self.upload_active) < self.max_upload_queue:
                # Verificar se é o próximo álbum na ordem cronológica
                can_move = True
                for other_id, other_pos in [(aid, pos) for aid, pos in self.upload_queue]:
                    if other_pos.original_index < position.original_index:
                        can_move = False
                        break
                
                if can_move:
                    self.upload_queue.append((album.grouped_id, position))
                    self.upload_active.add(album.grouped_id)
                    position.upload_started = True
                    self.logger.info(f"[UPLOAD] Movido para fila: álbum {album.grouped_id} (posição {position.original_index})")
                    
                    # Iniciar upload
                    asyncio.create_task(self.upload_worker(album, position))
                else:
                    self.logger.info(f"[UPLOAD] Álbum {album.grouped_id} aguardando ordem (pos {position.original_index})")

    async def upload_manager(self, sorted_albums: List[AlbumInfo], queue_positions: Dict[int, QueuePosition]):
        """Gerencia a fila de upload - máximo 3 simultâneos, ordem rigorosa"""
        albums_dict = {album.grouped_id: album for album in sorted_albums}
        
        while True:
            # Verificar se há álbuns prontos para upload (download concluído + ordem correta)
            async with self.upload_lock:
                albums_ready = []
                for album in sorted_albums:
                    pos = queue_positions[album.grouped_id]
                    if (pos.download_completed and 
                        not pos.upload_started and 
                        len(self.upload_active) < self.max_upload_queue):
                        
                        # Verificar se não há álbuns anteriores ainda pendentes
                        can_start = True
                        for other_album in sorted_albums:
                            other_pos = queue_positions[other_album.grouped_id]
                            if (other_pos.original_index < pos.original_index and 
                                not other_pos.upload_completed):
                                can_start = False
                                break
                        
                        if can_start:
                            albums_ready.append((album, pos))
                
                # Iniciar uploads dos álbuns prontos
                for album, pos in albums_ready:
                    self.upload_active.add(album.grouped_id)
                    pos.upload_started = True
                    self.logger.info(f"[UPLOAD] Iniciando álbum {album.grouped_id} (posição {pos.original_index})")
                    asyncio.create_task(self.upload_worker(album, pos))
            
            # Verificar se todas as operações foram concluídas
            all_completed = all(pos.send_completed for pos in queue_positions.values())
            if all_completed:
                break
                
            await asyncio.sleep(0.2)

    async def upload_worker(self, album: AlbumInfo, position: QueuePosition):
        """Worker individual para upload de um álbum"""
        try:
            await self.upload_album_corrected(album)
            album.uploaded = True
            await self.progress_tracker.save_album(album)
            
            async with self.upload_lock:
                self.upload_active.discard(album.grouped_id)
                position.upload_completed = True
            
            self.logger.info(f"[UPLOAD] Concluído álbum {album.grouped_id} (posição {position.original_index})")
            
            # Tentar mover para fila de envio
            await self.try_move_to_send(album, position)
            
        except Exception as e:
            self.logger.error(f"[UPLOAD] Erro no álbum {album.grouped_id}: {e}")
            async with self.upload_lock:
                self.upload_active.discard(album.grouped_id)

    async def try_move_to_send(self, album: AlbumInfo, position: QueuePosition):
        """Tenta mover álbum para fila de envio (apenas 1 por vez, ordem rigorosa)"""
        async with self.send_lock:
            # Só pode mover se:
            # 1. Fila de envio está vazia
            # 2. É o próximo álbum na ordem cronológica
            if self.send_active is None:
                # Verificar se é realmente o próximo na ordem
                can_move = True
                for other_pos in self.upload_queue:
                    if other_pos[1].original_index < position.original_index and not other_pos[1].send_completed:
                        can_move = False
                        break
                
                if can_move:
                    self.send_active = album.grouped_id
                    self.logger.info(f"[ENVIO] Movido para fila: álbum {album.grouped_id} (posição {position.original_index})")
                    
                    # Iniciar envio
                    asyncio.create_task(self.send_worker(album, position))

    async def send_manager(self, sorted_albums: List[AlbumInfo], queue_positions: Dict[int, QueuePosition]):
        """Gerencia a fila de envio - apenas 1 por vez, ordem rigorosa"""
        while True:
            # Verificar álbums prontos para envio
            async with self.send_lock:
                if self.send_active is None:
                    # Procurar próximo álbum na ordem que está pronto para envio
                    for album in sorted_albums:
                        pos = queue_positions[album.grouped_id]
                        if (pos.upload_completed and not pos.send_completed):
                            # Verificar se todos os anteriores já foram enviados
                            can_send = True
                            for other_album in sorted_albums:
                                other_pos = queue_positions[other_album.grouped_id]
                                if (other_pos.original_index < pos.original_index and 
                                    not other_pos.send_completed):
                                    can_send = False
                                    break
                            
                            if can_send:
                                self.send_active = album.grouped_id
                                self.logger.info(f"[ENVIO] Iniciando álbum {album.grouped_id} (posição {pos.original_index})")
                                asyncio.create_task(self.send_worker(album, pos))
                                break
            
            # Verificar se todos foram enviados
            all_sent = all(pos.send_completed for pos in queue_positions.values())
            if all_sent:
                break
                
            await asyncio.sleep(0.3)

    async def send_worker(self, album: AlbumInfo, position: QueuePosition):
        """Worker para envio final do álbum"""
        try:
            # Aqui você pode adicionar lógica de envio final se necessário
            # Por enquanto, apenas marca como concluído pois o upload já enviou
            
            async with self.send_lock:
                self.send_active = None
                position.send_completed = True
            
            self.logger.info(f"[ENVIO] Concluído álbum {album.grouped_id} (posição {position.original_index})")
            
            # Limpar arquivos locais
            await self.cleanup_album_files(album)
            
        except Exception as e:
            self.logger.error(f"[ENVIO] Erro no álbum {album.grouped_id}: {e}")
            async with self.send_lock:
                self.send_active = None

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
                    largest_size = max(
                        message.media.photo.sizes,
                        key=lambda s: getattr(s, 'size', 0) if hasattr(s, 'size') else 0
                    )
                    file_size = getattr(largest_size, 'size', 0)
                file_name = f"photo_{message.id}.jpg"
            elif isinstance(message.media, MessageMediaDocument):
                doc = message.media.document
                file_size = getattr(doc, 'size', 0)
                if hasattr(doc, 'attributes'):
                    for attr in doc.attributes:
                        if hasattr(attr, 'file_name') and attr.file_name:
                            file_name = attr.file_name
                            break
                        elif hasattr(attr, 'performer') and hasattr(attr, 'title'):
                            file_name = f"{attr.performer} - {attr.title}.mp3"
                            media_type = "audio"
                            break
                    else:
                        if doc.mime_type:
                            if doc.mime_type.startswith('video/'):
                                media_type = "video"
                                file_name = f"video_{message.id}.mp4"
                            elif doc.mime_type.startswith('audio/'):
                                media_type = "audio"
                                file_name = f"audio_{message.id}.mp3"
                            elif doc.mime_type.startswith('image/'):
                                media_type = "photo"
                                file_name = f"image_{message.id}.jpg"
                            else:
                                media_type = "document"
                                file_name = f"doc_{message.id}"
                else:
                    media_type = "document"
                    file_name = f"doc_{message.id}"

        except Exception as e:
            self.logger.warning(f"Erro processando atributos da mídia: {e}")

        return MediaInfo(
            message_id=message.id,
            grouped_id=getattr(message, 'grouped_id', None),
            date=message.date,
            media_type=media_type,
            file_size=file_size,
            file_name=file_name,
            caption=getattr(message, 'message', None)
        )

    async def download_album_safe(self, album: AlbumInfo):
        """Download seguro de um álbum completo"""
        self.logger.info(f"[DOWNLOAD] Iniciando álbum {album.grouped_id} ({len(album.medias)} mídias)")
        
        for i, media in enumerate(album.medias):
            if media.downloaded:
                continue
                
            file_path = self.temp_dir / f"{album.grouped_id}_{i}_{media.file_name}"
            
            for attempt in range(self.max_retries):
                try:
                    # Buscar mensagem original
                    message = await self.safe_telegram_call(
                        self.client.get_messages,
                        self.source_chat_id,
                        ids=media.message_id
                    )
                    
                    if not message or not hasattr(message, 'media'):
                        self.logger.warning(f"Mensagem {media.message_id} não encontrada ou sem mídia")
                        break
                    
                    # Download da mídia
                    await asyncio.sleep(self.download_delay)
                    downloaded_file = await self.safe_telegram_call(
                        self.client.download_media,
                        message.media,
                        file=str(file_path)
                    )
                    
                    if downloaded_file and os.path.exists(downloaded_file):
                        media.local_path = downloaded_file
                        media.downloaded = True
                        self.logger.info(f"[DOWNLOAD] Mídia {i+1}/{len(album.medias)} baixada: {media.file_name}")
                        break
                    else:
                        raise Exception("Arquivo não foi baixado corretamente")
                        
                except Exception as e:
                    self.logger.warning(f"[DOWNLOAD] Tentativa {attempt+1} falhou para {media.file_name}: {e}")
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(2 ** attempt)
                    else:
                        self.logger.error(f"[DOWNLOAD] Falha definitiva para {media.file_name}")
                        raise

    async def upload_album_corrected(self, album: AlbumInfo):
        """Upload de um álbum respeitando rate limits"""
        self.logger.info(f"[UPLOAD] Iniciando álbum {album.grouped_id}")
        
        # Verificar rate limit
        current_time = time.time()
        time_since_last = current_time - self.last_upload_time
        if time_since_last < self.upload_delay:
            wait_time = self.upload_delay - time_since_last
            self.logger.info(f"[UPLOAD] Aguardando rate limit: {wait_time:.1f}s")
            await asyncio.sleep(wait_time)
        
        try:
            # Preparar arquivos para upload
            media_files = []
            for media in album.medias:
                if not media.local_path or not os.path.exists(media.local_path):
                    raise Exception(f"Arquivo local não encontrado: {media.file_name}")
                media_files.append(media.local_path)
            
            # Enviar como álbum
            caption = album.caption or f"Álbum transferido - {album.date.strftime('%Y-%m-%d %H:%M:%S')}"
            
            await self.safe_telegram_call(
                self.client.send_file,
                self.target_chat_id,
                media_files,
                caption=caption,
                force_document=False
            )
            
            self.last_upload_time = time.time()
            self.logger.info(f"[UPLOAD] Álbum {album.grouped_id} enviado com sucesso")
            
        except Exception as e:
            self.logger.error(f"[UPLOAD] Erro enviando álbum {album.grouped_id}: {e}")
            raise

    async def cleanup_album_files(self, album: AlbumInfo):
        """Limpa arquivos locais de um álbum"""
        for media in album.medias:
            if media.local_path and os.path.exists(media.local_path):
                try:
                    os.remove(media.local_path)
                    self.logger.debug(f"Arquivo removido: {media.local_path}")
                except Exception as e:
                    self.logger.warning(f"Erro removendo arquivo {media.local_path}: {e}")

    async def cleanup(self):
        """Limpeza final"""
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                self.logger.info("Diretório temporário removido")
        except Exception as e:
            self.logger.warning(f"Erro na limpeza final: {e}")
        
        if self.client.is_connected():
            await self.client.disconnect()
            self.logger.info("Cliente Telegram desconectado")

async def main():
    """Função principal"""
    # Configurações - AJUSTE AQUI
    API_ID = 20372456  # Seu API ID
    API_HASH = "4bf8017e548b790415a11cc8ed1b9804"  # Sua API Hash
    SESSION_NAME = "album_transfer_session"  # Nome da sessão
    SOURCE_CHAT_ID = -1001781722146  # ID do chat de origem
    TARGET_CHAT_ID = -1002608875175  # ID do chat de destino
    
    # Criar instância do transferidor
    transfer = TelegramAlbumTransfer(
        api_id=API_ID,
        api_hash=API_HASH,
        session_name=SESSION_NAME,
        source_chat_id=SOURCE_CHAT_ID,
        target_chat_id=TARGET_CHAT_ID,
        max_download_queue=8,  # 8 downloads simultâneos
        max_upload_queue=3,    # 3 uploads simultâneos
        temp_dir="./temp_media",
        progress_db="./transfer_progress.db"
    )
    
    try:
        await transfer.start()
    except KeyboardInterrupt:
        logging.info("Transferência interrompida pelo usuário")
    except Exception as e:
        logging.error(f"Erro na transferência: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())