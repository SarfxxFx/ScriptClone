#!/usr/bin/env python3
"""
Versão otimizada do script de transferência de álbuns do Telegram.
Sistema de 3 filas: Download (8), Upload (3), Envio (1), com ordem absoluta e sem ultrapassagem.
CORREÇÃO: Implementação rigorosa de ordem cronológica e priorização por ID.
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

    async def scan_messages_chronological(self):
        self.logger.info("Iniciando escaneamento cronológico completo...")
        try:
            # Verificar se o chat existe e temos acesso
            chat_info = await self.safe_telegram_call(self.client.get_entity, self.source_chat_id)
            self.logger.info(f"Chat: {getattr(chat_info, 'title', 'Chat privado')} (ID: {self.source_chat_id})")
            
            # Verificar se temos permissão para ler mensagens
            if not await self.client.get_permissions(self.source_chat_id).can_read_messages:
                raise Exception(f"Sem permissão para ler mensagens do chat {self.source_chat_id}")
            
        except Exception as e:
            self.logger.error(f"Erro ao verificar chat de origem: {e}")
            raise

        self.logger.info("Coletando todas as mensagens com mídia...")
        all_messages = []
        message_count = 0
        batch_count = 0
        last_message_id = None
        
        try:
            async for message in self.client.iter_messages(
                self.source_chat_id,
                limit=None,
                reverse=True,  # Importante: mantém a ordem cronológica
                min_id=1,     # Começa da primeira mensagem
                wait_time=1   # Adiciona pequeno delay para evitar rate limits
            ):
                # Verificar se a mensagem pertence ao chat correto
                if message.chat_id != self.source_chat_id:
                    self.logger.warning(f"Mensagem {message.id} não pertence ao chat de origem!")
                    continue
                    
                # Verificar se os IDs estão em ordem crescente
                if last_message_id and message.id <= last_message_id:
                    self.logger.warning(f"Sequência de IDs inconsistente: {last_message_id} -> {message.id}")
                    continue
                    
                last_message_id = message.id
                message_count += 1
                batch_count += 1
                
                if message_count <= 5 or message_count % 10000 == 0:
                    self.logger.info(f"Mensagem {message_count}: ID={message.id}, Data={message.date}, Chat={message.chat_id}")
                
                if hasattr(message, 'media') and message.media:
                    all_messages.append(message)
                    
                if batch_count >= 200:
                    await asyncio.sleep(1.2)
                    batch_count = 0
                    
                if message_count % 5000 == 0:
                    self.logger.info(f"Coletadas {message_count} mensagens... ({len(all_messages)} com mídia)")
                    
        except Exception as e:
            self.logger.error(f"Erro durante coleta de mensagens: {e}")
            raise

        if all_messages:
            self.logger.info("\nInformações das mensagens coletadas:")
            self.logger.info(f"Total de mensagens processadas: {message_count}")
            self.logger.info(f"Total de mensagens com mídia: {len(all_messages)}")
            self.logger.info(f"Primeira mensagem: ID={all_messages[0].id}, Data={all_messages[0].date}, Chat={all_messages[0].chat_id}")
            self.logger.info(f"Última mensagem: ID={all_messages[-1].id}, Data={all_messages[-1].date}, Chat={all_messages[-1].chat_id}")
            
            # Verificação adicional de integridade
            chat_ids = set(m.chat_id for m in all_messages)
            if len(chat_ids) > 1:
                self.logger.error(f"ERRO: Encontradas mensagens de múltiplos chats: {chat_ids}")
                raise Exception("Violação de segurança: mensagens de múltiplos chats detectadas")
                
            # Verificar se todos os IDs são do chat correto
            invalid_msgs = [m for m in all_messages if m.chat_id != self.source_chat_id]
            if invalid_msgs:
                self.logger.error(f"ERRO: Encontradas {len(invalid_msgs)} mensagens de outros chats!")
                raise Exception("Violação de segurança: mensagens de outros chats detectadas")
        
        all_messages.sort(key=lambda x: (x.date.timestamp(), x.id))
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
                loose_album_buffer.sort(key=lambda x: x.message_id)
                timestamp = int(loose_album_buffer[0].date.timestamp())
                msg_id = loose_album_buffer[0].message_id
                grouped_id = -(int(f"{timestamp}{msg_id:010}"))
                
                # Log para debug
                self.logger.debug(
                    f"Criando álbum: timestamp={timestamp}, "
                    f"msg_id={msg_id}, grouped_id={grouped_id}"
                )
                
                for m in loose_album_buffer:
                    m.grouped_id = grouped_id
                album_groups[grouped_id].extend(loose_album_buffer)
            loose_album_buffer.clear()

        messages.sort(key=lambda x: (x.date.timestamp(), x.id))
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
                # Sempre recriar o ID no formato padronizado
                timestamp = int(message.date.timestamp())
                grouped_id = -(int(f"{timestamp}{message.id:010}"))
                media_info.grouped_id = grouped_id
                
                # Log para debug
                self.logger.debug(
                    f"Usando grouped_id existente: original={media_info.grouped_id}, "
                    f"novo={grouped_id}, msg_id={message.id}"
                )
                
                album_groups[grouped_id].append(media_info)
                previous = None
            else:
                if previous:
                    same_second = abs((media_info.date - previous.date).total_seconds()) < 1
                    same_type = media_info.media_type == previous.media_type
                    if same_second and same_type:
                        loose_album_buffer.append(media_info)
                    else:
                        flush_loose_album()
                        loose_album_buffer.append(media_info)
                else:
                    loose_album_buffer.append(media_info)
                previous = media_info

        flush_loose_album()

        # Ordenar as mídias dentro de cada álbum por ID
        for grouped_id, medias in album_groups.items():
            medias.sort(key=lambda x: x.message_id)

        self.logger.info(f"Encontradas {media_count} mídias, {len(album_groups)} grupos potenciais")

        valid_albums = 0
        batch_albums = []
        sorted_groups = sorted(
            album_groups.items(), 
            key=lambda x: (min(m.date.timestamp() for m in x[1]), min(m.message_id for m in x[1]))
        )
        
        for grouped_id, medias in sorted_groups:
            if len(medias) >= 2:
                album = AlbumInfo(
                    grouped_id=grouped_id,
                    medias=medias,
                    caption=next((m.caption for m in medias if m.caption), None),
                    date=medias[0].date
                )
                self.albums[grouped_id] = album
                batch_albums.append(album)
                valid_albums += 1
                
                if valid_albums <= 5:
                    self.logger.info(
                        f"Álbum {valid_albums}: ID={grouped_id}, "
                        f"Data={medias[0].date.strftime('%Y-%m-%d %H:%M:%S')}, "
                        f"Primeiro ID={medias[0].message_id}, "
                        f"Mídias={len(medias)}"
                    )
                
                if len(batch_albums) >= 100:
                    await self.progress_tracker.save_albums_batch(batch_albums)
                    batch_albums = []

        if batch_albums:
            await self.progress_tracker.save_albums_batch(batch_albums)

        self.logger.info(f"Álbuns válidos criados: {valid_albums}")

    # [O resto do código permanece igual ao enviado anteriormente...]

async def main():
    """Função principal"""
    # Configurações
    API_ID = 20372456
    API_HASH = "4bf8017e548b790415a11cc8ed1b9804"
    SESSION_NAME = "album_transfer_session"
    SOURCE_CHAT_ID = -1001781722146
    TARGET_CHAT_ID = -1002608875175
    
    # Criar instância do transferidor
    transfer = TelegramAlbumTransfer(
        api_id=API_ID,
        api_hash=API_HASH,
        session_name=SESSION_NAME,
        source_chat_id=SOURCE_CHAT_ID,
        target_chat_id=TARGET_CHAT_ID,
        max_download_queue=8,
        max_upload_queue=3,
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