import asyncio
import logging
import os
import json
import threading
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Set
import time
from flask import Flask, render_template, jsonify, request, send_from_directory
from flask_socketio import SocketIO
import glob

from TikTokLive import TikTokLiveClient
from TikTokLive.client.logger import LogLevel
from TikTokLive.events import (
    ConnectEvent, DisconnectEvent, LiveEndEvent,
    CommentEvent, GiftEvent, LikeEvent, FollowEvent,
    JoinEvent, ShareEvent, SubscribeEvent,
    RoomUserSeqEvent
)
from TikTokLive.client.web.routes.fetch_room_id_live_html import FailedParseRoomIdError

# 配置日志 - 使用轮转日志避免文件过大
from logging.handlers import RotatingFileHandler
import os

def setup_logging():
    """设置日志配置，支持文件轮转"""
    
    # 从环境变量获取日志级别，默认为INFO
    log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
    level = getattr(logging, log_level, logging.INFO)
    
    # 创建日志格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 创建轮转文件处理器
    # 最大1MB，保留5个备份文件
    file_handler = RotatingFileHandler(
        'tiktok_monitor.log',
        maxBytes=1*1024*1024,  # 1MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 配置根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    # 减少第三方库的日志级别
    logging.getLogger('werkzeug').setLevel(logging.WARNING)  # Flask的日志
    logging.getLogger('socketio').setLevel(logging.WARNING)  # SocketIO的日志
    logging.getLogger('engineio').setLevel(logging.WARNING)  # EngineIO的日志
    logging.getLogger('urllib3').setLevel(logging.WARNING)   # HTTP请求的日志
    
    return logging.getLogger(__name__)

# 设置日志
logger = setup_logging()

# 创建数据存储目录
os.makedirs('stats', exist_ok=True)
os.makedirs('history', exist_ok=True)
os.makedirs('history/daily', exist_ok=True)
os.makedirs('history/sessions', exist_ok=True)
os.makedirs('history/analytics', exist_ok=True)
os.makedirs('config', exist_ok=True)

# 初始化Flask应用
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# 存储所有活跃的监控实例
active_monitors = {}
monitor_threads = {}

# 并发监控管理配置
CONCURRENT_CONFIG = {
    'max_concurrent_connections': 3,  # 最大同时连接数（降低避免限流）
    'update_interval_base': 120,      # 基础更新间隔（秒）- 大幅增加避免限流
    'stagger_delay': 15,              # 错开启动延迟（秒）- 增加避免并发请求
    'rate_limit_backoff': 300,        # 遇到限流时的退避时间（秒）
    'max_retries_per_hour': 20,       # 每小时最大重试次数
}

# 代理配置（可选）
PROXY_CONFIG = {
    'enabled': False,
    'http_proxy': None,   # 例如: 'http://127.0.0.1:7890'
    'https_proxy': None,  # 例如: 'http://127.0.0.1:7890'
}

# 限流计数器
rate_limit_tracker = {
    'last_reset': datetime.now(),
    'request_count': 0,
    'is_rate_limited': False,
    'rate_limit_until': None,
}

# ==================== 账号池轮换系统 ====================
class SessionPool:
    """
    TikTok账号池管理器
    支持多个sessionid轮换使用，避免单个账号被限流
    """
    
    def __init__(self):
        self.config_file = 'config/session_pool.json'
        self.sessions = []  # [{sessionid, name, added_at, last_used, use_count, is_active, is_rate_limited}]
        self.current_index = 0
        self.rotation_interval = 1800  # 默认30分钟轮换一次（秒）
        self.last_rotation = datetime.now()
        self.load_sessions()
    
    def load_sessions(self):
        """从配置文件加载账号池"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.sessions = data.get('sessions', [])
                    self.rotation_interval = data.get('rotation_interval', 1800)
                    self.current_index = data.get('current_index', 0) % max(len(self.sessions), 1)
                    logger.info(f"已加载 {len(self.sessions)} 个TikTok账号到账号池")
        except Exception as e:
            logger.error(f"加载账号池配置失败: {e}")
            self.sessions = []
    
    def save_sessions(self):
        """保存账号池到配置文件"""
        try:
            os.makedirs('config', exist_ok=True)
            data = {
                'sessions': self.sessions,
                'rotation_interval': self.rotation_interval,
                'current_index': self.current_index,
                'last_updated': datetime.now().isoformat()
            }
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info(f"账号池配置已保存，共 {len(self.sessions)} 个账号")
        except Exception as e:
            logger.error(f"保存账号池配置失败: {e}")
    
    def add_session(self, sessionid: str, name: str = None):
        """添加一个新的sessionid到账号池"""
        # 检查是否已存在
        for s in self.sessions:
            if s['sessionid'] == sessionid:
                logger.warning(f"Session已存在: {name or sessionid[:20]}...")
                return False
        
        session_data = {
            'sessionid': sessionid,
            'name': name or f"账号{len(self.sessions) + 1}",
            'added_at': datetime.now().isoformat(),
            'last_used': None,
            'use_count': 0,
            'is_active': True,
            'is_rate_limited': False,
            'rate_limit_until': None
        }
        self.sessions.append(session_data)
        self.save_sessions()
        logger.info(f"已添加账号到池: {session_data['name']}")
        return True
    
    def remove_session(self, sessionid: str):
        """从账号池移除一个sessionid"""
        original_len = len(self.sessions)
        self.sessions = [s for s in self.sessions if s['sessionid'] != sessionid]
        if len(self.sessions) < original_len:
            self.save_sessions()
            logger.info(f"已从账号池移除一个账号")
            return True
        return False
    
    def get_current_session(self) -> Optional[str]:
        """获取当前应使用的sessionid"""
        if not self.sessions:
            return None
        
        # 检查是否需要轮换
        self._check_rotation()
        
        # 找到一个可用的session（未被限流的）
        attempts = 0
        while attempts < len(self.sessions):
            session = self.sessions[self.current_index]
            
            # 检查是否被限流
            if session.get('is_rate_limited'):
                rate_limit_until = session.get('rate_limit_until')
                if rate_limit_until:
                    try:
                        limit_time = datetime.fromisoformat(rate_limit_until)
                        if datetime.now() >= limit_time:
                            # 限流已解除
                            session['is_rate_limited'] = False
                            session['rate_limit_until'] = None
                            self.save_sessions()
                        else:
                            # 仍在限流中，切换到下一个
                            self._rotate()
                            attempts += 1
                            continue
                    except:
                        session['is_rate_limited'] = False
            
            if session.get('is_active', True):
                return session['sessionid']
            
            self._rotate()
            attempts += 1
        
        # 所有账号都被限流，返回第一个
        if self.sessions:
            return self.sessions[0]['sessionid']
        return None
    
    def get_current_session_info(self) -> dict:
        """获取当前账号的详细信息"""
        if not self.sessions:
            return {'name': '无账号', 'index': 0, 'total': 0}
        
        session = self.sessions[self.current_index]
        return {
            'name': session.get('name', f'账号{self.current_index + 1}'),
            'index': self.current_index + 1,
            'total': len(self.sessions),
            'use_count': session.get('use_count', 0),
            'last_used': session.get('last_used'),
            'is_rate_limited': session.get('is_rate_limited', False)
        }
    
    def _check_rotation(self):
        """检查是否需要轮换账号"""
        if len(self.sessions) <= 1:
            return
        
        elapsed = (datetime.now() - self.last_rotation).total_seconds()
        if elapsed >= self.rotation_interval:
            self._rotate()
            logger.info(f"账号池自动轮换，当前使用: {self.get_current_session_info()['name']}")
    
    def _rotate(self):
        """轮换到下一个账号"""
        if len(self.sessions) <= 1:
            return
        
        self.current_index = (self.current_index + 1) % len(self.sessions)
        self.last_rotation = datetime.now()
        
        # 更新使用记录
        session = self.sessions[self.current_index]
        session['last_used'] = datetime.now().isoformat()
        session['use_count'] = session.get('use_count', 0) + 1
        self.save_sessions()
    
    def mark_rate_limited(self, sessionid: str, duration_seconds: int = 1800):
        """标记某个账号被限流"""
        for session in self.sessions:
            if session['sessionid'] == sessionid:
                session['is_rate_limited'] = True
                session['rate_limit_until'] = (datetime.now() + timedelta(seconds=duration_seconds)).isoformat()
                self.save_sessions()
                logger.warning(f"账号 {session['name']} 被标记为限流，{duration_seconds}秒后恢复")
                
                # 自动切换到下一个账号
                self._rotate()
                return True
        return False
    
    def force_rotate(self):
        """强制轮换到下一个账号"""
        self._rotate()
        return self.get_current_session_info()
    
    def set_rotation_interval(self, seconds: int):
        """设置轮换间隔（秒）"""
        self.rotation_interval = max(60, seconds)  # 最少1分钟
        self.save_sessions()
        logger.info(f"账号池轮换间隔已设置为 {self.rotation_interval} 秒")
    
    def get_all_sessions(self) -> list:
        """获取所有账号信息（隐藏完整sessionid）"""
        result = []
        for i, s in enumerate(self.sessions):
            result.append({
                'index': i,
                'name': s.get('name', f'账号{i+1}'),
                'sessionid_preview': s['sessionid'][:10] + '...' + s['sessionid'][-6:] if len(s['sessionid']) > 20 else s['sessionid'],
                'added_at': s.get('added_at'),
                'last_used': s.get('last_used'),
                'use_count': s.get('use_count', 0),
                'is_active': s.get('is_active', True),
                'is_rate_limited': s.get('is_rate_limited', False),
                'is_current': i == self.current_index
            })
        return result
    
    def get_stats(self) -> dict:
        """获取账号池统计信息"""
        active_count = sum(1 for s in self.sessions if s.get('is_active', True))
        limited_count = sum(1 for s in self.sessions if s.get('is_rate_limited', False))
        
        return {
            'total_sessions': len(self.sessions),
            'active_sessions': active_count,
            'rate_limited_sessions': limited_count,
            'current_index': self.current_index + 1,
            'rotation_interval': self.rotation_interval,
            'rotation_interval_minutes': self.rotation_interval // 60,
            'last_rotation': self.last_rotation.isoformat(),
            'next_rotation': (self.last_rotation + timedelta(seconds=self.rotation_interval)).isoformat()
        }

# 创建全局账号池实例
session_pool = SessionPool()

class ConfigManager:
    """配置管理类"""
    
    def __init__(self):
        self.config_file = 'config/monitor_config.json'
        self.ensure_directories()
    
    def ensure_directories(self):
        """确保配置目录存在"""
        os.makedirs('config', exist_ok=True)
    
    def save_monitor_config(self, monitor_configs: list):
        """保存监控配置"""
        try:
            config_data = {
                'monitors': monitor_configs,
                'saved_at': datetime.now().isoformat(),
                'version': '1.0'
            }
            
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"监控配置已保存: {len(monitor_configs)} 个账号")
            return True
        except Exception as e:
            logger.error(f"保存监控配置失败: {str(e)}")
            return False
    
    def load_monitor_config(self) -> list:
        """加载监控配置"""
        try:
            if not os.path.exists(self.config_file):
                logger.info("监控配置文件不存在，返回空配置")
                return []
            
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            monitors = config_data.get('monitors', [])
            logger.info(f"加载监控配置: {len(monitors)} 个账号")
            return monitors
        except Exception as e:
            logger.error(f"加载监控配置失败: {str(e)}")
            return []
    
    def add_monitor_to_config(self, unique_id: str, sessionid: str = None, anti_detection: bool = False, use_session_pool: bool = False):
        """添加监控到配置"""
        try:
            configs = self.load_monitor_config()
            
            # 检查是否已存在
            for config in configs:
                if config['unique_id'] == unique_id:
                    # 更新现有配置
                    config['sessionid'] = sessionid
                    config['anti_detection'] = anti_detection
                    config['use_session_pool'] = use_session_pool
                    config['updated_at'] = datetime.now().isoformat()
                    self.save_monitor_config(configs)
                    return True
            
            # 添加新配置
            new_config = {
                'unique_id': unique_id,
                'sessionid': sessionid,
                'anti_detection': anti_detection,
                'use_session_pool': use_session_pool,
                'added_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            configs.append(new_config)
            self.save_monitor_config(configs)
            return True
        except Exception as e:
            logger.error(f"添加监控配置失败: {str(e)}")
            return False
    
    def remove_monitor_from_config(self, unique_id: str):
        """从配置中移除监控"""
        try:
            configs = self.load_monitor_config()
            original_count = len(configs)
            
            configs = [c for c in configs if c['unique_id'] != unique_id]
            
            if len(configs) < original_count:
                self.save_monitor_config(configs)
                logger.info(f"从配置中移除监控: {unique_id}")
                return True
            else:
                logger.warning(f"配置中未找到监控: {unique_id}")
                return False
        except Exception as e:
            logger.error(f"移除监控配置失败: {str(e)}")
            return False

# 创建配置管理器实例
config_manager = ConfigManager()

class HistoryDataManager:
    """历史数据管理类 - 重构版本"""
    
    def __init__(self):
        self.base_path = 'history'
        self.index_file = os.path.join(self.base_path, 'index.json')
        self.ensure_directories()
    
    def ensure_directories(self):
        """确保所有必要的目录存在"""
        os.makedirs(self.base_path, exist_ok=True)
        
        # 创建索引文件（如果不存在）
        if not os.path.exists(self.index_file):
            self._create_index_file()
    
    def _create_index_file(self):
        """创建索引文件"""
        index_data = {
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'total_sessions': 0,
            'total_users': 0,
            'users': {}  # user_id: {total_sessions, last_session, first_session}
        }
        with open(self.index_file, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)
    
    def _update_index(self, unique_id: str, session_id: str):
        """更新索引文件"""
        try:
            with open(self.index_file, 'r', encoding='utf-8') as f:
                index_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            self._create_index_file()
            with open(self.index_file, 'r', encoding='utf-8') as f:
                index_data = json.load(f)
        
        # 更新用户信息
        if unique_id not in index_data['users']:
            index_data['users'][unique_id] = {
                'total_sessions': 0,
                'first_session': session_id,
                'last_session': session_id,
                'created_at': datetime.now().isoformat()
            }
            index_data['total_users'] += 1
        
        index_data['users'][unique_id]['total_sessions'] += 1
        index_data['users'][unique_id]['last_session'] = session_id
        index_data['users'][unique_id]['updated_at'] = datetime.now().isoformat()
        index_data['total_sessions'] += 1
        index_data['last_updated'] = datetime.now().isoformat()
        
        with open(self.index_file, 'w', encoding='utf-8') as f:
            json.dump(index_data, f, ensure_ascii=False, indent=2)
    
    def get_user_directory(self, unique_id: str) -> str:
        """获取用户数据目录"""
        user_dir = os.path.join(self.base_path, unique_id)
        os.makedirs(user_dir, exist_ok=True)
        return user_dir
    
    def generate_session_id(self) -> str:
        """生成会话ID"""
        return f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def save_session_data(self, unique_id: str, live_data: dict):
        """保存单次直播会话数据（新结构）"""
        try:
            session_id = self.generate_session_id()
            user_dir = self.get_user_directory(unique_id)
            session_file = os.path.join(user_dir, f"{session_id}.json")
            
            # 构建优化的数据结构
            session_data = {
                # 基本信息
                'session_id': session_id,
                'unique_id': unique_id,
                'start_time': live_data.get('start_time'),
                'end_time': datetime.now().isoformat(),
                'duration_seconds': live_data.get('duration', 0),
                'room_id': live_data.get('room_id'),
                'title': live_data.get('title', ''),
                'stream_status': live_data.get('stream_status', 0),
                
                # 总体统计（只记录数量）
                'summary': {
                    'peak_viewers': live_data.get('current_viewers', 0),
                    'total_viewers': live_data.get('total_viewers', 0),
                    'total_likes': live_data.get('like_count', 0),
                    'total_comments': live_data.get('comment_count', 0),
                    'total_gifts': live_data.get('gift_count', 0),
                    'total_gift_value': live_data.get('gift_value', 0),
                    'total_follows': live_data.get('follow_count', 0),
                    'total_shares': live_data.get('share_count', 0),
                    'total_subscribes': live_data.get('subscribe_count', 0),
                    'unique_gift_types': len(live_data.get('gifts_detail', {}))
                },
                
                # 每分钟数据（压缩存储）
                'minute_data': self._compress_minute_data(live_data.get('minute_data', [])),
                
                # 图表数据（压缩存储）
                'chart_data': {
                    'timestamps': live_data.get('chart_data', {}).get('timestamps', [])[-60:],  # 只保留最后60个点
                    'viewer_counts': live_data.get('chart_data', {}).get('viewer_counts', [])[-60:],
                    'like_counts': live_data.get('chart_data', {}).get('like_counts', [])[-60:],
                    'comment_counts': live_data.get('chart_data', {}).get('comment_counts', [])[-60:]
                },
                
                # 配置信息
                'settings': {
                    'anti_detection_used': live_data.get('anti_detection_used', False),
                    'custom_interval': getattr(active_monitors.get(unique_id), 'custom_interval', None)
                },
                
                # 元数据
                'metadata': {
                    'created_at': datetime.now().isoformat(),
                    'file_version': '2.0',
                    'data_optimized': True
                }
            }
            
            # 保存会话数据
            with open(session_file, 'w', encoding='utf-8') as f:
                json.dump(session_data, f, ensure_ascii=False, indent=2)
            
            # 更新索引
            self._update_index(unique_id, session_id)
            
            logger.info(f"会话数据已保存: {session_file}")
            return session_file
            
        except Exception as e:
            logger.error(f"保存会话数据失败: {str(e)}")
            return None
    
    def _compress_minute_data(self, minute_data: list) -> list:
        """压缩每分钟数据，只保留关键信息"""
        compressed = []
        for data in minute_data:
            if isinstance(data, dict):
                compressed.append({
                    't': data.get('minute', ''),  # 时间（简化键名）
                    'v': data.get('current_viewers', 0),  # 观众数
                    'l': data.get('like_count', 0),  # 点赞数
                    'c': data.get('comment_count', 0),  # 评论数
                    'g': data.get('gift_count', 0),  # 礼物数
                    'f': data.get('follow_count', 0),  # 关注数
                    's': data.get('share_count', 0)  # 分享数
                })
        return compressed
    
    def get_user_sessions(self, unique_id: str, limit: int = None) -> list:
        """获取用户的直播会话列表"""
        try:
            user_dir = self.get_user_directory(unique_id)
            if not os.path.exists(user_dir):
                return []
            
            sessions = []
            for filename in os.listdir(user_dir):
                if filename.endswith('.json') and filename.startswith('session_'):
                    session_file = os.path.join(user_dir, filename)
                    try:
                        with open(session_file, 'r', encoding='utf-8') as f:
                            session_data = json.load(f)
                            sessions.append({
                                'session_id': session_data.get('session_id'),
                                'start_time': session_data.get('start_time'),
                                'end_time': session_data.get('end_time'),
                                'duration_seconds': session_data.get('duration_seconds', 0),
                                'peak_viewers': session_data.get('summary', {}).get('peak_viewers', 0),
                                'total_likes': session_data.get('summary', {}).get('total_likes', 0),
                                'filename': filename
                            })
                    except (json.JSONDecodeError, FileNotFoundError):
                        logger.warning(f"无法读取会话文件: {session_file}")
                        continue
            
            # 按开始时间排序
            sessions.sort(key=lambda x: x.get('start_time', ''), reverse=True)
            
            if limit:
                sessions = sessions[:limit]
            
            return sessions
        except Exception as e:
            logger.error(f"获取用户会话失败: {str(e)}")
            return []
    
    def get_session_detail(self, unique_id: str, session_id: str) -> dict:
        """获取指定会话的详细数据"""
        try:
            user_dir = self.get_user_directory(unique_id)
            session_file = os.path.join(user_dir, f"{session_id}.json")
            
            if not os.path.exists(session_file):
                return None
            
            with open(session_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"获取会话详情失败: {str(e)}")
            return None
    
    def get_user_analytics(self, unique_id: str, days: int = 30) -> dict:
        """获取用户数据分析"""
        try:
            sessions = self.get_user_sessions(unique_id)
            if not sessions:
                return {
                    'total_sessions': 0,
                    'total_duration': 0,
                    'avg_duration': 0,
                    'peak_viewers': 0,
                    'avg_viewers': 0,
                    'total_interactions': 0,
                    'sessions_by_hour': {},
                    'trend': 'stable'
                }
            
            # 过滤指定天数内的数据
            cutoff_date = datetime.now() - timedelta(days=days)
            recent_sessions = []
            
            for session in sessions:
                try:
                    session_start = datetime.fromisoformat(session['start_time'])
                    if session_start >= cutoff_date:
                        # 获取详细数据
                        detail = self.get_session_detail(unique_id, session['session_id'])
                        if detail:
                            recent_sessions.append(detail)
                except Exception:
                    continue
            
            if not recent_sessions:
                return {'total_sessions': 0}
            
            # 计算分析数据
            total_sessions = len(recent_sessions)
            total_duration = sum(s.get('duration_seconds', 0) for s in recent_sessions)
            avg_duration = total_duration / total_sessions if total_sessions > 0 else 0
            
            peak_viewers = max(s.get('summary', {}).get('peak_viewers', 0) for s in recent_sessions)
            avg_viewers = sum(s.get('summary', {}).get('peak_viewers', 0) for s in recent_sessions) / total_sessions
            
            total_interactions = sum(
                s.get('summary', {}).get('total_likes', 0) +
                s.get('summary', {}).get('total_comments', 0) +
                s.get('summary', {}).get('total_gifts', 0)
                for s in recent_sessions
            )
            
            # 按小时统计
            sessions_by_hour = {}
            for session in recent_sessions:
                try:
                    start_time = datetime.fromisoformat(session['start_time'])
                    hour = start_time.hour
                    sessions_by_hour[hour] = sessions_by_hour.get(hour, 0) + 1
                except Exception:
                    continue
            
            # 计算趋势
            if len(recent_sessions) >= 2:
                recent_avg = sum(s.get('summary', {}).get('peak_viewers', 0) for s in recent_sessions[:5]) / min(5, len(recent_sessions))
                older_avg = sum(s.get('summary', {}).get('peak_viewers', 0) for s in recent_sessions[-5:]) / min(5, len(recent_sessions))
                
                if recent_avg > older_avg * 1.1:
                    trend = 'rising'
                elif recent_avg < older_avg * 0.9:
                    trend = 'falling'
                else:
                    trend = 'stable'
            else:
                trend = 'stable'
            
            return {
                'total_sessions': total_sessions,
                'total_duration': total_duration,
                'avg_duration': avg_duration / 60,  # 转换为分钟
                'peak_viewers': peak_viewers,
                'avg_viewers': round(avg_viewers, 1),
                'total_interactions': total_interactions,
                'sessions_by_hour': sessions_by_hour,
                'trend': trend,
                'days_analyzed': days
            }
            
        except Exception as e:
            logger.error(f"获取用户分析失败: {str(e)}")
            return {'error': str(e)}
    
    def get_all_users(self) -> list:
        """获取所有用户列表"""
        try:
            with open(self.index_file, 'r', encoding='utf-8') as f:
                index_data = json.load(f)
                return list(index_data.get('users', {}).keys())
        except Exception:
            # 如果索引文件损坏，扫描目录
            users = []
            for item in os.listdir(self.base_path):
                item_path = os.path.join(self.base_path, item)
                if os.path.isdir(item_path) and item not in ['exports', 'temp']:
                    users.append(item)
            return users
    
    def clear_history_data(self, unique_id: str = None, confirm_token: str = None):
        """清除历史数据"""
        if confirm_token != "CONFIRM_DELETE":
            raise ValueError("需要确认令牌才能删除历史数据")
        
        try:
            deleted_files = 0
            
            if unique_id:
                # 删除指定用户的数据
                user_dir = self.get_user_directory(unique_id)
                if os.path.exists(user_dir):
                    for filename in os.listdir(user_dir):
                        file_path = os.path.join(user_dir, filename)
                        if os.path.isfile(file_path):
                            os.remove(file_path)
                            deleted_files += 1
                    os.rmdir(user_dir)
                    
                    # 更新索引
                    try:
                        with open(self.index_file, 'r', encoding='utf-8') as f:
                            index_data = json.load(f)
                        if unique_id in index_data.get('users', {}):
                            del index_data['users'][unique_id]
                            index_data['total_users'] -= 1
                            index_data['last_updated'] = datetime.now().isoformat()
                            with open(self.index_file, 'w', encoding='utf-8') as f:
                                json.dump(index_data, f, ensure_ascii=False, indent=2)
                    except Exception:
                        pass
                    
                logger.info(f"用户 {unique_id} 的历史数据删除完成，共删除 {deleted_files} 个文件")
            else:
                # 删除所有历史数据
                for user in self.get_all_users():
                    user_dir = os.path.join(self.base_path, user)
                    if os.path.exists(user_dir) and os.path.isdir(user_dir):
                        for filename in os.listdir(user_dir):
                            file_path = os.path.join(user_dir, filename)
                            if os.path.isfile(file_path):
                                os.remove(file_path)
                                deleted_files += 1
                        os.rmdir(user_dir)
                
                # 重新创建索引文件
                self._create_index_file()
                
                logger.info(f"所有历史数据删除完成，共删除 {deleted_files} 个文件")
            
            return {
                'success': True,
                'deleted_files': deleted_files,
                'message': f'成功删除 {deleted_files} 个历史数据文件'
            }
        except Exception as e:
            logger.error(f"删除历史数据失败: {str(e)}")
            return {
                'success': False,
                'error': str(e),
                'message': '删除历史数据时发生错误'
            }
    
    def get_data_usage_stats(self):
        """获取数据使用统计"""
        try:
            stats = {
                'total_users': 0,
                'total_sessions': 0,
                'total_size_mb': 0,
                'users': [],
                'date_range': {'earliest': None, 'latest': None}
            }
            
            users = self.get_all_users()
            stats['total_users'] = len(users)
            
            for user in users:
                user_dir = os.path.join(self.base_path, user)
                if os.path.exists(user_dir):
                    user_sessions = 0
                    for filename in os.listdir(user_dir):
                        if filename.endswith('.json') and filename.startswith('session_'):
                            user_sessions += 1
                            file_path = os.path.join(user_dir, filename)
                            stats['total_size_mb'] += os.path.getsize(file_path)
                            
                            # 提取日期信息
                            try:
                                session_id = filename.replace('.json', '')
                                date_part = session_id.split('_')[1]  # session_20231210_143052
                                date_str = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}"
                                
                                if stats['date_range']['earliest'] is None or date_str < stats['date_range']['earliest']:
                                    stats['date_range']['earliest'] = date_str
                                if stats['date_range']['latest'] is None or date_str > stats['date_range']['latest']:
                                    stats['date_range']['latest'] = date_str
                            except Exception:
                                pass
                    
                    stats['total_sessions'] += user_sessions
                    stats['users'].append(user)
            
            # 转换为MB
            stats['total_size_mb'] = round(stats['total_size_mb'] / (1024 * 1024), 2)
            
            # 添加索引文件大小
            if os.path.exists(self.index_file):
                stats['total_size_mb'] += round(os.path.getsize(self.index_file) / (1024 * 1024), 4)
            
            return stats
        except Exception as e:
            logger.error(f"获取数据使用统计失败: {str(e)}")
            return None
    
    def export_user_data(self, unique_id: str, format: str = 'json') -> str:
        """导出用户数据"""
        try:
            sessions = self.get_user_sessions(unique_id)
            if not sessions:
                return None
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            if format == 'json':
                export_file = os.path.join(self.base_path, f"{unique_id}_export_{timestamp}.json")
                export_data = {
                    'unique_id': unique_id,
                    'export_time': datetime.now().isoformat(),
                    'total_sessions': len(sessions),
                    'sessions': []
                }
                
                for session in sessions:
                    detail = self.get_session_detail(unique_id, session['session_id'])
                    if detail:
                        export_data['sessions'].append(detail)
                
                with open(export_file, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, ensure_ascii=False, indent=2)
            
            elif format == 'csv':
                import csv
                export_file = os.path.join(self.base_path, f"{unique_id}_export_{timestamp}.csv")
                
                with open(export_file, 'w', newline='', encoding='utf-8-sig') as csvfile:
                    fieldnames = [
                        'session_id', 'start_time', 'duration_minutes', 'peak_viewers',
                        'total_likes', 'total_comments', 'total_gifts', 'total_follows'
                    ]
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    for session in sessions:
                        detail = self.get_session_detail(unique_id, session['session_id'])
                        if detail:
                            writer.writerow({
                                'session_id': detail.get('session_id'),
                                'start_time': detail.get('start_time'),
                                'duration_minutes': round(detail.get('duration_seconds', 0) / 60, 1),
                                'peak_viewers': detail.get('summary', {}).get('peak_viewers', 0),
                                'total_likes': detail.get('summary', {}).get('total_likes', 0),
                                'total_comments': detail.get('summary', {}).get('total_comments', 0),
                                'total_gifts': detail.get('summary', {}).get('total_gifts', 0),
                                'total_follows': detail.get('summary', {}).get('total_follows', 0)
                            })
            
            logger.info(f"用户数据导出完成: {export_file}")
            return export_file
            
        except Exception as e:
            logger.error(f"导出用户数据失败: {str(e)}")
            return None

# 创建历史数据管理器实例
history_manager = HistoryDataManager()

class TikTokLiveMonitor:
    """TikTok 直播监控类"""
    
    def __init__(self, unique_id: str, sessionid: Optional[str] = None, anti_detection: bool = False, use_session_pool: bool = False):
        """
        初始化监控器
        
        Args:
            unique_id: TikTok 用户名 (不含@符号)
            sessionid: TikTok 会话ID (用于绕过年龄限制)
            anti_detection: 是否启用防检测模式
            use_session_pool: 是否使用账号池轮换
        """
        self.unique_id = unique_id
        self.sessionid = sessionid
        self.anti_detection = anti_detection
        self.use_session_pool = use_session_pool
        self.current_pool_session = None  # 当前使用的账号池session
        
        self.client: Optional[TikTokLiveClient] = TikTokLiveClient(
            unique_id=unique_id
        )
        
        # 监控控制状态
        self.is_paused = False        # 暂停状态（不同于停止）
        self.custom_interval = None   # 自定义监控间隔
        self.last_update_time = None  # 最后更新时间
        self.last_session_switch = datetime.now()  # 上次切换session的时间
        
        # 设置sessionid：优先使用账号池，其次使用指定的sessionid
        self._apply_session()
        
        # 如果启用防检测模式，配置伪装参数
        if anti_detection:
            self._setup_anti_detection()
            logger.info(f"为 @{unique_id} 启用了防检测模式")
    
    def _apply_session(self):
        """应用sessionid到客户端"""
        session_to_use = None
        
        # 优先使用账号池
        if self.use_session_pool and session_pool.sessions:
            session_to_use = session_pool.get_current_session()
            if session_to_use:
                self.current_pool_session = session_to_use
                pool_info = session_pool.get_current_session_info()
                logger.info(f"@{self.unique_id} 使用账号池: {pool_info['name']} ({pool_info['index']}/{pool_info['total']})")
        
        # 如果账号池没有可用session，使用指定的sessionid
        if not session_to_use and self.sessionid:
            session_to_use = self.sessionid
            logger.info(f"@{self.unique_id} 使用指定的sessionid")
        
        # 应用到客户端
        if session_to_use:
            self.client.web.set_session_id(session_to_use)
    
    def _check_and_rotate_session(self):
        """检查是否需要轮换session"""
        if not self.use_session_pool or not session_pool.sessions:
            return False
        
        # 获取当前应使用的session
        current_session = session_pool.get_current_session()
        
        # 如果session已变化，需要重新创建客户端
        if current_session and current_session != self.current_pool_session:
            logger.info(f"@{self.unique_id} 检测到账号池轮换，切换session...")
            self.current_pool_session = current_session
            
            # 重新创建客户端
            self.client = TikTokLiveClient(unique_id=self.unique_id)
            self.client.web.set_session_id(current_session)
            self._setup_event_listeners()
            
            if self.anti_detection:
                self._setup_anti_detection()
            
            pool_info = session_pool.get_current_session_info()
            logger.info(f"@{self.unique_id} 已切换到: {pool_info['name']} ({pool_info['index']}/{pool_info['total']})")
            self.last_session_switch = datetime.now()
            return True
        
        return False
        
        self.client.logger.setLevel(LogLevel.INFO.value)
        self.is_running = False
        self.is_live = False
        
        # 统计数据 - 优化结构，减少内存占用
        self.stats = {
            'start_time': None,
            'room_id': None,
            'current_viewers': 0,
            'total_viewers': 0,
            'like_count': 0,
            'comment_count': 0,
            'gift_count': 0,
            'gift_value': 0,
            'follow_count': 0,
            'share_count': 0,
            'subscribe_count': 0,
            'gifts_detail': {},  # 简化的礼物记录
            'room_info': {},     # 精简的房间信息
            'comments': [],      # 只保留最近10条评论
            'gifts': [],         # 只保留最近10条礼物记录
            'chart_data': {
                'timestamps': [],
                'viewer_counts': [],
                'like_counts': [],
                'comment_counts': []
            },
            'title': "",         # 直播标题
            'stream_status': 0,  # 直播状态
            'create_time': 0,    # 创建时间戳
            'minute_data': []    # 每分钟记录的数据点
        }
        
        # 每分钟数据记录
        self.last_minute_record = None
        
        # 用于存储用户信息
        self.viewers: Set[str] = set()
        
        # 设置事件监听器
        self._setup_event_listeners()
        
    def _setup_anti_detection(self):
        """设置防检测参数"""
        import random
        
        # 使用更保守的User-Agent列表，模拟真实浏览器
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        
        # 保守的语言设置
        accept_languages = [
            'en-US,en;q=0.9',
            'zh-CN,zh;q=0.9,en;q=0.8'
        ]
        
        # 更温和的请求头设置 - 只设置关键的几个
        selected_ua = random.choice(user_agents)
        selected_lang = random.choice(accept_languages)
        
        # 仅设置最关键的请求头，避免过度伪装
        minimal_headers = {
            'User-Agent': selected_ua,
            'Accept-Language': selected_lang
        }
        
        # 应用请求头到客户端
        try:
            if hasattr(self.client.web, 'session') and hasattr(self.client.web.session, 'headers'):
                # 只更新关键请求头，保留其他默认设置
                for key, value in minimal_headers.items():
                    self.client.web.session.headers[key] = value
                logger.info(f"已设置防检测请求头: UA={selected_ua[:50]}..., Lang={selected_lang}")
        except Exception as e:
            logger.warning(f"设置请求头失败: {str(e)}")
        
        # 更保守的防检测配置 - 大幅增加间隔避免被限流
        self.anti_detection_config = {
            'random_delay_min': 3,        # 最小延迟（秒）
            'random_delay_max': 10,       # 最大延迟（秒）
            'request_interval_min': 180,  # 最小检查间隔（3分钟）
            'request_interval_max': 300   # 最大检查间隔（5分钟）
        }
            
    def _setup_event_listeners(self):
        """设置所有事件监听器"""
        
        @self.client.on(ConnectEvent)
        async def on_connect(event: ConnectEvent):
            """连接事件"""
            self.is_live = True
            self.stats['start_time'] = datetime.now()
            self.stats['room_id'] = self.client.room_id
            logger.info(f"已连接到直播间 @{event.unique_id} (房间ID: {self.client.room_id})")
            
            # 获取房间信息
            if hasattr(self.client, 'room_info') and self.client.room_info:
                self.stats['room_info'] = self.client.room_info
                
                # 直播状态
                self.stats['stream_status'] = self.client.room_info.get('status', 0)
                
                # 基本信息
                self.stats['title'] = self.client.room_info.get('title', '')
                self.stats['cover'] = self.client.room_info.get('cover', {})
                self.stats['create_time'] = self.client.room_info.get('create_time', 0)
                
                # 流地址
                self.stats['stream_url'] = self.client.room_info.get('stream_url', {})
                
                socketio.emit('room_info', {
                    'unique_id': self.unique_id,
                    'room_info': self.stats['room_info']
                })
            
        @self.client.on(DisconnectEvent)
        async def on_disconnect(event: DisconnectEvent):
            """断开连接事件"""
            duration = (datetime.now() - self.stats['start_time']).total_seconds() if self.stats['start_time'] else 0
            logger.info(f"已断开连接，持续时间: {duration:.0f} 秒")
            socketio.emit('disconnect', {'unique_id': self.unique_id})
            await self.save_stats()
            
        @self.client.on(LiveEndEvent)
        async def on_live_end(event: LiveEndEvent):
            """直播结束事件"""
            self.is_live = False
            logger.info("直播已结束")
            socketio.emit('live_end', {'unique_id': self.unique_id})
            await self.save_stats()
            
        @self.client.on(RoomUserSeqEvent)
        async def on_room_user_seq(event: RoomUserSeqEvent):
            """房间用户数更新事件"""
            try:
                # 尝试所有可能的属性名称获取观众数量
                viewer_count = 0
                total_viewers = 0
                tried_attrs = []
                
                # 尝试获取当前观众数（实时在线人数）
                for attr_name in ["user_count", "member_count", "m_total", "viewers", "audience", "audience_count"]:
                    tried_attrs.append(attr_name)
                    if hasattr(event, attr_name):
                        try:
                            viewer_count = getattr(event, attr_name)
                            logger.debug(f"获取到当前观众数: {viewer_count}，使用属性: {attr_name}")
                            break
                        except Exception as e:
                            logger.error(f"获取属性 {attr_name} 时出错: {str(e)}")
                
                # 尝试获取累计观众数
                if hasattr(event, "total_user"):
                    try:
                        total_viewers = getattr(event, "total_user")
                        logger.debug(f"获取到累计观众数: {total_viewers}")
                        # 更新累计观众数
                        self.stats['total_viewers'] = max(self.stats['total_viewers'], total_viewers)
                    except Exception as e:
                        logger.error(f"获取total_user属性时出错: {str(e)}")
                elif hasattr(event, "total"):
                    try:
                        total_viewers = getattr(event, "total")
                        logger.debug(f"获取到总观众数: {total_viewers}")
                        # 可能是累计观众数，需要判断
                        if total_viewers > viewer_count:
                            self.stats['total_viewers'] = max(self.stats['total_viewers'], total_viewers)
                    except Exception as e:
                        logger.error(f"获取total属性时出错: {str(e)}")
                
                # 如果获取不到当前观众数，尝试解析原始字符串
                if viewer_count == 0:
                    logger.warning(f"尝试所有属性都失败: {tried_attrs}，尝试解析原始字符串")
                    try:
                        event_str = str(event)
                        import re
                        numbers = re.findall(r'\d+', event_str)
                        if numbers:
                            viewer_count = int(numbers[0])  # 使用第一个数字作为当前观众数
                            if len(numbers) > 1:
                                # 如果有多个数字，第二个可能是累计观众数
                                potential_total = int(numbers[1])
                                if potential_total > viewer_count:
                                    self.stats['total_viewers'] = max(self.stats['total_viewers'], potential_total)
                            logger.debug(f"从字符串中提取的观众数量: {viewer_count}")
                    except Exception as e:
                        logger.error(f"解析字符串时出错: {str(e)}")
                
                # 确保我们至少有一个观众
                if viewer_count == 0:
                    viewer_count = 1
                    logger.warning("无法获取有效的观众数量，设置为默认值1")
                
                # 更新当前观众数
                self.stats['current_viewers'] = viewer_count
                
                # 如果没有获取到累计观众数，则用当前观众数的最大值作为累计观众数
                if self.stats['total_viewers'] == 0:
                    self.stats['total_viewers'] = viewer_count
                else:
                    # 确保累计观众数不小于当前观众数
                    self.stats['total_viewers'] = max(self.stats['total_viewers'], viewer_count)
                
                # 每分钟记录一次重要数据
                self._record_minute_data()
                
                # 发送数据更新
                self._send_data_updates()
                
            except Exception as e:
                logger.error(f"处理观众数量更新失败: {str(e)}")
        
        @self.client.on(JoinEvent)
        async def on_join(event: JoinEvent):
            """用户加入事件"""
            try:
                # 获取用户ID - 在新版中可能是 unique_id
                user_id = self._get_user_id(event.user)
                if user_id and user_id not in self.viewers:
                    self.viewers.add(user_id)
                    socketio.emit('user_join', {
                        'unique_id': self.unique_id,
                        'user': {
                            'nickname': event.user.nickname,
                            'user_id': user_id
                        }
                    })
                    
            except Exception as e:
                logger.error(f"处理JoinEvent时出错: {str(e)}")
                
        @self.client.on(CommentEvent)
        async def on_comment(event: CommentEvent):
            """评论事件"""
            try:
                # 调试：打印事件的所有属性（首次收到时）
                if self.stats['comment_count'] == 0:
                    event_attrs = {attr: str(getattr(event, attr, None))[:100] 
                                   for attr in dir(event) if not attr.startswith('_')}
                    logger.info(f"CommentEvent属性: {event_attrs}")
                
                # 获取用户 - 尝试多种属性名
                user = None
                for attr in ['user', 'user_info', 'from_user', 'sender']:
                    if hasattr(event, attr) and getattr(event, attr) is not None:
                        user = getattr(event, attr)
                        break
                
                if user is None:
                    logger.warning(f"无法获取评论用户信息，可用属性: {[a for a in dir(event) if not a.startswith('_')]}")
                    return
                    
                # 获取用户ID
                user_id = self._get_user_id(user)
                if not user_id:
                    return
                
                # 获取评论内容 - 尝试多种属性名
                comment_text = None
                for attr in ['comment', 'content', 'text', 'message', 'msg']:
                    if hasattr(event, attr) and getattr(event, attr) is not None:
                        comment_text = getattr(event, attr)
                        break
                
                if comment_text is None:
                    logger.warning(f"无法获取评论内容，可用属性: {[a for a in dir(event) if not a.startswith('_')]}")
                    return
                
                # 更新统计
                self.stats['comment_count'] += 1
                
                # 记录评论历史 - 只保留最近10条
                timestamp = datetime.now().isoformat()
                comment_data = {
                    'user': user.nickname,
                    'comment': comment_text,
                    'timestamp': timestamp
                }
                self.stats['comments'].append(comment_data)
                
                # 只保留最新的10条评论
                if len(self.stats['comments']) > 10:
                    self.stats['comments'] = self.stats['comments'][-10:]
                
                # 发送到前端
                socketio.emit('comment', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': user.nickname,
                        'user_id': user_id
                    },
                    'comment': comment_text,
                    'timestamp': timestamp
                })
                
            except Exception as e:
                logger.error(f"处理CommentEvent时出错: {str(e)}")
            
        @self.client.on(LikeEvent)
        async def on_like(event: LikeEvent):
            """点赞事件"""
            try:
                # 调试：首次收到时打印事件的所有属性
                if self.stats['like_count'] == 0:
                    event_attrs = {attr: str(getattr(event, attr, None))[:100] 
                                   for attr in dir(event) if not attr.startswith('_')}
                    logger.info(f"LikeEvent属性: {event_attrs}")
                
                # 获取用户 - 尝试多种属性名
                user = None
                for attr in ['user', 'user_info', 'from_user', 'sender']:
                    if hasattr(event, attr) and getattr(event, attr) is not None:
                        user = getattr(event, attr)
                        break
                
                user_id = self._get_user_id(user) if user else "unknown"
                
                # 获取点赞数量 - 尝试多种属性名
                like_count = 1  # 默认值
                for attr in ['like_count', 'count', 'likes', 'total_likes', 'likeCount']:
                    if hasattr(event, attr):
                        val = getattr(event, attr)
                        if val is not None and isinstance(val, (int, float)):
                            like_count = int(val)
                            break
                
                # 记录点赞信息（改为DEBUG级别，减少日志量）
                logger.debug(f"收到点赞: 用户={user_id}, 数量={like_count}")
                
                # 更新统计
                self.stats['like_count'] += like_count
                
                # 获取用户昵称
                nickname = "用户"
                if user and hasattr(user, 'nickname'):
                    nickname = user.nickname
                
                # 发送到前端
                socketio.emit('like', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': nickname,
                        'user_id': user_id
                    },
                    'count': like_count,
                    'total_likes': self.stats['like_count'],  # 发送总点赞数
                    'timestamp': datetime.now().isoformat()
                })
                
            except Exception as e:
                logger.error(f"处理LikeEvent时出错: {str(e)}")
                logger.exception("详细错误信息:")
            
        @self.client.on(GiftEvent)
        async def on_gift(event: GiftEvent):
            """礼物事件"""
            try:
                # 根据最新版本的API，使用正确的属性
                user = event.user if hasattr(event, 'user') else (event.from_user if hasattr(event, 'from_user') else None)
                
                if user is None:
                    logger.warning(f"无法获取礼物发送者信息，可用属性: {dir(event)}")
                    return
                
                # 获取用户ID
                user_id = self._get_user_id(user)
                if not user_id:
                    return
                
                # 根据示例获取正确的礼物属性
                gift = event.gift if hasattr(event, 'gift') else (event.m_gift if hasattr(event, 'm_gift') else None)
                
                if gift is None:
                    logger.warning(f"无法获取礼物信息，可用属性: {dir(event)}")
                    return
                
                # 获取礼物信息
                gift_name = gift.name
                gift_count = event.repeat_count if hasattr(event, 'repeat_count') else 1
                
                # 检查是否处于连击状态
                streaking = False
                if hasattr(event, 'streaking'):
                    streaking = event.streaking
                elif hasattr(gift, 'streakable') and hasattr(event, 'repeat_end'):
                    streaking = gift.streakable and event.repeat_end != 1
                
                # 仅在连击结束或非连击礼物时才记录
                if not streaking or not gift.streakable:
                    # 更新礼物统计
                    self.stats['gift_count'] += gift_count
                    
                    # 记录礼物详情
                    if gift_name not in self.stats['gifts_detail']:
                        self.stats['gifts_detail'][gift_name] = {
                            'count': 0,
                            'users': []
                        }
                    
                    self.stats['gifts_detail'][gift_name]['count'] += gift_count
                    timestamp = datetime.now().isoformat()
                    
                    # 记录发送礼物的用户信息
                    self.stats['gifts_detail'][gift_name]['users'].append({
                        'nickname': user.nickname,
                        'count': gift_count,
                        'time': timestamp
                    })
                    
                    # 记录礼物历史 - 只保留最近10条
                    gift_data = {
                        'user': user.nickname,
                        'gift_name': gift_name,
                        'gift_count': gift_count,
                        'timestamp': timestamp
                    }
                    self.stats['gifts'].append(gift_data)
                    
                    # 只保留最新的10个礼物记录
                    if len(self.stats['gifts']) > 10:
                        self.stats['gifts'] = self.stats['gifts'][-10:]
                    
                    # 如果礼物有价值信息，累加价值
                    if hasattr(gift, 'diamond_count'):
                        self.stats['gift_value'] += gift.diamond_count * gift_count
                    elif hasattr(gift, 'value'):
                        self.stats['gift_value'] += gift.value * gift_count
                
                # 发送礼物事件到前端
                socketio.emit('gift', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': user.nickname,
                        'user_id': user_id
                    },
                    'gift': {
                        'name': gift_name,
                        'count': gift_count,
                        'streaking': streaking
                    },
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"处理GiftEvent时出错: {str(e)}")
                
        @self.client.on(FollowEvent)
        async def on_follow(event: FollowEvent):
            """关注事件"""
            try:
                # 获取用户ID
                user_id = self._get_user_id(event.user)
                if not user_id:
                    return
                
                self.stats['follow_count'] += 1
                socketio.emit('follow', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': event.user.nickname,
                        'user_id': user_id
                    },
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"处理FollowEvent时出错: {str(e)}")
            
        @self.client.on(ShareEvent)
        async def on_share(event: ShareEvent):
            """分享事件"""
            try:
                # 获取用户ID
                user_id = self._get_user_id(event.user)
                if not user_id:
                    return
                
                self.stats['share_count'] += 1
                socketio.emit('share', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': event.user.nickname,
                        'user_id': user_id
                    },
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"处理ShareEvent时出错: {str(e)}")
            
        @self.client.on(SubscribeEvent)
        async def on_subscribe(event: SubscribeEvent):
            """订阅事件"""
            try:
                # 获取用户ID
                user_id = self._get_user_id(event.user)
                if not user_id:
                    return
                
                self.stats['subscribe_count'] += 1
                socketio.emit('subscribe', {
                    'unique_id': self.unique_id,
                    'user': {
                        'nickname': event.user.nickname,
                        'user_id': user_id
                    },
                    'timestamp': datetime.now().isoformat()
                })
            except Exception as e:
                logger.error(f"处理SubscribeEvent时出错: {str(e)}")
                
    def _get_user_id(self, user):
        """获取用户ID，处理不同版本API的差异"""
        if user is None:
            return "unknown"
        
        try:    
            # 直接调试输出所有可用属性
            logger.debug(f"User对象属性: {dir(user)}")
            
            # 尝试优先获取用户显示名称，如果无法获取ID
            nickname = None
            if hasattr(user, 'nickname'):
                nickname = user.nickname
            
            # 使用正确的属性获取ID
            if hasattr(user, 'unique_id'):
                return user.unique_id
            elif hasattr(user, 'id'):
                return user.id
            elif hasattr(user, 'uid'):
                return user.uid
            elif hasattr(user, 'user_id'):
                return user.user_id
            
            # 如果无法获取ID但有昵称，使用昵称
            if nickname:
                return f"user_{nickname}"
            
            # 记录可用属性帮助调试
            attrs = {}
            for attr in dir(user):
                if not attr.startswith('_') and not callable(getattr(user, attr, None)):
                    try:
                        attrs[attr] = getattr(user, attr)
                    except Exception:
                        attrs[attr] = "无法获取"
            
            logger.warning(f"无法获取用户ID，可用属性: {attrs}")
            
            # 生成一个随机ID
            import uuid
            return f"user_{uuid.uuid4().hex[:8]}"
            
        except Exception as e:
            logger.error(f"获取用户ID时出错: {str(e)}")
            return "unknown"
    
    async def fetch_room_info(self):
        """
        手动拉取房间信息
        """
        try:
            # 防检测：仅在必要时添加轻微延迟
            if self.anti_detection:
                import random
                await asyncio.sleep(random.uniform(0.5, 2))
            
            # 尝试先获取房间ID
            try:
                room_id = await self.client.web.fetch_room_id_from_html(self.client.unique_id)
            except FailedParseRoomIdError:
                logger.info(f"HTML 解析房间ID失败，使用 API 获取")
                # 防检测：在API调用前轻微延迟
                if self.anti_detection:
                    import random
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                room_id = await self.client.web.fetch_room_id_from_api(self.client.unique_id)
                
            self.client.web.params['room_id'] = str(room_id)
            self.stats['room_id'] = room_id
            
            # 获取直播间信息
            room_info = await self.client.web.fetch_room_info()
            # 获取礼物列表信息
            gift_info = await self.client.web.fetch_gift_list()
            
            # 只保存精简的房间信息，不再保存完整的大文件
            simplified_room_info = {
                'id': room_info.get('id', 0),
                'title': room_info.get('title', ''),
                'status': room_info.get('status', 0),
                'user_count': room_info.get('user_count', 0),
                'create_time': room_info.get('create_time', 0),
                'stats': room_info.get('stats', {}),
                'owner': room_info.get('owner', {}).get('nickname', '') if room_info.get('owner') else ''
            }
            
            # 保存房间信息到stats（新版本只在内存中保存，直播结束时一起保存）
            logger.debug(f"房间信息已获取: {self.unique_id}")
            
            # 更新统计数据
            if room_info:
                self.stats['room_info'] = simplified_room_info
                
                # 直播状态
                self.stats['stream_status'] = room_info.get('status', 0)
                self.is_live = self.stats['stream_status'] == 2
                
                # 基本信息
                self.stats['title'] = room_info.get('title', '')
                self.stats['create_time'] = room_info.get('create_time', 0)
                
                # 统计数据
                if 'stats' in room_info:
                    stats = room_info['stats']
                    # 累计观众 - 使用max确保只增加不减少
                    room_total_viewers = stats.get('total_user', 0)
                    if room_total_viewers > 0:
                        self.stats['total_viewers'] = max(self.stats['total_viewers'], room_total_viewers)
                    
                    # 其他统计数据也使用max确保只增加
                    self.stats['like_count'] = max(self.stats['like_count'], stats.get('like_count', 0))
                    self.stats['comment_count'] = max(self.stats['comment_count'], stats.get('comment_count', 0))
                    
                # 当前在线人数 - 从room_info['user_count']获取
                self.stats['current_viewers'] = room_info.get('user_count', 0)
                
                # 记录分钟数据
                self._record_minute_data()
            
            # 发送数据更新
            self._send_data_updates()
            return True
        except Exception as e:
            # 检查是否是用户离线错误或用户不存在错误
            error_str = str(e)
            if ("is offline" in error_str or "UserOfflineError" in error_str or 
                "UserNotFoundError" in error_str or "No Message Provided" in error_str):
                logger.info(f"@{self.unique_id} 当前离线或不存在，10分钟后重试")
                return False
            # 检查是否是限流错误（429 Too Many Requests）
            elif "429" in error_str or "Too Many Requests" in error_str or "rate limit" in error_str.lower():
                logger.warning(f"@{self.unique_id} 遭遇限流(429)，将等待 {CONCURRENT_CONFIG['rate_limit_backoff']} 秒后重试")
                rate_limit_tracker['is_rate_limited'] = True
                rate_limit_tracker['rate_limit_until'] = datetime.now() + timedelta(seconds=CONCURRENT_CONFIG['rate_limit_backoff'])
                return False
            # 检查是否是签名API错误
            elif "SIGN_NOT_200" in error_str or "500" in error_str or "503" in error_str:
                logger.error(f"@{self.unique_id} API服务错误，等待较长时间后重试: {error_str[:100]}")
                return False
            else:
                logger.error(f"获取房间信息失败: {str(e)}")
            return False
            
    async def check_and_connect(self):
        """
        检查并连接到直播间
        """
        try:
            # 如果已经连接，只更新房间信息而不重新连接
            if self.client.connected:
                logger.debug(f"@{self.unique_id} 已经连接，更新房间信息")
                await self.fetch_room_info()
                return True

            # 先尝试获取房间信息，判断用户是否在直播
            # 无论获取结果如何，都先初始化一个界面
            result = await self.fetch_room_info()
            
            # 发送初始数据以显示界面
            self._send_default_data()
            
            # 检查是否获取成功并且在直播中
            if result and self.stats['stream_status'] == 2:  # 直播中
                logger.info(f"检查 @{self.unique_id} 是否在直播...")
                
                logger.info(f"开始监控 @{self.unique_id} 的直播...")
                
                # 使用已获取的房间ID直接连接
                room_id = self.stats['room_id']
                
                try:
                    # 防检测模式下，连接前先延迟
                    if self.anti_detection:
                        await self._random_delay()
                    
                    # 确保客户端处于断开状态后再连接
                    if self.client.connected:
                        logger.debug(f"断开现有连接后重新连接 @{self.unique_id}")
                        await self.client.disconnect()
                        await asyncio.sleep(1)  # 等待断开完成
                    
                    # 开始连接
                    await self.client.start(
                        fetch_room_info=True,  # 额外获取房间信息
                        fetch_gift_info=True,   # 额外获取礼物信息
                        process_connect_events=True,
                        room_id=room_id  # 直接使用房间ID，避免再次获取
                    )
                    return True
                except Exception as e:
                    # 如果防检测模式下连接失败，尝试禁用防检测重试一次
                    if self.anti_detection and ("UserNotFoundError" in str(e) or "404" in str(e)):
                        logger.warning(f"防检测模式连接失败，尝试使用标准模式重连: {str(e)}")
                        try:
                            # 临时禁用防检测
                            self.anti_detection = False
                            # 重新设置客户端（恢复默认请求头）
                            self.client = TikTokLiveClient(unique_id=self.unique_id)
                            if self.sessionid:
                                self.client.web.set_session_id(self.sessionid)
                            self._setup_event_listeners()
                            
                            # 重新获取房间信息
                            result = await self.fetch_room_info()
                            if result and self.stats['stream_status'] == 2:
                                await self.client.start(
                                    fetch_room_info=True,
                                    fetch_gift_info=True,
                                    process_connect_events=True,
                                    room_id=self.stats['room_id']
                                )
                                logger.info(f"标准模式重连成功: @{self.unique_id}")
                                return True
                        except Exception as retry_e:
                            logger.error(f"标准模式重连也失败: {str(retry_e)}")
                    
                    # 如果是签名API错误，记录日志但不重试太频繁
                    if "SIGN_NOT_200" in str(e) or "500" in str(e):
                        logger.error(f"签名API错误，等待较长时间后重试: {str(e)}")
                        return False
                    else:
                        # 其他错误记录并继续
                        logger.error(f"连接直播间失败: {str(e)}")
                        return False
            else:
                if result:
                    logger.info(f"@{self.unique_id} 当前不在直播中 (状态: {self.stats['stream_status']})")
                else:
                    logger.info(f"@{self.unique_id} 无法获取房间信息")
                
                # 不在直播或无法获取信息时也发送更新，让前端显示"离线"状态
                self._send_data_updates()
                
            return False
        except Exception as e:
            logger.error(f"连接直播间失败: {str(e)}")
            # 发生错误时也发送更新
            self._send_default_data()
            return False
        
    def _send_default_data(self):
        """发送默认/初始数据"""
        socketio.emit('stream_data', {
            'unique_id': self.unique_id,
            'is_live': self.is_live,
            'room_info': self.stats['room_info'],
            'title': self.stats['title'],
            'current_viewers': self.stats['current_viewers'],
            'total_viewers': self.stats['total_viewers'],
            'total_likes': self.stats['like_count'],
            'total_gifts': self.stats['gift_count'],
            'total_comments': self.stats['comment_count'],
            'total_shares': self.stats['share_count'],
            'total_follows': self.stats['follow_count'],
            'start_time': self.stats['start_time'].isoformat() if self.stats.get('start_time') else None,
            'chart_data': self.stats['chart_data'],
            'comments': self.stats['comments'],  # 现在只有10条
            'gifts': self.stats['gifts'],        # 现在只有10条
            'last_update_time': self.last_update_time,  # 添加最后更新时间
            'status': 'live' if self.is_live else 'offline'
        })
    
    # 删除了_update_daily_with_room_summary方法，新版本不再需要
    
    def _record_minute_data(self):
        """每分钟记录一次重要数据"""
        current_time = datetime.now()
        current_minute = current_time.strftime('%H:%M')
        
        # 检查是否需要记录新的分钟数据
        if self.last_minute_record is None or self.last_minute_record != current_minute:
            self.last_minute_record = current_minute
            
            # 记录当前分钟的数据点
            minute_record = {
                'timestamp': current_time.isoformat(),
                'minute': current_minute,
                'current_viewers': self.stats['current_viewers'],
                'total_viewers': self.stats['total_viewers'],
                'like_count': self.stats['like_count'],
                'comment_count': self.stats['comment_count'],
                'gift_count': self.stats['gift_count'],
                'gift_value': self.stats['gift_value'],
                'follow_count': self.stats['follow_count'],
                'share_count': self.stats['share_count']
            }
            
            # 添加到分钟数据列表
            self.stats['minute_data'].append(minute_record)
            
            # 同时更新图表数据（保持向后兼容）
            self.stats['chart_data']['timestamps'].append(current_minute)
            self.stats['chart_data']['viewer_counts'].append(self.stats['current_viewers'])
            self.stats['chart_data']['like_counts'].append(self.stats['like_count'])
            self.stats['chart_data']['comment_counts'].append(self.stats['comment_count'])
            
            # 保持数据长度合理（最多保留100个数据点，约1.5小时）
            max_points = 100
            if len(self.stats['minute_data']) > max_points:
                self.stats['minute_data'] = self.stats['minute_data'][-max_points:]
            
            if len(self.stats['chart_data']['timestamps']) > max_points:
                self.stats['chart_data']['timestamps'] = self.stats['chart_data']['timestamps'][-max_points:]
                self.stats['chart_data']['viewer_counts'] = self.stats['chart_data']['viewer_counts'][-max_points:]
                self.stats['chart_data']['like_counts'] = self.stats['chart_data']['like_counts'][-max_points:]
                self.stats['chart_data']['comment_counts'] = self.stats['chart_data']['comment_counts'][-max_points:]
            
            # 只在DEBUG级别记录分钟数据，减少日志量
            logger.debug(f"记录分钟数据: {current_minute}, 观众: {self.stats['current_viewers']}, 点赞: {self.stats['like_count']}")
    
    def _send_data_updates(self):
        """发送数据更新"""
        # 设置最后更新时间
        self.last_update_time = datetime.now().isoformat()
        
        socketio.emit('stream_data', {
            'unique_id': self.unique_id,
            'is_live': self.is_live,
            'room_info': self.stats['room_info'],
            'title': self.stats['title'],
            'current_viewers': self.stats['current_viewers'],
            'total_viewers': self.stats['total_viewers'],
            'total_likes': self.stats['like_count'],
            'total_gifts': self.stats['gift_count'],
            'total_comments': self.stats['comment_count'],
            'total_shares': self.stats['share_count'],
            'total_follows': self.stats['follow_count'],
            'start_time': self.stats['start_time'].isoformat() if self.stats.get('start_time') else None,
            'chart_data': self.stats['chart_data'],
            'comments': self.stats['comments'],  # 现在只有10条
            'gifts': self.stats['gifts'],        # 现在只有10条
            'last_update_time': self.last_update_time,  # 添加最后更新时间
            'status': 'live' if self.is_live else 'offline'
        })
    
    async def _random_delay(self):
        """添加随机延迟（仅在启用防检测时）"""
        if self.anti_detection:
            import random
            delay = random.uniform(
                self.anti_detection_config['random_delay_min'],
                self.anti_detection_config['random_delay_max']
            )
            logger.debug(f"防检测随机延迟: {delay:.2f}秒")
            await asyncio.sleep(delay)
    
    async def start_monitoring(self):
        """启动监控"""
        self.is_running = True
        self.is_paused = False  # 重置暂停状态
        # 用于跟踪连续失败次数
        consecutive_failures = 0
        
        # 错开启动时间避免同时请求API - 增加延迟
        active_count = len(active_monitors)
        if active_count > 0:
            stagger_delay = min(active_count * CONCURRENT_CONFIG['stagger_delay'], 120)  # 最多2分钟
            logger.info(f"@{self.unique_id} 错开启动延迟: {stagger_delay}秒")
            await asyncio.sleep(stagger_delay)
        
        # 如果启用防检测，先进行随机延迟
        if self.anti_detection:
            await self._random_delay()
        
        # 尝试连接，如果失败则重试
        while self.is_running:
            # 检查是否处于全局限流状态
            if rate_limit_tracker['is_rate_limited']:
                if rate_limit_tracker['rate_limit_until'] and datetime.now() < rate_limit_tracker['rate_limit_until']:
                    wait_seconds = (rate_limit_tracker['rate_limit_until'] - datetime.now()).total_seconds()
                    logger.warning(f"@{self.unique_id} 全局限流中，等待 {wait_seconds:.0f} 秒...")
                    await asyncio.sleep(min(wait_seconds, 60))  # 每分钟检查一次
                    continue
                else:
                    # 限流时间已过，重置状态
                    rate_limit_tracker['is_rate_limited'] = False
                    rate_limit_tracker['rate_limit_until'] = None
                    logger.info("限流已解除，继续监控")
            # 检查是否被暂停
            if self.is_paused:
                logger.debug(f"@{self.unique_id} 监控已暂停，等待恢复...")
                await asyncio.sleep(5)  # 暂停时每5秒检查一次
                continue
                
            try:
                connection_result = await self.check_and_connect()
                
                if connection_result:
                    # 连接成功，重置失败计数
                    consecutive_failures = 0
                    logger.info(f"@{self.unique_id} 连接成功，开始监控")
                    
                    # 连接成功，定期检查直播状态
                    while self.is_running and self.client.connected:
                        # 根据活跃监控数量动态调整间隔
                        active_count = len(active_monitors)
                        
                        if self.anti_detection:
                            import random
                            interval = random.randint(
                                self.anti_detection_config['request_interval_min'],
                                self.anti_detection_config['request_interval_max']
                            )
                            # 多账号时增加间隔避免API限制
                            if active_count > 3:
                                interval = int(interval * (1 + active_count * 0.1))
                            logger.debug(f"防检测模式，下次检查间隔: {interval}秒 (活跃监控: {active_count})")
                        else:
                            # 使用自定义间隔或根据监控数量调整
                            if self.custom_interval:
                                interval = self.custom_interval
                                logger.debug(f"使用自定义间隔: {interval}秒")
                            else:
                                # 基础间隔根据监控数量调整 - 大幅增加避免限流
                                base_interval = CONCURRENT_CONFIG['update_interval_base']  # 默认120秒
                                if active_count <= 2:
                                    interval = base_interval
                                elif active_count <= 4:
                                    interval = base_interval + 60   # 180秒（3分钟）
                                else:
                                    interval = base_interval + 120  # 240秒（4分钟）
                                
                                logger.debug(f"更新间隔: {interval}秒 (活跃监控: {active_count})")
                        
                        await asyncio.sleep(interval)
                        
                        # 防检测模式下添加轻微随机延迟
                        if self.anti_detection:
                            import random
                            await asyncio.sleep(random.uniform(0.5, 2))
                        
                        # 只在还连接且未暂停时才更新房间信息
                        if self.is_running and self.client.connected and not self.is_paused:
                            # 检查是否需要轮换session（使用账号池时）
                            if self._check_and_rotate_session():
                                # session已切换，需要重新连接
                                logger.info(f"@{self.unique_id} session已切换，断开当前连接...")
                                await self.client.disconnect()
                                await asyncio.sleep(2)
                                break  # 跳出内层循环，重新连接
                            
                            await self.fetch_room_info()  # 定期更新房间信息
                            self.last_update_time = datetime.now().isoformat()
                else:
                    # 连接失败，增加失败计数并使用指数退避策略
                    consecutive_failures += 1
                    
                    # 检查是否是离线状态（stream_status != 2）
                    if hasattr(self, 'stats') and self.stats.get('stream_status') != 2:
                        # 用户离线，使用10分钟间隔
                        wait_time = 600  # 10分钟
                        logger.info(f"@{self.unique_id} 离线中，{wait_time//60}分钟后重新检查")
                    else:
                        # 其他连接失败，使用指数退避策略 - 增加基础等待时间
                        base_wait = 120  # 2分钟基础等待
                        if self.anti_detection:
                            # 防检测模式下使用更长的等待时间
                            base_wait = 180  # 3分钟
                        
                        # 指数退避，最长15分钟
                        wait_time = min(base_wait * (2 ** (consecutive_failures - 1)), 900)
                        logger.info(f"@{self.unique_id} 连接失败，{wait_time}秒后重试 (连续失败: {consecutive_failures}次)")
                    
                    await asyncio.sleep(wait_time)
                    
            except Exception as e:
                error_str = str(e)
                logger.error(f"@{self.unique_id} 监控循环出错: {error_str}")
                consecutive_failures += 1
                
                # 检查是否是限流错误
                if "429" in error_str or "Too Many Requests" in error_str or "rate limit" in error_str.lower():
                    logger.warning(f"检测到限流错误，等待 {CONCURRENT_CONFIG['rate_limit_backoff']} 秒")
                    rate_limit_tracker['is_rate_limited'] = True
                    rate_limit_tracker['rate_limit_until'] = datetime.now() + timedelta(seconds=CONCURRENT_CONFIG['rate_limit_backoff'])
                    await asyncio.sleep(CONCURRENT_CONFIG['rate_limit_backoff'])
                else:
                    await asyncio.sleep(120)  # 发生异常时等待2分钟再重试
    
    async def stop_monitoring(self):
        """停止监控"""
        self.is_running = False
        try:
            if self.client and self.client.connected:
                logger.info(f"停止监控 @{self.unique_id}")
                await self.client.disconnect()
                # 等待断开完成
                await asyncio.sleep(0.5)
                # 断开连接后保存统计数据
                await self.save_stats()
        except Exception as e:
            logger.error(f"停止监控时出错 @{self.unique_id}: {str(e)}")
            # 即使出错也要保存统计数据
            await self.save_stats()
    
    async def save_stats(self):
        """保存优化后的统计数据到文件"""
        try:
            # 创建一个优化的数据结构用于保存
            save_data = {
                'unique_id': self.unique_id,
                'room_id': self.stats['room_id'],
                'title': self.stats['title'],
                'stream_status': self.stats['stream_status'],
                'create_time': self.stats['create_time'],
                'start_time': self.stats['start_time'].isoformat() if self.stats.get('start_time') else None,
                'end_time': datetime.now().isoformat(),
                'duration': (datetime.now() - self.stats['start_time']).total_seconds() if self.stats.get('start_time') else 0,
                'peak_viewers': self.stats['current_viewers'],  # 当前观众作为峰值观众
                'total_viewers': self.stats['total_viewers'],
                'like_count': self.stats['like_count'],
                'comment_count': self.stats['comment_count'],
                'gift_count': self.stats['gift_count'],
                'gift_value': self.stats['gift_value'],
                'follow_count': self.stats['follow_count'],
                'share_count': self.stats['share_count'],
                'subscribe_count': self.stats['subscribe_count'],
                # 优化后的数据
                'minute_data': self.stats['minute_data'],  # 分钟级数据记录
                'recent_comments': self.stats['comments'],  # 最近10条评论
                'recent_gifts': self.stats['gifts'],       # 最近10条礼物
                'gifts_summary': {                          # 礼物汇总而非详细记录
                    'total_gifts': self.stats['gift_count'],
                    'total_value': self.stats['gift_value'],
                    'unique_gift_types': len(self.stats['gifts_detail'])
                },
                'anti_detection_used': self.anti_detection,
                'data_optimized': True  # 标记这是优化后的数据格式
            }
            
            # 保存到传统stats目录（向后兼容）
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"stats/{self.unique_id}_{timestamp}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"优化后的统计数据已保存到 {filename}")
            
            # 同时保存到历史数据管理系统
            history_file = history_manager.save_session_data(self.unique_id, save_data)
            if history_file:
                logger.info(f"历史数据已保存到 {history_file}")
                
        except Exception as e:
            logger.error(f"保存统计数据失败: {str(e)}")


def monitor_thread_func(unique_id):
    """监控线程函数"""
    try:
        logger.info(f"监控线程已启动: {unique_id}")
        monitor = active_monitors[unique_id]
        asyncio.run(monitor.start_monitoring())
    except Exception as e:
        logger.error(f"监控线程异常: {str(e)}", exc_info=True)


# Flask 路由
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start_monitor', methods=['POST'])
def start_monitor():
    try:
        data = request.get_json()
        if not data:
            logger.error("请求数据为空或格式错误")
            return jsonify({'error': '请求数据格式错误', 'status': 'error'}), 400
        
        unique_id = data.get('unique_id', '').strip()
        sessionid = data.get('sessionid', '').strip() or None
        anti_detection = data.get('anti_detection', False)
        custom_interval = data.get('custom_interval')
        logger.info(f"收到开始监控请求: {unique_id}, sessionid: {'已提供' if sessionid else '未提供'}, 防检测: {anti_detection}")
    
        if not unique_id:
            logger.warning("没有提供用户名")
            return jsonify({'error': '请提供用户名', 'status': 'error'}), 400
        
        # 移除可能的@符号
        if unique_id.startswith('@'):
            unique_id = unique_id[1:]
            logger.info(f"移除@符号，处理后的ID: {unique_id}")
        
        # 检查是否已经在监控中
        if unique_id in active_monitors:
            logger.warning(f"用户 {unique_id} 已在监控中")
            return jsonify({
                'message': f'用户 @{unique_id} 已在监控中',
                'unique_id': unique_id,
                'status': 'success'  # 返回成功状态，避免前端认为出错
            })
        
        try:
            # 创建新的监控实例
            logger.info(f"创建监控实例: {unique_id}")
            monitor = TikTokLiveMonitor(unique_id, sessionid, anti_detection)
            
            # 设置自定义间隔
            if custom_interval and isinstance(custom_interval, int) and 1 <= custom_interval <= 500:
                monitor.custom_interval = custom_interval
                logger.info(f"设置自定义监控间隔: {custom_interval}秒")
            
            active_monitors[unique_id] = monitor
        
            # 在新线程中启动监控
            logger.info(f"启动监控线程: {unique_id}")
            thread = threading.Thread(target=monitor_thread_func, args=(unique_id,), daemon=True)
            thread.start()
            monitor_threads[unique_id] = thread
        
            # 保存监控配置
            config_manager.add_monitor_to_config(unique_id, sessionid, anti_detection)
        
            # 立即返回成功状态，并发送初始数据
            logger.info(f"发送初始数据: {unique_id}")
            monitor._send_default_data()
        
            logger.info(f"成功启动监控: {unique_id}")
            return jsonify({
                'message': f'开始监控 @{unique_id}',
                'unique_id': unique_id,
                'status': 'success'
            })
        except Exception as e:
            logger.error(f"创建或启动监控实例失败: {str(e)}", exc_info=True)
            return jsonify({'error': str(e), 'status': 'error'}), 500
    except Exception as e:
        logger.error(f"处理开始监控请求时发生未预期错误: {str(e)}", exc_info=True)
        return jsonify({'error': '服务器处理请求时出错', 'status': 'error'}), 500

@app.route('/stop_monitor', methods=['POST'])
def stop_monitor():
    data = request.get_json()
    unique_id = data.get('unique_id', '').strip()
    
    if not unique_id:
        return jsonify({'error': '请提供用户名', 'status': 'error'}), 400
        
    # 移除可能的@符号
    if unique_id.startswith('@'):
        unique_id = unique_id[1:]
        
    if unique_id not in active_monitors:
        return jsonify({'error': '该用户未被监控', 'status': 'error'}), 400
        
    try:
        # 获取监控实例
        monitor = active_monitors[unique_id]
        
        # 停止监控
        asyncio.run(monitor.stop_monitoring())
        
        # 从字典中移除
        del active_monitors[unique_id]
        if unique_id in monitor_threads:
            del monitor_threads[unique_id]
        
        # 从配置中移除
        config_manager.remove_monitor_from_config(unique_id)
            
        return jsonify({
            'message': f'已停止监控 @{unique_id}',
            'unique_id': unique_id,
            'status': 'success'
        })
    except Exception as e:
        logger.error(f"停止监控失败: {str(e)}")
        return jsonify({'error': str(e), 'status': 'error'}), 500

@app.route('/stop_all', methods=['POST'])
def stop_all():
    try:
        stopped_count = 0
        failed_count = 0
        
        # 停止所有监控
        for unique_id, monitor in list(active_monitors.items()):
            try:
                logger.info(f"正在停止监控: @{unique_id}")
                asyncio.run(monitor.stop_monitoring())
                stopped_count += 1
            except Exception as e:
                logger.error(f"停止监控 @{unique_id} 失败: {str(e)}")
                failed_count += 1
        
        # 清空字典
        active_monitors.clear()
        monitor_threads.clear()
        
        # 清空配置
        config_manager.save_monitor_config([])
        
        message = f'已停止 {stopped_count} 个监控'
        if failed_count > 0:
            message += f'，{failed_count} 个停止失败'
            
        return jsonify({
            'message': message,
            'stopped_count': stopped_count,
            'failed_count': failed_count,
            'status': 'success'
        })
    except Exception as e:
        logger.error(f"停止所有监控失败: {str(e)}")
        return jsonify({'error': str(e), 'status': 'error'}), 500

# 添加一个别名，以兼容前端请求
@app.route('/stop_all_monitors', methods=['POST'])
def stop_all_monitors():
    return stop_all()

@app.route('/list_monitors', methods=['GET'])
def get_active_monitors():
    return jsonify({
        'monitors': list(active_monitors.keys())
    })

@app.route('/get_stream_data', methods=['GET'])
def get_stream_data():
    unique_id = request.args.get('unique_id', '').strip()
    
    # 处理错误的请求参数
    if not unique_id or unique_id == "monitors":
        return jsonify({
            'is_live': False,
            'current_viewers': 0,
            'total_viewers': 0,
            'total_likes': 0,
            'total_gifts': 0,
            'total_comments': 0,
            'room_info': {},
            'chart_data': {'timestamps': [], 'viewer_counts': []},
            'comments': [],
            'gifts': [],
            'status': 'not_found'
        })
    
    # 移除可能的@符号
    if unique_id.startswith('@'):
        unique_id = unique_id[1:]
        
    # 如果直播间不存在，返回默认数据而不是404
    if unique_id not in active_monitors:
        return jsonify({
            'unique_id': unique_id,
            'is_live': False,
            'current_viewers': 0,
            'total_viewers': 0,
            'total_likes': 0,
            'total_gifts': 0,
            'total_comments': 0,
            'room_info': {},
            'chart_data': {'timestamps': [], 'viewer_counts': []},
            'comments': [],
            'gifts': [],
            'status': 'not_found'
        })
        
    monitor = active_monitors[unique_id]
    
    # 构建前端所需的数据结构
    room_info = monitor.stats.get('room_info', {})
    
    return jsonify({
        'unique_id': unique_id,
        'is_live': monitor.is_live,
        'room_info': room_info,
        'current_viewers': monitor.stats['current_viewers'],
        'total_viewers': monitor.stats['total_viewers'],
        'total_likes': monitor.stats['like_count'],
        'total_gifts': monitor.stats['gift_count'],
        'total_comments': monitor.stats['comment_count'],
        'total_shares': monitor.stats['share_count'],
        'total_follows': monitor.stats['follow_count'],
        'start_time': monitor.stats['start_time'].isoformat() if monitor.stats.get('start_time') else None,
        'chart_data': monitor.stats['chart_data'],
        'comments': monitor.stats['comments'][-20:],  # 仅发送最新的20条
        'gifts': monitor.stats['gifts'][-20:],        # 仅发送最新的20条
        'last_update_time': monitor.last_update_time,
        'status': 'success'
    })

@app.route('/update_chart_data', methods=['GET'])
def update_chart_data():
    data = {}
    for unique_id, monitor in active_monitors.items():
        data[unique_id] = {
            'current_viewers': monitor.stats['current_viewers'],
            'like_count': monitor.stats['like_count'],
            'comment_count': monitor.stats['comment_count'],
            'gift_count': monitor.stats['gift_count'],
            'gift_value': monitor.stats['gift_value'],
            'follow_count': monitor.stats['follow_count'],
            'share_count': monitor.stats['share_count'],
            'subscribe_count': monitor.stats['subscribe_count'],
            'is_live': monitor.is_live,
            'chart_data': monitor.stats['chart_data']
        }
    return jsonify(data)

@app.route('/monitor_info/<unique_id>')
def monitor_info(unique_id):
    """
    渲染指定 unique_id 的房间信息和礼物列表（读取 JSON 文件）
    """
    file_path = f"{unique_id}_tiktoklive_info.json"
    if not os.path.exists(file_path):
        return f"信息文件 {file_path} 未找到", 404
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    room_info = json.dumps(data.get('room_info', {}), indent=2, ensure_ascii=False)
    gift_info = json.dumps(data.get('gift_info', {}), indent=2, ensure_ascii=False)
    return render_template('monitor_info.html', unique_id=unique_id, room_info=room_info, gift_info=gift_info)

# 历史数据相关路由
@app.route('/history')
def history_page():
    """历史数据分析页面"""
    return render_template('history.html')

@app.route('/monitor_control')
def monitor_control_page():
    """监控管理控制台页面"""
    return render_template('monitor_control.html')

@app.route('/api/history/user/<unique_id>')
def get_user_history(unique_id):
    """获取指定用户的历史数据（新版本）"""
    days = request.args.get('days', 7, type=int)
    days = min(max(days, 1), 90)  # 限制在1-90天之间
    
    try:
        # 获取用户会话列表
        sessions = history_manager.get_user_sessions(unique_id)
        
        # 过滤指定天数内的数据
        cutoff_date = datetime.now() - timedelta(days=days)
        recent_sessions = []
        
        for session in sessions:
            try:
                session_start = datetime.fromisoformat(session['start_time'])
                if session_start >= cutoff_date:
                    recent_sessions.append(session)
            except Exception:
                continue
        
        return jsonify({
            'status': 'success',
            'unique_id': unique_id,
            'days': days,
            'total_sessions': len(recent_sessions),
            'sessions': recent_sessions
        })
    except Exception as e:
        logger.error(f"获取用户历史数据失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/summary')
def get_daily_summary():
    """获取每日汇总数据（新版本）"""
    date = request.args.get('date')  # 格式: YYYY-MM-DD
    
    try:
        # 如果没有提供日期，使用今天
        if not date:
            date = datetime.now().strftime('%Y-%m-%d')
        
        # 获取所有用户
        users = history_manager.get_all_users()
        summaries = []
        
        for user in users:
            # 获取该用户的会话
            sessions = history_manager.get_user_sessions(user)
            
            # 过滤指定日期的会话
            daily_sessions = []
            for session in sessions:
                try:
                    session_start = datetime.fromisoformat(session['start_time'])
                    session_date = session_start.strftime('%Y-%m-%d')
                    if session_date == date:
                        daily_sessions.append(session)
                except Exception:
                    continue
            
            if daily_sessions:
                # 计算该用户当日的汇总数据
                total_duration = sum(s.get('duration_seconds', 0) for s in daily_sessions)
                peak_viewers = max(s.get('peak_viewers', 0) for s in daily_sessions)
                total_likes = sum(s.get('total_likes', 0) for s in daily_sessions)
                
                summaries.append({
                    'unique_id': user,
                    'date': date,
                    'total_sessions': len(daily_sessions),
                    'total_duration': total_duration,
                    'peak_viewers': peak_viewers,
                    'total_likes': total_likes
                })
        
        # 按峰值观众数排序
        summaries.sort(key=lambda x: x.get('peak_viewers', 0), reverse=True)
        
        return jsonify({
            'status': 'success',
            'date': date,
            'total_users': len(summaries),
            'data': summaries
        })
    except Exception as e:
        logger.error(f"获取每日汇总失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/export')
def export_history():
    """导出历史数据（新版本）"""
    unique_id = request.args.get('unique_id')
    format_type = request.args.get('format', 'json')  # json 或 csv
    
    try:
        if not unique_id:
            return jsonify({'error': '请提供用户ID'}), 400
        
        # 使用新的导出方法
        export_file = history_manager.export_user_data(unique_id, format_type)
        
        if export_file:
            # 返回文件下载
            directory = os.path.dirname(export_file)
            filename = os.path.basename(export_file)
            return send_from_directory(directory, filename, as_attachment=True)
        else:
            return jsonify({'error': '没有找到要导出的数据'}), 404
    except Exception as e:
        logger.error(f"导出数据失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/users')
def get_monitored_users():
    """获取所有被监控过的用户列表（新版本）"""
    try:
        users = history_manager.get_all_users()
        
        # 获取每个用户的基本统计信息
        user_stats = []
        for user in users:
            sessions = history_manager.get_user_sessions(user, limit=1)  # 只获取最新一条
            if sessions:
                latest_session = sessions[0]
                user_stats.append({
                    'unique_id': user,
                    'latest_session': latest_session['start_time'],
                    'total_sessions': len(history_manager.get_user_sessions(user))
                })
            else:
                user_stats.append({
                    'unique_id': user,
                    'latest_session': None,
                    'total_sessions': 0
                })
        
        # 按最新会话时间排序
        user_stats.sort(key=lambda x: x.get('latest_session') or '', reverse=True)
        
        return jsonify({
            'status': 'success',
            'users': [stat['unique_id'] for stat in user_stats],
            'user_stats': user_stats,
            'total_count': len(user_stats)
        })
    except Exception as e:
        logger.error(f"获取用户列表失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/analytics/<unique_id>')
def get_user_analytics(unique_id):
    """获取用户数据分析（新版本）"""
    try:
        days = request.args.get('days', 30, type=int)
        analytics = history_manager.get_user_analytics(unique_id, days)
        
        return jsonify({
            'status': 'success',
            'unique_id': unique_id,
            'analytics': analytics
        })
        
    except Exception as e:
        logger.error(f"获取用户分析失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/session/<unique_id>/<session_id>')
def get_session_detail(unique_id, session_id):
    """获取指定会话的详细数据"""
    try:
        session_data = history_manager.get_session_detail(unique_id, session_id)
        
        if session_data:
            return jsonify({
                'status': 'success',
                'unique_id': unique_id,
                'session_id': session_id,
                'data': session_data
            })
        else:
            return jsonify({
                'status': 'not_found',
                'message': '会话数据未找到'
            }), 404
            
    except Exception as e:
        logger.error(f"获取会话详情失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 配置管理相关路由
@app.route('/api/config/load_monitors', methods=['POST'])
def load_saved_monitors():
    """加载保存的监控配置并启动"""
    try:
        configs = config_manager.load_monitor_config()
        
        if not configs:
            return jsonify({
                'status': 'success',
                'message': '没有保存的监控配置',
                'loaded_count': 0
            })
        
        loaded_count = 0
        failed_list = []
        
        for config in configs:
            unique_id = config['unique_id']
            sessionid = config.get('sessionid')
            anti_detection = config.get('anti_detection', False)
            
            # 检查是否已经在监控中
            if unique_id in active_monitors:
                logger.info(f"监控 {unique_id} 已在运行中，跳过")
                continue
            
            try:
                # 创建监控实例
                monitor = TikTokLiveMonitor(unique_id, sessionid, anti_detection)
                active_monitors[unique_id] = monitor
                
                # 启动监控线程
                thread = threading.Thread(target=monitor_thread_func, args=(unique_id,), daemon=True)
                thread.start()
                monitor_threads[unique_id] = thread
                
                # 发送初始数据
                monitor._send_default_data()
                
                loaded_count += 1
                logger.info(f"成功加载监控: {unique_id}")
                
            except Exception as e:
                logger.error(f"加载监控 {unique_id} 失败: {str(e)}")
                failed_list.append({'unique_id': unique_id, 'error': str(e)})
        
        return jsonify({
            'status': 'success',
            'message': f'成功加载 {loaded_count} 个监控配置',
            'loaded_count': loaded_count,
            'failed_count': len(failed_list),
            'failed_list': failed_list
        })
        
    except Exception as e:
        logger.error(f"加载监控配置失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/config/get_saved_monitors', methods=['GET'])
def get_saved_monitors():
    """获取保存的监控配置列表"""
    try:
        configs = config_manager.load_monitor_config()
        return jsonify({
            'status': 'success',
            'configs': configs,
            'count': len(configs)
        })
    except Exception as e:
        logger.error(f"获取监控配置失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 历史数据清理相关路由
@app.route('/api/history/clear', methods=['POST'])
def clear_history_data():
    """清除历史数据"""
    try:
        data = request.get_json()
        unique_id = data.get('unique_id')  # 可选，不提供则清除所有
        confirm_token = data.get('confirm_token')  # 必须提供确认令牌
        
        result = history_manager.clear_history_data(unique_id, confirm_token)
        
        if result['success']:
            return jsonify({
                'status': 'success',
                'message': result['message'],
                'deleted_files': result['deleted_files']
            })
        else:
            return jsonify({
                'status': 'error',
                'message': result['message'],
                'error': result.get('error')
            }), 400
            
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"清除历史数据失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/history/stats', methods=['GET'])
def get_history_stats():
    """获取历史数据使用统计"""
    try:
        stats = history_manager.get_data_usage_stats()
        if stats:
            return jsonify({
                'status': 'success',
                'stats': stats
            })
        else:
            return jsonify({'error': '获取统计数据失败'}), 500
    except Exception as e:
        logger.error(f"获取历史数据统计失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 监控管理API路由
@app.route('/api/monitor/list_detailed', methods=['GET'])
def get_detailed_monitor_list():
    """获取详细的监控列表"""
    try:
        monitor_list = []
        for unique_id, monitor in active_monitors.items():
            status = 'stopped'
            if monitor.is_running:
                if monitor.is_paused:
                    status = 'paused'
                else:
                    status = 'active'
            
            monitor_info = {
                'unique_id': unique_id,
                'status': status,
                'is_live': monitor.is_live,
                'current_viewers': monitor.stats.get('current_viewers', 0),
                'total_viewers': monitor.stats.get('total_viewers', 0),
                'anti_detection': monitor.anti_detection,
                'interval': monitor.custom_interval or CONCURRENT_CONFIG['update_interval_base'],
                'last_update': monitor.last_update_time,
                'connected': monitor.client.connected if monitor.client else False
            }
            monitor_list.append(monitor_info)
        
        return jsonify({
            'status': 'success',
            'monitors': monitor_list,
            'total_count': len(monitor_list),
            'active_count': len([m for m in monitor_list if m['status'] == 'active'])
        })
    except Exception as e:
        logger.error(f"获取监控列表失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitor/pause', methods=['POST'])
def pause_monitor():
    """暂停指定监控"""
    try:
        data = request.get_json()
        unique_id = data.get('unique_id', '').strip()
        
        if not unique_id or unique_id not in active_monitors:
            return jsonify({'error': '监控不存在'}), 400
        
        monitor = active_monitors[unique_id]
        monitor.is_paused = True
        
        logger.info(f"已暂停监控: @{unique_id}")
        return jsonify({
            'status': 'success',
            'message': f'已暂停监控 @{unique_id}',
            'unique_id': unique_id
        })
    except Exception as e:
        logger.error(f"暂停监控失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitor/resume', methods=['POST'])
def resume_monitor():
    """恢复指定监控"""
    try:
        data = request.get_json()
        unique_id = data.get('unique_id', '').strip()
        
        if not unique_id or unique_id not in active_monitors:
            return jsonify({'error': '监控不存在'}), 400
        
        monitor = active_monitors[unique_id]
        monitor.is_paused = False
        
        logger.info(f"已恢复监控: @{unique_id}")
        return jsonify({
            'status': 'success',
            'message': f'已恢复监控 @{unique_id}',
            'unique_id': unique_id
        })
    except Exception as e:
        logger.error(f"恢复监控失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitor/pause_all', methods=['POST'])
def pause_all_monitors():
    """暂停所有监控"""
    try:
        paused_count = 0
        for unique_id, monitor in active_monitors.items():
            if monitor.is_running and not monitor.is_paused:
                monitor.is_paused = True
                paused_count += 1
        
        logger.info(f"已暂停所有监控，共 {paused_count} 个")
        return jsonify({
            'status': 'success',
            'message': f'已暂停 {paused_count} 个监控',
            'paused_count': paused_count
        })
    except Exception as e:
        logger.error(f"暂停所有监控失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitor/resume_all', methods=['POST'])
def resume_all_monitors():
    """恢复所有监控"""
    try:
        resumed_count = 0
        for unique_id, monitor in active_monitors.items():
            if monitor.is_running and monitor.is_paused:
                monitor.is_paused = False
                resumed_count += 1
        
        logger.info(f"已恢复所有监控，共 {resumed_count} 个")
        return jsonify({
            'status': 'success',
            'message': f'已恢复 {resumed_count} 个监控',
            'resumed_count': resumed_count
        })
    except Exception as e:
        logger.error(f"恢复所有监控失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitor/update_interval', methods=['POST'])
def update_monitor_interval():
    """更新监控间隔"""
    try:
        data = request.get_json()
        unique_id = data.get('unique_id', '').strip()
        interval = data.get('interval')
        
        if not unique_id or unique_id not in active_monitors:
            return jsonify({'error': '监控不存在'}), 400
        
        if not isinstance(interval, int) or interval < 1 or interval > 500:
            return jsonify({'error': '间隔必须在1-500秒之间'}), 400
        
        monitor = active_monitors[unique_id]
        monitor.custom_interval = interval
        
        logger.info(f"已更新监控间隔: @{unique_id} -> {interval}秒")
        return jsonify({
            'status': 'success',
            'message': f'已更新 @{unique_id} 的监控间隔为 {interval}秒',
            'unique_id': unique_id,
            'interval': interval
        })
    except Exception as e:
        logger.error(f"更新监控间隔失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitor/update_anti_detection', methods=['POST'])
def update_anti_detection():
    """更新防检测设置"""
    try:
        data = request.get_json()
        unique_id = data.get('unique_id', '').strip()
        anti_detection = data.get('anti_detection', False)
        
        if not unique_id or unique_id not in active_monitors:
            return jsonify({'error': '监控不存在'}), 400
        
        monitor = active_monitors[unique_id]
        monitor.anti_detection = anti_detection
        
        # 更新配置文件
        config_manager.add_monitor_to_config(unique_id, monitor.sessionid, anti_detection)
        
        logger.info(f"已更新防检测设置: @{unique_id} -> {anti_detection}")
        return jsonify({
            'status': 'success',
            'message': f'已{"启用" if anti_detection else "禁用"} @{unique_id} 的防检测模式',
            'unique_id': unique_id,
            'anti_detection': anti_detection
        })
    except Exception as e:
        logger.error(f"更新防检测设置失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitor/global_settings', methods=['POST'])
def update_global_settings():
    """更新全局设置"""
    try:
        data = request.get_json()
        global_interval = data.get('global_interval')
        max_concurrent = data.get('max_concurrent')
        
        updated = []
        
        if global_interval and isinstance(global_interval, int) and 1 <= global_interval <= 500:
            CONCURRENT_CONFIG['update_interval_base'] = global_interval
            updated.append(f'监控间隔: {global_interval}秒')
        
        if max_concurrent and isinstance(max_concurrent, int) and 1 <= max_concurrent <= 30:
            CONCURRENT_CONFIG['max_concurrent_connections'] = max_concurrent
            updated.append(f'最大并发: {max_concurrent}个')
        
        if not updated:
            return jsonify({'error': '无有效设置更新'}), 400
        
        logger.info(f"已更新全局设置: {', '.join(updated)}")
        return jsonify({
            'status': 'success',
            'message': f'已更新全局设置: {", ".join(updated)}',
            'settings': {
                'update_interval_base': CONCURRENT_CONFIG['update_interval_base'],
                'max_concurrent_connections': CONCURRENT_CONFIG['max_concurrent_connections']
            }
        })
    except Exception as e:
        logger.error(f"更新全局设置失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/monitor/global_settings', methods=['GET'])
def get_global_settings():
    """获取当前全局设置"""
    try:
        return jsonify({
            'status': 'success',
            'settings': {
                'update_interval_base': CONCURRENT_CONFIG['update_interval_base'],
                'max_concurrent_connections': CONCURRENT_CONFIG['max_concurrent_connections']
            }
        })
    except Exception as e:
        logger.error(f"获取全局设置失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ==================== 账号池管理API ====================

@app.route('/api/session_pool', methods=['GET'])
def get_session_pool():
    """获取账号池信息"""
    try:
        return jsonify({
            'status': 'success',
            'sessions': session_pool.get_all_sessions(),
            'stats': session_pool.get_stats(),
            'current': session_pool.get_current_session_info()
        })
    except Exception as e:
        logger.error(f"获取账号池信息失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/session_pool/add', methods=['POST'])
def add_session_to_pool():
    """添加账号到账号池"""
    try:
        data = request.get_json()
        sessionid = data.get('sessionid')
        name = data.get('name')
        
        if not sessionid:
            return jsonify({'error': '请提供sessionid'}), 400
        
        if len(sessionid) < 20:
            return jsonify({'error': 'sessionid格式不正确'}), 400
        
        success = session_pool.add_session(sessionid, name)
        if success:
            return jsonify({
                'status': 'success',
                'message': f'已添加账号: {name or "新账号"}',
                'stats': session_pool.get_stats()
            })
        else:
            return jsonify({'error': '该sessionid已存在'}), 400
    except Exception as e:
        logger.error(f"添加账号到池失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/session_pool/remove', methods=['POST'])
def remove_session_from_pool():
    """从账号池移除账号"""
    try:
        data = request.get_json()
        sessionid = data.get('sessionid')
        index = data.get('index')
        
        # 支持通过索引或sessionid删除
        if index is not None and 0 <= index < len(session_pool.sessions):
            sessionid = session_pool.sessions[index]['sessionid']
        
        if not sessionid:
            return jsonify({'error': '请提供sessionid或index'}), 400
        
        success = session_pool.remove_session(sessionid)
        if success:
            return jsonify({
                'status': 'success',
                'message': '已移除账号',
                'stats': session_pool.get_stats()
            })
        else:
            return jsonify({'error': '未找到该账号'}), 404
    except Exception as e:
        logger.error(f"移除账号失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/session_pool/rotate', methods=['POST'])
def force_rotate_session():
    """强制轮换到下一个账号"""
    try:
        info = session_pool.force_rotate()
        return jsonify({
            'status': 'success',
            'message': f'已切换到: {info["name"]}',
            'current': info,
            'stats': session_pool.get_stats()
        })
    except Exception as e:
        logger.error(f"强制轮换失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/session_pool/interval', methods=['POST'])
def set_rotation_interval():
    """设置账号池轮换间隔"""
    try:
        data = request.get_json()
        minutes = data.get('minutes')
        
        if not minutes or not isinstance(minutes, (int, float)) or minutes < 1:
            return jsonify({'error': '请提供有效的轮换间隔（分钟）'}), 400
        
        seconds = int(minutes * 60)
        session_pool.set_rotation_interval(seconds)
        
        return jsonify({
            'status': 'success',
            'message': f'轮换间隔已设置为 {minutes} 分钟',
            'stats': session_pool.get_stats()
        })
    except Exception as e:
        logger.error(f"设置轮换间隔失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/session_pool/batch_add', methods=['POST'])
def batch_add_sessions():
    """批量添加账号到账号池"""
    try:
        data = request.get_json()
        sessions = data.get('sessions', [])
        
        if not sessions:
            return jsonify({'error': '请提供sessions数组'}), 400
        
        added = 0
        failed = 0
        for item in sessions:
            if isinstance(item, str):
                # 仅提供sessionid
                if session_pool.add_session(item):
                    added += 1
                else:
                    failed += 1
            elif isinstance(item, dict):
                # 提供sessionid和name
                sessionid = item.get('sessionid')
                name = item.get('name')
                if sessionid and session_pool.add_session(sessionid, name):
                    added += 1
                else:
                    failed += 1
        
        return jsonify({
            'status': 'success',
            'message': f'成功添加 {added} 个账号，失败 {failed} 个',
            'added': added,
            'failed': failed,
            'stats': session_pool.get_stats()
        })
    except Exception as e:
        logger.error(f"批量添加账号失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# ==================== 启动监控时使用账号池 ====================

@app.route('/start_monitor_with_pool', methods=['POST'])
def start_monitor_with_pool():
    """使用账号池启动监控（推荐用于18+直播间）"""
    try:
        data = request.get_json()
        unique_id = data.get('unique_id', '').strip().replace('@', '')
        anti_detection = data.get('anti_detection', True)  # 默认启用防检测
        
        if not unique_id:
            return jsonify({'error': '请提供用户ID'}), 400
        
        if unique_id in active_monitors:
            return jsonify({'error': f'@{unique_id} 已在监控中'}), 400
        
        if not session_pool.sessions:
            return jsonify({'error': '账号池为空，请先添加TikTok账号'}), 400
        
        # 使用账号池创建监控
        monitor = TikTokLiveMonitor(
            unique_id, 
            sessionid=None,  # 不使用单独的sessionid
            anti_detection=anti_detection,
            use_session_pool=True  # 启用账号池
        )
        active_monitors[unique_id] = monitor
        
        # 启动监控线程
        thread = threading.Thread(target=monitor_thread_func, args=(unique_id,), daemon=True)
        thread.start()
        monitor_threads[unique_id] = thread
        
        # 保存到配置（标记使用账号池）
        config_manager.add_monitor_to_config(unique_id, sessionid=None, anti_detection=anti_detection, use_session_pool=True)
        
        pool_info = session_pool.get_current_session_info()
        logger.info(f"使用账号池启动监控: @{unique_id}，当前账号: {pool_info['name']}")
        
        return jsonify({
            'status': 'success',
            'message': f'已开始监控 @{unique_id}（使用账号池）',
            'unique_id': unique_id,
            'pool_info': pool_info
        })
    except Exception as e:
        logger.error(f"启动监控失败: {str(e)}")
        return jsonify({'error': str(e)}), 500

# 自动加载配置的启动函数
def auto_load_monitors():
    """应用启动时自动加载监控配置"""
    try:
        logger.info("正在自动加载保存的监控配置...")
        configs = config_manager.load_monitor_config()
        
        if not configs:
            logger.info("没有保存的监控配置")
            return
        
        loaded_count = 0
        for config in configs:
            unique_id = config['unique_id']
            sessionid = config.get('sessionid')
            anti_detection = config.get('anti_detection', False)
            use_session_pool = config.get('use_session_pool', False)
            
            try:
                # 创建监控实例（但不立即连接，等待首次检查）
                monitor = TikTokLiveMonitor(unique_id, sessionid, anti_detection, use_session_pool)
                active_monitors[unique_id] = monitor
                
                # 启动监控线程
                thread = threading.Thread(target=monitor_thread_func, args=(unique_id,), daemon=True)
                thread.start()
                monitor_threads[unique_id] = thread
                
                loaded_count += 1
                logger.info(f"自动加载监控: {unique_id}")
                
            except Exception as e:
                logger.error(f"自动加载监控 {unique_id} 失败: {str(e)}")
        
        logger.info(f"自动加载完成，成功加载 {loaded_count} 个监控")
        
    except Exception as e:
        logger.error(f"自动加载监控配置失败: {str(e)}")

def migrate_old_data():
    """
    数据迁移函数：将旧的history结构迁移到新结构
    只在首次启动时运行一次
    """
    try:
        migration_flag = os.path.join('history', '.migration_complete')
        if os.path.exists(migration_flag):
            logger.info("数据迁移已完成，跳过迁移过程")
            return
        
        logger.info("开始数据迁移：将旧格式转换为新格式...")
        
        # 检查是否存在旧的数据结构
        old_daily_dir = os.path.join('history', 'daily')
        old_sessions_dir = os.path.join('history', 'sessions')
        
        migrated_users = set()
        
        # 迁移sessions数据（优先）
        if os.path.exists(old_sessions_dir):
            for filename in os.listdir(old_sessions_dir):
                if filename.endswith('.json'):
                    try:
                        file_path = os.path.join(old_sessions_dir, filename)
                        with open(file_path, 'r', encoding='utf-8') as f:
                            session_data = json.load(f)
                        
                        unique_id = session_data.get('unique_id')
                        if unique_id:
                            # 创建新的会话数据结构
                            new_session_data = {
                                'unique_id': unique_id,
                                'room_id': session_data.get('room_id'),
                                'title': session_data.get('title', ''),
                                'stream_status': session_data.get('stream_status', 0),
                                'start_time': session_data.get('start_time'),
                                'end_time': session_data.get('end_time'),
                                'duration': session_data.get('duration', 0),
                                'current_viewers': session_data.get('peak_viewers', 0),
                                'total_viewers': session_data.get('total_viewers', 0),
                                'like_count': session_data.get('like_count', 0),
                                'comment_count': session_data.get('comment_count', 0),
                                'gift_count': session_data.get('gift_count', 0),
                                'gift_value': session_data.get('gift_value', 0),
                                'follow_count': session_data.get('follow_count', 0),
                                'share_count': session_data.get('share_count', 0),
                                'subscribe_count': session_data.get('subscribe_count', 0),
                                'gifts_detail': session_data.get('gifts_detail', {}),
                                'minute_data': session_data.get('minute_data', []),
                                'anti_detection_used': session_data.get('anti_detection_used', False)
                            }
                            
                            # 使用新的保存方法
                            history_manager.save_session_data(unique_id, new_session_data)
                            migrated_users.add(unique_id)
                            logger.info(f"迁移会话数据: {unique_id} - {filename}")
                            
                    except Exception as e:
                        logger.error(f"迁移会话文件 {filename} 失败: {str(e)}")
        
        # 迁移daily数据（补充信息）
        if os.path.exists(old_daily_dir):
            for filename in os.listdir(old_daily_dir):
                if filename.endswith('.json'):
                    try:
                        file_path = os.path.join(old_daily_dir, filename)
                        with open(file_path, 'r', encoding='utf-8') as f:
                            daily_data = json.load(f)
                        
                        unique_id = daily_data.get('unique_id')
                        if unique_id and unique_id not in migrated_users:
                            # 如果没有会话数据，从daily数据创建一个虚拟会话
                            session_date = daily_data.get('date', datetime.now().strftime('%Y-%m-%d'))
                            fake_session = {
                                'unique_id': unique_id,
                                'title': f"历史数据 {session_date}",
                                'start_time': f"{session_date}T00:00:00",
                                'end_time': f"{session_date}T23:59:59",
                                'duration': daily_data.get('total_duration', 0),
                                'current_viewers': daily_data.get('peak_viewers', 0),
                                'total_viewers': daily_data.get('total_viewers', 0),
                                'like_count': daily_data.get('total_likes', 0),
                                'comment_count': daily_data.get('total_comments', 0),
                                'gift_count': daily_data.get('total_gifts', 0),
                                'gift_value': daily_data.get('total_gift_value', 0),
                                'follow_count': daily_data.get('total_follows', 0),
                                'share_count': daily_data.get('total_shares', 0),
                                'subscribe_count': 0,
                                'minute_data': [],
                                'anti_detection_used': False
                            }
                            
                            history_manager.save_session_data(unique_id, fake_session)
                            migrated_users.add(unique_id)
                            logger.info(f"从daily数据创建会话: {unique_id} - {filename}")
                            
                    except Exception as e:
                        logger.error(f"迁移daily文件 {filename} 失败: {str(e)}")
        
        # 创建迁移完成标记
        with open(migration_flag, 'w', encoding='utf-8') as f:
            f.write(json.dumps({
                'migration_date': datetime.now().isoformat(),
                'migrated_users': list(migrated_users),
                'total_migrated': len(migrated_users)
            }, ensure_ascii=False, indent=2))
        
        logger.info(f"数据迁移完成！共迁移 {len(migrated_users)} 个用户的数据")
        
        # 可选：备份旧数据
        if migrated_users:
            backup_dir = os.path.join('history', 'backup_old_format')
            os.makedirs(backup_dir, exist_ok=True)
            
            if os.path.exists(old_daily_dir):
                import shutil
                shutil.move(old_daily_dir, os.path.join(backup_dir, 'daily'))
                logger.info("旧的daily数据已备份")
            
            if os.path.exists(old_sessions_dir):
                import shutil
                shutil.move(old_sessions_dir, os.path.join(backup_dir, 'sessions'))
                logger.info("旧的sessions数据已备份")
                
    except Exception as e:
        logger.error(f"数据迁移失败: {str(e)}")

if __name__ == '__main__':
    # 打印启动信息和Web UI访问地址
    print("=" * 60)
    print("🚀 TikTok 直播监控系统启动中...")
    print("=" * 60)
    print()
    print("📱 Web UI 访问地址:")
    print("  本地访问: http://localhost:5000")
    print("  本地访问: http://127.0.0.1:5000")
    print("  局域网访问: http://0.0.0.0:5000")
    print()
    print("🔧 功能特性:")
    print("  ✓ 实时直播数据监控")
    print("  ✓ 历史数据分析 (新结构)")
    print("  ✓ 配置管理")
    print("  ✓ 数据导出")
    print("  ✓ 自动日志轮转 (最大1MB)")
    print("  ✓ 多账号并发监控优化")
    print("  ✓ 每账号独立文件夹存储")
    print("  ✓ 每场直播独立记录文件")
    print()
    print("⚡ 防限流优化说明:")
    print(f"  最大并发连接: {CONCURRENT_CONFIG['max_concurrent_connections']} 个")
    print(f"  基础监控间隔: {CONCURRENT_CONFIG['update_interval_base']} 秒")
    print(f"  限流退避时间: {CONCURRENT_CONFIG['rate_limit_backoff']} 秒")
    print("  监控间隔: 2-5分钟 (根据账号数量自动调整)")
    print("  离线用户: 10分钟重试间隔")
    print("  错开启动: 避免API请求冲突")
    print()
    print("🔄 账号池轮换系统 (新功能):")
    pool_stats = session_pool.get_stats()
    print(f"  已配置账号: {pool_stats['total_sessions']} 个")
    print(f"  轮换间隔: {pool_stats['rotation_interval_minutes']} 分钟")
    print("  支持24个TikTok账号轮换使用")
    print("  被限流账号自动切换到下一个")
    print()
    print("📡 账号池API:")
    print("  GET  /api/session_pool          - 查看账号池状态")
    print("  POST /api/session_pool/add      - 添加账号")
    print("  POST /api/session_pool/batch_add - 批量添加账号")
    print("  POST /api/session_pool/rotate   - 强制切换账号")
    print("  POST /api/session_pool/interval - 设置轮换间隔")
    print("  POST /start_monitor_with_pool   - 使用账号池启动监控")
    print()
    print("📁 新数据结构:")
    print("  history/{账号名}/session_YYYYMMDD_HHMMSS.json")
    print("  优化存储：压缩格式，避免大文件")
    print("  便于分析：每场直播独立文件")
    print()
    print("📋 日志级别控制:")
    print("  生产环境: $env:LOG_LEVEL = 'WARNING'")
    print("  调试模式: $env:LOG_LEVEL = 'DEBUG'")
    print()
    print("=" * 60)
    print()
    
    # 数据迁移检查
    migrate_old_data()
    
    # 启动时自动加载监控配置
    auto_load_monitors()
    
    # 启动Flask应用
    try:
        socketio.run(app, host='0.0.0.0', port=5000, debug=False)
    except KeyboardInterrupt:
        print("\n\n👋 程序已停止，感谢使用！")
    except Exception as e:
        print(f"\n❌ 程序启动失败: {str(e)}")
        print("请检查端口5000是否被占用或查看错误日志")