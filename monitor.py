import asyncio
import sys
import time
import requests
import threading
from TikTokLive import TikTokLiveClient
from TikTokLive.events import (
    ConnectEvent, DisconnectEvent, LiveEndEvent, 
    CommentEvent, GiftEvent, LikeEvent, ShareEvent, FollowEvent
)
from TikTokLive.events.proto_events import RoomUserSeqEvent
from TikTokLive.client.logger import LogLevel
from datetime import datetime

#
# Arguments
#
if len(sys.argv) < 2:
    print("Usage: python monitor.py <unique_id>")
    sys.exit(1)

unique_id: str = sys.argv[1]

#
# Client
#
client = TikTokLiveClient(
    unique_id=unique_id if unique_id.startswith("@") else f"@{unique_id}",
)
client.logger.setLevel(LogLevel.INFO.value)

#
# Global State
#
monitor_data = []
total_viewers = 0
room_info = {}
is_connected = False
room_title = ""
host_name = ""
avatar_url = ""
room_id = ""


#
# Connection & Room Info Management
#
def send_data_to_flask(data: dict):
    """Send data to the Flask frontend"""
    try:
        # 确保每次发送数据都包含 unique_id
        data['unique_id'] = unique_id.replace('@', '')
        
        response = requests.post('http://localhost:5000/update_stream_data', json=data, timeout=5)
        if response.status_code == 200:
            print("数据成功发送到Flask")
        else:
            print(f"发送数据到Flask失败: 状态码 {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"发送数据到Flask失败: {e}")


async def fetch_room_info():
    """尝试通过多种方式获取房间信息"""
    global room_id, room_title, host_name, avatar_url
    
    room_info_data = None
    
    try:
        # 方法1: 直接通过client.room_info获取
        if hasattr(client, 'room_info') and client.room_info:
            room_info_data = client.room_info
            
        # 方法2: 通过get_room_info方法获取(如果存在)
        if not room_info_data and hasattr(client, 'get_room_info'):
            try:
                room_info_data = await client.get_room_info()
            except:
                pass
                
        # 方法3: 通过fetch_room_info方法获取(如果存在)
        if not room_info_data and hasattr(client, 'fetch_room_info'):
            try:
                room_info_data = await client.fetch_room_info()
            except:
                pass
                
        # 方法4: 通过client.api获取(如果存在)
        if not room_info_data and hasattr(client, 'api') and hasattr(client.api, 'get_room_info'):
            try:
                room_info_data = await client.api.get_room_info()
            except:
                pass
        
        # 如果获取到了房间信息，提取需要的字段
        if room_info_data:
            # 处理不同结构的API返回
            if isinstance(room_info_data, dict):
                # 提取room_id - 尝试多种可能的字段名
                if 'room_id' in room_info_data:
                    room_id = room_info_data['room_id']
                elif 'id' in room_info_data:
                    room_id = room_info_data['id']
                elif hasattr(client, 'room_id'):
                    room_id = client.room_id
                else:
                    room_id = unique_id.replace('@', '')
                
                # 提取标题
                if 'title' in room_info_data:
                    room_title = room_info_data['title']
                elif 'description' in room_info_data:
                    room_title = room_info_data['description']
                else:
                    room_title = f"{unique_id.replace('@', '')}的直播间"
                
                # 提取主播信息
                owner_info = room_info_data.get('owner', {})
                if isinstance(owner_info, dict):
                    host_name = owner_info.get('nickname', unique_id.replace('@', ''))
                    avatar_thumb = owner_info.get('avatar_thumb', {})
                    if isinstance(avatar_thumb, dict):
                        url_list = avatar_thumb.get('url_list', [])
                        if url_list and len(url_list) > 0:
                            avatar_url = url_list[0]
            
            # 发送房间信息到Flask
            send_data_to_flask({
                "room_info": {
                    "room_id": room_id,
                    "room_title": room_title,
                    "host_name": host_name,
                    "avatar_url": avatar_url
                }
            })
            
            # 如果有观众数据，也一并发送
            if isinstance(room_info_data, dict):
                if "user_count" in room_info_data:
                    send_data_to_flask({
                        "current_viewers": room_info_data.get("user_count", 0)
                    })
                
                if "total_user_count" in room_info_data:
                    send_data_to_flask({
                        "total_viewers": room_info_data.get("total_user_count", 0)
                    })
                    
                if "like_count" in room_info_data:
                    send_data_to_flask({
                        "set_total_likes": room_info_data.get("like_count", 0)
                    })
            
            return True
        
        return False
    except Exception as e:
        print(f"获取房间信息出错: {e}")
        return False


@client.on(ConnectEvent)
async def on_connect(event: ConnectEvent):
    """
    Handle connection to the livestream
    """
    global is_connected, room_id, room_title, host_name, avatar_url

    is_connected = True
    print(f"已连接到 @{client.unique_id}")
    
    # 发送初始连接状态
    send_data_to_flask({
        "is_live": True,
        "start_time": datetime.now().isoformat(),
    })
    
    # Retry until room_info is available
    retry_count = 0
    while retry_count < 10:
        print(f"等待room_info... (尝试 {retry_count + 1})")
        
        # 尝试获取房间信息
        if await fetch_room_info():
            break  # 成功获取room_info，跳出循环
        
        # 未成功获取，等待后重试
        await asyncio.sleep(1)
        retry_count += 1
    
    # 如果多次尝试后仍未能获取room_info，创建一个基本的房间信息
    if retry_count >= 10:
        print("多次尝试后未能获取room_info，创建默认信息。")
        cleaned_id = unique_id.replace('@', '')
        room_id = cleaned_id
        room_title = f"{cleaned_id}的直播间"
        host_name = cleaned_id
        
        send_data_to_flask({
            "room_info": {
                "room_id": room_id,
                "room_title": room_title,
                "host_name": host_name,
                "avatar_url": avatar_url
            },
        })


@client.on(DisconnectEvent)
async def on_disconnect(event: DisconnectEvent):
    """
    Handle disconnection from the livestream
    """
    global is_connected
    
    is_connected = False
    print(f"与 @{client.unique_id} 的连接已断开")
    send_data_to_flask({"is_live": False})


@client.on(LiveEndEvent)
async def on_live_end(event: LiveEndEvent):
    """
    Handle end of the livestream
    """
    global is_connected
    
    is_connected = False
    print(f"@{client.unique_id} 的直播已结束")
    send_data_to_flask({"is_live": False})


@client.on(RoomUserSeqEvent)
async def on_room_user_seq(event: RoomUserSeqEvent):
    """
    Handle viewer count updates from RoomUserSeqEvent
    """
    global monitor_data, total_viewers
    
    try:
        data = event.__dict__
        current_viewers = None
        
        # 尝试获取观众人数，检查可能的字段名
        if hasattr(event, 'total'):
            current_viewers = event.total
        elif hasattr(event, 'user_count'):
            current_viewers = event.user_count
        elif hasattr(event, 'viewer_count'):
            current_viewers = event.viewer_count
        elif hasattr(event, 'member_count'):
            current_viewers = event.member_count
            
        if current_viewers is not None:
            # 存储数据点
            monitor_data.append((time.time(), current_viewers))
            # 最多保留最近30分钟的数据
            cutoff_time = time.time() - 30 * 60
            monitor_data = [(ts, v) for ts, v in monitor_data if ts >= cutoff_time]
            
            print(f"当前观众人数: {current_viewers}")
            send_data_to_flask({"current_viewers": current_viewers})
            
        # 尝试获取总观众人数
        if hasattr(event, 'total_user'):
            total_viewers = event.total_user
            send_data_to_flask({"total_viewers": total_viewers})
            
    except Exception as e:
        print(f"处理RoomUserSeqEvent时出错: {e}")


# 添加评论事件处理
@client.on(CommentEvent)
async def on_comment(event: CommentEvent):
    """处理评论事件"""
    try:
        # 构建评论数据 - 根据 TikTokLive 最新结构访问 user 属性
        comment_data = {
            "user_id": event.user.unique_id,  # 使用 unique_id 替代 user_id
            "user_name": event.user.nickname,
            "user_avatar": "",  # 可能需要额外步骤获取头像
            "comment": event.comment,
            "timestamp": datetime.now().isoformat()
        }
        
        # 尝试获取用户头像 (如果可用)
        if hasattr(event.user, "avatar_thumb") and event.user.avatar_thumb:
            try:
                if isinstance(event.user.avatar_thumb, str):
                    comment_data["user_avatar"] = event.user.avatar_thumb
                elif isinstance(event.user.avatar_thumb, dict) and "url_list" in event.user.avatar_thumb:
                    if event.user.avatar_thumb["url_list"] and len(event.user.avatar_thumb["url_list"]) > 0:
                        comment_data["user_avatar"] = event.user.avatar_thumb["url_list"][0]
            except:
                pass
        
        # 打印评论信息
        print(f"收到评论: {comment_data['user_name']}: {comment_data['comment']}")
        
        # 发送评论数据到Flask
        send_data_to_flask({
            "add_comment": comment_data
        })
    except Exception as e:
        print(f"处理评论事件时出错: {e}")


# 添加礼物事件处理
@client.on(GiftEvent)
async def on_gift(event: GiftEvent):
    """处理礼物事件"""
    try:
        # 构建礼物数据，适配 TikTokLive 最新结构
        gift_data = {
            "user_id": event.user.unique_id,
            "user_name": event.user.nickname,
            "gift_name": event.gift.name,
            "gift_count": event.gift.count,
            "gift_value": getattr(event.gift, "diamond_count", 0),
            "timestamp": datetime.now().isoformat()
        }
        
        # 打印礼物信息
        if event.gift.streakable and not event.streaking:
            print(f"收到礼物: {gift_data['user_name']} 赠送了 {event.repeat_count}个{gift_data['gift_name']}")
            gift_data["gift_count"] = event.repeat_count  # 更新为连击最终数量
        else:
            print(f"收到礼物: {gift_data['user_name']} 赠送了 {gift_data['gift_count']}个{gift_data['gift_name']}")
        
        # 发送礼物数据到Flask
        send_data_to_flask({
            "add_gift": gift_data
        })
    except Exception as e:
        print(f"处理礼物事件时出错: {e}")


# 添加点赞事件处理
@client.on(LikeEvent)
async def on_like(event: LikeEvent):
    """处理点赞事件"""
    try:
        # 获取点赞数量
        like_count = event.count
        
        # 打印点赞信息
        nickname = event.user.nickname if hasattr(event.user, "nickname") else event.user.unique_id
        print(f"{nickname} 点了 {like_count} 个赞")
        
        # 发送点赞数据到Flask
        send_data_to_flask({
            "total_likes_increment": like_count
        })
    except Exception as e:
        print(f"处理点赞事件时出错: {e}")


# 添加分享事件处理
@client.on(ShareEvent)
async def on_share(event: ShareEvent):
    """处理分享事件"""
    try:
        # 打印分享信息
        nickname = event.user.nickname if hasattr(event.user, "nickname") else event.user.unique_id
        print(f"{nickname} 分享了直播")
        
        # 发送分享数据到Flask
        send_data_to_flask({
            "total_shares_increment": 1
        })
    except Exception as e:
        print(f"处理分享事件时出错: {e}")


# 添加关注事件处理
@client.on(FollowEvent)
async def on_follow(event: FollowEvent):
    """处理关注事件"""
    try:
        # 打印关注信息
        nickname = event.user.nickname if hasattr(event.user, "nickname") else event.user.unique_id
        print(f"{nickname} 关注了主播")
        
        # 发送关注数据到Flask
        send_data_to_flask({
            "total_follows_increment": 1
        })
    except Exception as e:
        print(f"处理关注事件时出错: {e}")


# 更新定时更新数据的函数
async def periodic_room_info_update():
    """定期更新房间信息和观众数量"""
    global is_connected
    
    while True:
        try:
            if is_connected:
                # 如果连接中，尝试更新房间信息
                await fetch_room_info()
        except Exception as e:
            print(f"定期更新数据时出错: {e}")
            
        # 30秒更新一次
        await asyncio.sleep(30)


async def check_loop():
    """检查主播是否在直播的循环"""
    global is_connected
    
    while True:
        try:
            if not is_connected:
                # 检查是否在直播
                try:
                    is_streaming = await client.is_live()
                    
                    if is_streaming:
                        print(f"检测到 @{client.unique_id} 正在直播，正在连接...")
                        try:
                            await client.connect()
                        except Exception as e:
                            print(f"连接失败: {e}，10秒后重试")
                            await asyncio.sleep(10)
                    else:
                        print(f"@{client.unique_id} 不在直播中，30秒后重新检查")
                        await asyncio.sleep(30)
                except Exception as e:
                    print(f"检查直播状态失败: {e}，20秒后重试")
                    await asyncio.sleep(20)
            else:
                # 定期发送心跳，确保连接状态正常
                send_data_to_flask({
                    "is_live": True,
                    "current_viewers": client.viewer_count if hasattr(client, "viewer_count") else 0
                })
                await asyncio.sleep(5)
        except Exception as e:
            is_connected = False
            print(f"检查循环出错: {e}，60秒后重试")
            await asyncio.sleep(60)


async def main():
    """主异步函数"""
    global is_connected
    
    # 最大重试次数
    max_retries = 3
    retry_count = 0
    
    while True:  # 永远循环，除非被KeyboardInterrupt终止
        try:
            # 先尝试连接
            try:
                if await client.is_live():
                    await client.connect()
                    retry_count = 0  # 重置重试计数
            except Exception as e:
                retry_count += 1
                print(f"连接尝试 {retry_count}/{max_retries} 失败: {e}")
                
                if retry_count >= max_retries:
                    print(f"达到最大重试次数，进入常规检查循环")
                    retry_count = 0
                else:
                    # 等待一段时间后继续尝试
                    await asyncio.sleep(10)
                    continue
            
            # 启动定期更新任务
            update_task = asyncio.create_task(periodic_room_info_update())
            
            # 启动检查循环
            await check_loop()
            
        except KeyboardInterrupt:
            print("程序被用户终止")
            break
        except Exception as e:
            print(f"主循环出错: {e}，10秒后重新启动")
            # 确保连接已断开
            if is_connected:
                try:
                    await client.disconnect()
                except:
                    pass
            is_connected = False
            # 等待后重启循环
            await asyncio.sleep(10)
    
    # 最终清理
    if is_connected:
        try:
            await client.disconnect()
        except:
            pass
    print("程序已退出")


def run_monitor():
    """运行监控的入口函数"""
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("程序被用户终止")
    except Exception as e:
        print(f"运行监控时出错: {e}")


if __name__ == '__main__':
    run_monitor() 