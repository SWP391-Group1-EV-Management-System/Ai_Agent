"""
🚀 Production-Ready LangGraph AI Worker with RabbitMQ & Redis
Enterprise-grade distributed agent system for LangGraph 1.0.0

Author: Your Team
Version: 2.0.0-production (LangGraph 1.0.0)
"""

import asyncio
import json
import selectors
import uuid
import os
import logging
import sys
import signal
import httpx
from sse_starlette.sse import EventSourceResponse
from typing import Dict, Any, List, Optional, Annotated
from datetime import datetime
from hashlib import md5
from contextlib import suppress, asynccontextmanager
from datetime import datetime, timezone
# Message Broker & Cache
import aio_pika
from aio_pika import DeliveryMode, ExchangeType
import redis.asyncio as aioredis

# Web Framework
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Environment & Config
from dotenv import load_dotenv

# Database
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

# LangGraph & LangChain - UPDATED FOR 1.0.0
from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.prebuilt import ToolNode, tools_condition

# Conditional import for checkpointer
try:
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False
    print("⚠️  PostgreSQL checkpointer not available. Running without persistence.")
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, BaseMessage, ToolMessage
from typing_extensions import TypedDict

# Tools
from tools.register_tools import TOOLS

# ==================== CONFIGURATION ====================
load_dotenv()
#lấy các biến môi trường từ file .env
class Config:
    """Centralized configuration with validation"""
    # Infrastructure
    RABBITMQ_URL: str = os.getenv("RABBITMQ_URL")
    REDIS_URL: str = os.getenv("REDIS_URL")
    DATABASE_URL: str = os.getenv("DATABASE_URL")
    CHECKPOINTER_DB_DSN: str = os.getenv("CHECKPOINTER_DB_DSN") 
    # Sharding
    SHARD_COUNT: int = int(os.getenv("SHARD_COUNT", "4"))
    AI_QUEUE_PREFIX: str = os.getenv("AI_QUEUE_PREFIX", "ai_jobs_")
    DLX_NAME: str = os.getenv("DLX", "ai_dlx")
    DLQ_NAME: str = "ai_jobs.dlq"
    SPRING_CHECK_TOKEN_URL : str = os.getenv("SPRING_CHECK_TOKEN_URL")
    
    # Performance
    LLM_CONCURRENCY: int = int(os.getenv("LLM_CONCURRENCY", "5"))
    PREFETCH_COUNT: int = int(os.getenv("PREFETCH_COUNT", "10"))
    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "20"))
    DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "10"))
    
    # Retry & Timeout
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "5"))
    LOCK_TTL_MS: int = int(os.getenv("LOCK_TTL_MS", "30000"))
    JOB_TIMEOUT_SEC: int = int(os.getenv("JOB_TIMEOUT_SEC", "300"))
    
    # LLM
    USE_DUMMY: bool = os.getenv("USE_DUMMY", "false").lower() == "true"
    GOOGLE_API_KEY: str = os.getenv("GOOGLE_API_KEY")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gemini-2.0-flash-exp")
    LLM_TEMPERATURE: float = float(os.getenv("LLM_TEMPERATURE", "0.3"))
    LLM_MAX_TOKENS: int = int(os.getenv("LLM_MAX_TOKENS", "2048"))
    
    # Memory & History
    MAX_HISTORY_MESSAGES: int = int(os.getenv("MAX_HISTORY_MESSAGES", "20"))
    CHECKPOINT_TTL_SEC: int = int(os.getenv("CHECKPOINT_TTL_SEC", "3600"))
    
    # System Prompt từ file txt
    with open("system_prompt.txt", "r", encoding="utf-8") as f:
        SYSTEM_PROMPT: str = f.read()
    
    @classmethod
    def validate(cls):
        """Validate critical configuration"""
        required = ["RABBITMQ_URL", "REDIS_URL", "DATABASE_URL"]
        missing = [k for k in required if not getattr(cls, k)]
        # nếu các biến môi trường quan trọng bị thiếu, ném lỗi
        if missing:
            raise ValueError(f"Missing required config: {', '.join(missing)}")
        
        if not cls.USE_DUMMY and not cls.GOOGLE_API_KEY:
            raise ValueError("GOOGLE_API_KEY required when USE_DUMMY=false")

config = Config() #tạo instance config
config.validate() #kiểm tra cấu hình quan trọng trước khi chạy

# ==================== LOGGING ====================
# cấu hình cho hệ thống ghi log
logging.basicConfig( 
    level=logging.INFO, #chỉ lấy log từ mức INFO trở lên bỏ qua DEBUG (WARNING, ERROR, CRITICAL)
    # giống java LoggerFactory 
    format="%(asctime)s [%(levelname)s] [%(name)s:%(lineno)d] %(message)s",
    #asctime là thời gian | levelname là mức độ log (INFO, ...) | name là tên logger | lineno là số dòng trong code | message là nội dung log
    datefmt="%Y-%m-%d %H:%M:%S", #định dạng lại thời gian
    # 2025-10-23 17:33:05 ví dụ 
    handlers=[
        logging.FileHandler("ai_worker.log", encoding="utf-8"), #ghi vào file
        logging.StreamHandler(sys.stdout) #ghi ra console
    ]
)
logger = logging.getLogger(__name__) #khởi tạo logger trong file hiện tại 

# ==================== DATABASE ====================
engine = create_async_engine( # sử dụng kết nối bất đồng bộ ( không gây chặn chương trình khi đang truy xuất)
    config.DATABASE_URL, # link kết nối db
    pool_size=config.DB_POOL_SIZE, #số kết nối tối đa trong pool 
    # (giống như cổng điện thoại công cộng, người này sử dụng trã lại đến người khác sử dụng, có 20 cái đt)
    max_overflow=config.DB_MAX_OVERFLOW, #phòng trường hợp quá tải
    # (nếu 20 cái đt đều bận, có thể tạm thời mượn thêm 10 cái nữa để phục vụ khách hàng)
    pool_pre_ping=True, #kiểm tra kết nối còn sống trước khi sử dụng
    echo=False #không in câu lệnh SQL ra log ( thừa), đổi true nếu muốn xem chi tiết để debug
)
AsyncSessionLocal = async_sessionmaker( #EntityManagerFactory trong java
    # tạo session giao tiếp bất đồng bộ với db
    engine, # cấu hình ở trên 
    class_=AsyncSession, #bất đồng bộ
    expire_on_commit=False # tránh xóa dữ liệu trong session, để truy xuất lại nhanh hơn
)

# ==================== LLM ====================
class DummyLLM: 
    """Dummy LLM dùng để test để không tốn token"""
    async def ainvoke(self, messages):
        await asyncio.sleep(1) # tăng độ trễ
        last_msg = messages[-1] #lấy tin nhắn cuối cùng vì trong đối tượng messages sẽ có nhiều fiedls khác nhau, fiedl cuối cùng là humanMessage 
        content = last_msg.content if hasattr(last_msg, 'content') else str(last_msg) # in nội dung tin nhắn tức phần content, nếu không thì in hêt ra tránh lỗi
        return AIMessage(content=f"[DUMMY] Response to: {content[:100]}...") # trả về tin nhắn AIMessage với nội dung giả lập
    
    def bind_tools(self, tools): # không gắn tools cho dummy 
        return self

# Xác thực gọi dummy hoặc Google Gemini
if config.USE_DUMMY:
    llm = DummyLLM()
    logger.info("🧪 Using DUMMY LLM (no API calls)")
else:
    llm = ChatGoogleGenerativeAI(
        model=config.LLM_MODEL, #gọi model gemini cụ thể 
        temperature=config.LLM_TEMPERATURE, # độ sáng tạo của câu trã lời (để ở mức an toàn tránh bịa chuyện)
        google_api_key=config.GOOGLE_API_KEY, #API key
        max_tokens=config.LLM_MAX_TOKENS, # giới hạn số token trong câu trả lời
        top_p=0.95, #độ rộng của phân phối xác suất (giúp đa dạng câu trả lời)
    )
    logger.info(f"🤖 Using Google Gemini: {config.LLM_MODEL}")

# gắn tools cho LLM gemini
llm_with_tools = llm.bind_tools(TOOLS) if not config.USE_DUMMY else llm
#TOOLS đã được gắn sẵn và import 
# Semaphore for LLM concurrency control
llm_sem = asyncio.Semaphore(config.LLM_CONCURRENCY) # cho phép số lượng LLM đồng thời một lúc chạy là LLM_CONCURRENCY

# ==================== AGENT STATE (LANGGRAPH 1.0.0) ====================
from operator import add

class AgentState(TypedDict):
    """Agent state with job_id and redis"""
    messages: Annotated[List[BaseMessage], add]
    user_id: str
    thread_id: str
    jwt: str
    job_id: str # thêm job_id để trả thêm action cho UI

# ==================== AGENT NODES ====================
async def call_model(state: AgentState): #state ở đây là dict giống như Map trong java
    """Trái tim của agent - gọi LLM với messages đã có"""
    messages = state["messages"]
    user_id = state.get("user_id", "unknown")
    jwt = state.get("jwt", None)  # ✅ Lấy JWT từ state
    job_id = state.get("job_id", None)  # ✅ Lấy job_id từ state    
    # ✅ Thêm JWT vào system prompt để LLM biết
    system_content = config.SYSTEM_PROMPT
    if jwt:
        system_content += f"\n\n🔐 **Authentication Context:**\nUser JWT Token (use this for API calls): `{jwt}`"
    if job_id:
        system_content += f"\n\n🔑 **Job Context:**\nUser Job ID (use this for tracking): `{job_id}`"
    system_msg = SystemMessage(content=system_content)
    
    # Remove any existing system messages to avoid duplicates
    messages_without_system = [m for m in messages if not isinstance(m, SystemMessage)] 
    # xóa systemMessage cũ nếu có, m sẽ lưu những phần còn lại mà không phải systemMessage
    
    # ✅ Filter out empty messages that could cause Gemini errors
    valid_messages = [] # danh sách lưu trữ các message hợp lệ ( lọc các message rỗng, tránh lỗi cho gemini)
    for m in messages_without_system:
        if hasattr(m, 'content') and m.content:
            valid_messages.append(m) #(hasattr(m, 'content') hàm kiểm tra m có thuộc tính content không, và m.content kiểm tra content có rỗng không)
        elif hasattr(m, 'tool_calls') and m.tool_calls:
            valid_messages.append(m)
        else:
            logger.warning(f"⚠️ Skipping empty message: {type(m).__name__}")
    
    # Add system prompt at the start
    messages_with_system = [system_msg] + valid_messages
    
    # bắt lỗi nếu chỉ có mỗi systemMessage mà không có message nào khác 
    if len(messages_with_system) < 2:
        logger.error(f"❌ Not enough messages to send to LLM: {len(messages_with_system)}") # số phần tử trong messages_with_system
        return {"messages": [AIMessage(content="Xin lỗi anh/chị, em gặp lỗi khi xử lý yêu cầu ạ.")]}
    
    # Log
    logger.debug(f"🔍 Sending {len(messages_with_system)} messages to LLM")
    
    # Invoke LLM with tools
    try:
        async with llm_sem: #còn chỗ trống trong semaphore thì mới gọi LLM
            response = await llm_with_tools.ainvoke(messages_with_system) # gửi tất cả message đã có (bao gồm systemMessage và userMessage) đến LLM để lấy câu trả lời
        return {"messages": [response]} 
    except Exception as e:
        logger.error(f"❌ LLM invocation error: {e}")
        return {"messages": [AIMessage(content="Xin lỗi anh/chị, em gặp lỗi khi xử lý yêu cầu ạ.")]}

def should_continue(state: AgentState):
    """Kiểm tra xem agent có cần gọi tools hay dừng lại"""
    messages = state["messages"] # lấy tin nhắn 
    last_message = messages[-1] # lấy tin nhắn cuối cùng mà AI vừa tạo ra 
    
    # kiểm tra xem nó có muốn gọi tools hay không, không thì cho nó end 
    if hasattr(last_message, "tool_calls") and last_message.tool_calls:
        return "tools"
    # end state ở đây
    return END

# ==================== LANGGRAPH AGENT (1.0.0) ====================
async def create_agent_executor():
    """
    Create LangGraph agent with PostgreSQL checkpointer
    Updated for LangGraph 1.0.0
    """
    # PostgreSQL checkpointer
    checkpointer = None
    checkpointer_cm = None  # Lưu context manager để cleanup sau
    
    if POSTGRES_AVAILABLE and config.CHECKPOINTER_DB_DSN: #khởi tạo checkpointer tăng tính bền vững
        try:
            # ✅ FIX: from_conn_string() trả về context manager, cần await __aenter__()
            checkpointer_cm = AsyncPostgresSaver.from_conn_string(config.CHECKPOINTER_DB_DSN)
            checkpointer = await checkpointer_cm.__aenter__()
            # Giờ checkpointer mới là AsyncPostgresSaver thực sự
            await checkpointer.setup() # sẵn sàng sử dụng 
            logger.info("✅ PostgreSQL checkpointer enabled")
        except Exception as e:
            logger.warning(f"⚠️  Failed to setup PostgreSQL checkpointer: {e}")
            logger.info("📝 Running without persistence")
            checkpointer = None
            checkpointer_cm = None
    else:
        logger.info("📝 Running without checkpointer (no persistence)")
    
    # Tạo tool node 
    tool_node = ToolNode(TOOLS) 
    
    # ✅ CRITICAL FIX: Wrap tool node to maintain conversation flow
    async def tool_node_with_context(state: AgentState):
        """
        Tool node wrapper tự động inject user_id và jwt vào tool calls
        """
        print("\n" + "=" * 80)
        print("🔧 TOOL NODE WITH CONTEXT CALLED")
        
        messages = state['messages']
        user_id = state.get('user_id')
        jwt = state.get('jwt')
        job_id = state.get('job_id')
        
        print(f"📊 Context available:")
        print(f"   - user_id: {user_id}")
        print(f"   - jwt: {jwt[:20] if jwt else 'None'}...")
        print(f"   - job_id: {job_id}")
        
        # Lấy last message (AIMessage with tool_calls)
        last_message = messages[-1]
        
        if not hasattr(last_message, 'tool_calls') or not last_message.tool_calls:
            print("⚠️ No tool calls found!")
            return {"messages": []}
        
        tool_messages = []
        
        for tool_call in last_message.tool_calls:
            tool_name = tool_call['name']
            tool_args = tool_call['args'].copy()  # Copy để không modify original
            tool_id = tool_call['id']
            
            print(f"\n🛠️ Processing tool: {tool_name}")
            print(f"   Original args: {tool_args}")
            
            # ✅ INJECT CONTEXT vào tool args
            if tool_name == "create_booking":
                # Override user parameter với user_id thật từ state
                if user_id:
                    tool_args['user'] = user_id
                    print(f"   ✅ Injected user_id: {user_id}")
                #if job_id:
                #    tool_args['job_id'] = job_id 
                else:
                    print(f"   ⚠️ No user_id in state!")
            
            print(f"   Final args: {tool_args}")
            
            # Execute tool với args đã inject
            try:
                from tools.register_tools import TOOLS
                
                # Find tool by name
                tool_func = None
                for t in TOOLS:
                    if t.name == tool_name:
                        tool_func = t
                        break
                
                if not tool_func:
                    result = json.dumps({
                        "error": f"Tool {tool_name} not found"
                    }, ensure_ascii=False)
                else:
                    # Call tool với args đã inject context
                    result = await tool_func.ainvoke(tool_args)
                
                print(f"   ✅ Tool result: {str(result)[:100]}...")
                
            except Exception as e:
                print(f"   ❌ Tool error: {e}")
                result = json.dumps({
                    "error": str(e)
                }, ensure_ascii=False)
            
            # Create ToolMessage
            tool_messages.append(
                ToolMessage(
                    content=str(result),
                    tool_call_id=tool_id,
                    name=tool_name
                )
            )
        
        print(f"\n🔗 Returning {len(tool_messages)} tool messages")
        print("=" * 80 + "\n")
        
        return {"messages": tool_messages}
    
    workflow = StateGraph(AgentState) # khai báo workflow với state đã fix
    #StateGraph đảm bảo luồng công việc của agent được quản lý đúng cách
    # Add nodes
    workflow.add_node("agent", call_model) #node này gọi LLM trã về câu trả lời và toolCalls nếu có
    workflow.add_node("tools", tool_node_with_context) #node này gọi tools nếu LLM yêu cầu
  
    # bắt đầu luồng công việc
    workflow.add_edge(START, "agent") 
    # kiểm tra có toolMessage không để quyết định chạy tiếp hay dừng
    workflow.add_conditional_edges(
        "agent",
        should_continue, # check có toolMessage không
        {
            "tools": "tools",
            END: END # trã end thì dừng vì lúc này agent đã xong việc
        }
    )
    workflow.add_edge("tools", "agent") # có thể gọi tools tiếp và add_conditional_edges lại chạy để quyết định
    # nếu cần tools thì cần đi tiếp đến agent để LLM trã kết quả theo ngôn ngữ con người 
    
    # Compile
    if checkpointer:
        app = workflow.compile(checkpointer=checkpointer) # thực hiện gói workflow thành executor (app)
        logger.info(f"✅ Agent created with {len(TOOLS)} tools + PostgreSQL persistence")
    else:
        app = workflow.compile()
        logger.info(f"✅ Agent created with {len(TOOLS)} tools (no persistence)")
    #app là executor để gọi agent và nó được python gói lại thành một Object
    return app, checkpointer, checkpointer_cm  # Trả về cả context manager

# Tạo Global instance
agent_executor = None
checkpointer = None # biến này thao tác chính với PostGre (thêm xóa sửa)
checkpointer_cm = None  # Biến toàn cục cho context manager, dùng để mở đóng kết nối nên cần được gán là global (fix do hệ đều hành window  )
# được gán ở start_consumer()

# ==================== UTILITIES ====================
def user_shard_queue(user_id: str) -> str:
    """Hàm phân tán người dùng vào một queue cố định"""
    # giúp lưu lại context trên mỗi lần hội thoại liên tục, tránh query lại
    h = int(md5(user_id.encode()).hexdigest()[:8], 16) # hash user_id lấy 8 ký tự đầu và chuyển thành số nguyên
    return f"{config.AI_QUEUE_PREFIX}{h % config.SHARD_COUNT}"
    # nên cải tiến sử dụng distributed state store lưu cache lên redis (RAM) và Load Balancer để chọn worker ít tải nhất
    # hiện tại chưa có cơ chế mỗi worker xử lý một queue cố định, mà các worker đang tranh nhau lấy message nếu rảnh  
async def acquire_lock(redis: aioredis.Redis, key: str, ttl_ms: int = None) -> bool:
    """Lock request lại nếu đã có worker xử lý nó (redis distributed lock)"""
    ttl = ttl_ms or config.LOCK_TTL_MS # khóa 30s cho worker xử lý (mặc định)
    return await redis.set(key, "1", nx=True, px=ttl) #key là đặt tên cho khóa, và gán đại value cho nó là 1 
#nx = NotExists: chỉ đặt khóa nếu nó chưa tồn tại , px thời gian 
# trả về true nếu đặt khóa thành công, false nếu đã có có worker khác lấy rồi 
# nếu hết thời gian redis tự xóa và chờ worker khác lấy 
async def release_lock(redis: aioredis.Redis, key: str):
    """Xóa khóa sau khi xử lý xong"""
    await redis.delete(key)
# aioredis là thư viện chạy redis bất đồng bộ
async def is_job_completed(redis: aioredis.Redis, job_id: str) -> bool:
    """Tránh xử lý lặp lại công việc đã hoàn thành"""
    return await redis.sismember("jobs:completed", job_id)

async def mark_job_completed(redis: aioredis.Redis, job_id: str, ttl: int = 86400):
    """Đánh dấu công việc đã hoàn thành để đảm bảo tính idempotency"""
    await redis.sadd("jobs:completed", job_id) # thêm id job vào redis 
    await redis.expire("jobs:completed", ttl) # đặt timeout 1 ngày 

# ==================== DATABASE OPERATIONS ====================
async def load_conversation_history(user_id: str, limit: int = None) -> List[Dict[str, str]]: # giá trị trã về ( có hay không cũng đc vì python là ngôn ngữ dynamically typed)
    """lấy lịch sử trò chuyện từ PostgreSQL ( có giới hạn số tin nhắn lấy)"""
    limit = limit or config.MAX_HISTORY_MESSAGES
    async with AsyncSessionLocal() as session: # tạo đối tượng session giao tiếp với db
        result = await session.execute(
            text("""
                SELECT role, content, created_at 
                FROM chat_messages 
                WHERE user_id = :user_id 
                ORDER BY created_at DESC 
                LIMIT :limit
            """),
            {"user_id": user_id, "limit": limit}
        ) # lấy DESC tin nhắn từ dưới lên trên rồi xuống kia mới reverse nó lại 
        rows = result.fetchall() # lấy tất cả các dòng kết quả
        return [
            {"role": row[0], "content": row[1], "created_at": str(row[2])} #row[0] là lấy index cột 0 trong row đó 
            for row in reversed(rows) # lặp qua từng row, đảo ngược thứ tự ( tức lặp dưới lên) để có tin nhắn từ cũ đến mới
        ]

async def save_message(user_id: str, role: str, content: str):
    """Lưu tin nhắn vào PostgreSQL"""
    async with AsyncSessionLocal() as session:
        await session.execute(
            text("""
                INSERT INTO chat_messages (user_id, role, content, created_at)
                VALUES (:user_id, :role, :content, :created_at)
            """),
            {
                "user_id": user_id,
                "role": role,
                "content": content,
                "created_at": datetime.now() 
            } # đã dùng place holder để tránh SQL injection (:user_id, :role, ...)
        )
        await session.commit()

async def clear_checkpoint(user_id: str):
    """
    Xóa checkpoint (trạng thái hội thoại) của người dùng khỏi Postgres checkpointer.
    Điều này giúp agent không reuse lại context cũ cho lần chat mới.
    """
    try:
        async with AsyncSessionLocal() as session:
            await session.execute(
                text("DELETE FROM checkpoints WHERE thread_id = :thread_id "),
                {"thread_id": f"thread_{user_id}"}
            )
            await session.execute(
                text("DELETE FROM checkpoint_blobs WHERE thread_id = :thread_id "),
                {"thread_id": f"thread_{user_id}"}

            )
            await session.execute(
                text("DELETE FROM checkpoint_writes WHERE thread_id = :thread_id "),
                {"thread_id": f"thread_{user_id}"}
            )       
            await session.commit()
        logger.info(f"✅ Cleared checkpoint for {user_id}")
    except Exception as e:
        logger.error(f"❌ Failed to clear checkpoint for {user_id}: {e}", exc_info=True)
async def get_user_jwt(user_email: str, redis):
    """Lấy JWT từ Redis"""
    jwt = await redis.get(f"jwt:{user_email}")
    if jwt:
        return jwt
    print(f"⚠️ No JWT found for user email: {user_email}")
    return None
# ==================== AGENT EXECUTION ====================
async def invoke_agent(user_id: str, user_input: str, job_id: str, redis: aioredis.Redis) -> str:
    """
    Gọi agent cùng với checkpointer, memory lịch sử chat, Phiên làm việc của 1 worker gồm nhiều node 
    (LangGraph 1.0.0)
    """
    try:
        # Tải lịch sử chat
        history = await load_conversation_history(user_id, 100)
        
        # chuyển sang định dạng mà agent hiểu được
        messages = []
        for msg in history:
            if msg["role"] == "user":
                messages.append(HumanMessage(content=msg["content"]))
            elif msg["role"] == "assistant":
                messages.append(AIMessage(content=msg["content"]))
        
        # thêm câu input của người dùng vào luôn 
        messages.append(HumanMessage(content=user_input))
        
        # Thread ID (giữ context hội thoại)
        thread_id = f"thread_{user_id}"
        
        # Configuration for agent
        config_dict = {
            "configurable": {
                "thread_id": thread_id
            }
        }
        jwt = await get_user_jwt(user_id, redis)  # Lấy JWT từ Redis
        # gọi agent 
        result = await asyncio.wait_for(
            agent_executor.ainvoke(
                {
                    "messages": messages,
                    "user_id": user_id,    
                    "thread_id": thread_id, # thread_id này để agent đọc lại context hội thoại
                    "jwt": jwt,
                    "job_id": job_id # truyền job_id để LLM biết
                },
                config=config_dict # cấu hình thread_id để cho các node trong agent dùng chung, giữ context xuyên suốt workflow, để tránh ghi đè thread_id nếu có nhiều người dùng cùng hội thoại một lúc
                # quản lý runtime, workflow là chính 
            ),
            timeout=config.JOB_TIMEOUT_SEC # giới hạn thời gian agent xử lý 
        )
        
        # Extract final response
        final_message = result["messages"][-1]
        response_text = final_message.content if hasattr(final_message, 'content') else str(final_message)
        
        logger.info(f"✅ Agent response for user {user_id}: {response_text[:100]}...")
        return response_text # trả kết quả
        
    except asyncio.TimeoutError: # bắt lỗi nếu agent chạy quá thời gian 
        logger.error(f"⏱️ Agent timeout for user {user_id}")
        return "Xin lỗi, yêu cầu của anh/chị mất quá nhiều thời gian xử lý. Vui lòng thử lại."
    except Exception as e:
        logger.error(f"❌ Agent error for user {user_id}: {e}", exc_info=True) # gửi đầy đủ thông tin log 
        return "Xin lỗi, đã xảy ra lỗi khi xử lý yêu cầu của anh/chị."

# ==================== MESSAGE HANDLER ====================
async def handle_message(
    redis: aioredis.Redis,
    payload: Dict[str, Any], # đoạn mã json được giải mã thành dict (hash map in java)
    message: aio_pika.IncomingMessage # giao tiếp trung gian qua aio_pika, không thể gọi trực tiếp đến RabbitMQ vì không có thư viện 
):
    """Bộ xử lý chính cho mỗi tin nhắn từ RabbitMQ, đảm bảo idempotency và retry logic, DLQ"""
    job_id = payload["job_id"]
    user_id = payload["user_id"]
    user_message = payload["text"]
    
    # 1. Kiểm tra job đã hoàn thành chưa 
    if await is_job_completed(redis, job_id):
        logger.info(f"⏭️ Job {job_id} already completed (idempotent)")
        await message.ack() # xóa khỏi queue
        return
    
    # 2. Kiểm tra nếu có worker khác đã lấy
    lock_key = f"lock:job:{job_id}"
    if not await acquire_lock(redis, lock_key, config.LOCK_TTL_MS):
        logger.info(f"🔒 Job {job_id} locked by another worker")
        await message.ack() 
        return
    
    try:
        # Kiểm tra lại
        if await is_job_completed(redis, job_id):
            logger.info(f"⏭️ Job {job_id} completed during lock acquisition")
            await message.ack()
            return
        
        logger.info(f"🔄 Processing job {job_id} for user {user_id}")
        
        # Gọi agent
        reply = await invoke_agent(user_id, user_message, job_id, redis)

        # Lưu vào cơ sở dữ liệu
        await save_message(user_id, "user", user_message)
        await save_message(user_id, "assistant", reply)

        # Đánh dấu là đã hoàn thành
        await mark_job_completed(redis, job_id)
        # ACK message
        await message.ack()
        logger.info(f"✅ Job {job_id} completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error processing job {job_id}: {e}", exc_info=True)
        # khi có lỗi xảy ra thử lại 
        # Retry logic
        headers = dict(message.headers) if message.headers else {} #lấy header từ rabbitmq message, để lưu số lần thử lại 
        retries = int(headers.get("x-retries", 0)) # lấy nếu có hoặc gán bằng 0 
        
        if retries < config.MAX_RETRIES: # chỉ thử lại số lần có hạn 
            headers["x-retries"] = retries + 1
            headers["x-error"] = str(e)[:200]
            await message.nack(requeue=True)
            logger.warning(f"⚠️ Job {job_id} requeued (retry {retries + 1}/{config.MAX_RETRIES})")
        else:
            # chuyển vào Dead Letter Queue nếu vượt quá số lần thử, nơi lưu tin nhắn bị lỗi 
            dlx_msg = aio_pika.Message(
                body=message.body,
                headers={**headers, "x-final-error": str(e)[:500]},
                delivery_mode=DeliveryMode.PERSISTENT # Persistent lưu trên ổ đĩa bền vững, Transient lưu trong RAM  
            )
            # gửi đến channel .default_exchange là nơi phân phối tin nhắn của rabbitmq .publish chọn gửi, tin nhắn sẽ chạy đến key được định nghĩa 
            await message.channel.default_exchange.publish(
                dlx_msg,
                routing_key=config.DLQ_NAME
            )
            await message.ack() # bất lực ack nó ra 
            logger.error(f"☠️ Job {job_id} sent to DLQ after {config.MAX_RETRIES} retries")
    finally:
        await release_lock(redis, lock_key) # giải phóng khóa trong redis dù thành công hay thất bại
        await clear_checkpoint(user_id) # xóa checkpoint để tránh reuse context cũ
# ==================== CONSUMER ====================
async def consume_shard(
    redis: aioredis.Redis,
    queue: aio_pika.Queue,
    shard_id: int,
    stop_event: asyncio.Event
):
    """Đăng ký consumer cho mỗi shard (queue), và gọi handle_message xử lý tin nhắn"""
    logger.info(f"🚀 Worker shard-{shard_id} starting on queue '{queue.name}'")
    
    async def on_message(msg: aio_pika.IncomingMessage): # truyền vào đối tượng msg từ rabbitmq
        async with msg.process(ignore_processed=True): # tự động ack sau khi xử lý xong, ignore_processed tránh lỗi nếu đã ack rồi
            try:
                payload = json.loads(msg.body.decode()) # giải mã json thành dict
                logger.debug(f"[shard-{shard_id}] 📩 Received: {payload}")
                await handle_message(redis, payload, msg) # gửi cho agent xử lý
            except json.JSONDecodeError as e:
                logger.error(f"[shard-{shard_id}] ❌ Invalid JSON: {e}")
                await msg.ack()
            except Exception as e:
                logger.error(f"[shard-{shard_id}] ❌ Handler error: {e}", exc_info=True)
                raise # ném lỗi ra hàm msg.process để xử lý retry và DLQ
    
    consumer_tag = await queue.consume(on_message, no_ack=False) # đăng ký tự động gọi hàm on_message khi có tin nhắn mới từ queue, no_ack = false để worker tự ack sau khi xử lý xong
    # on_message là callback function của thư viện aio-pika, thư viện sẽ tự truyền tham số 
    #no_ack=False chờ (msg.process(ignore_processed=True) ack, hoặc hàm handle_message ack) để true rabbitMQ giao rồi xóa luôn khỏi queue
    logger.info(f"✅ Shard-{shard_id} consumer registered (tag: {consumer_tag})")
    
    # dừng khi có tín hiệu
    try:
        await stop_event.wait()
        logger.info(f"[shard-{shard_id}] 🛑 Stop signal received")
    except asyncio.CancelledError:
        logger.info(f"[shard-{shard_id}] 🛑 Task cancelled")
        raise
    finally:
        try:
            await queue.cancel(consumer_tag)
            logger.info(f"[shard-{shard_id}] ✅ Consumer cancelled")
        except Exception as e:
            logger.warning(f"[shard-{shard_id}] ⚠️ Error cancelling consumer: {e}")

# ==================== INITIALIZATION ====================
async def init_infrastructure():
    """Khởi tạo kết nối RabbitMQ, Redis"""
    try:
        logger.info("🔌 Initializing infrastructure...")
        
        # RabbitMQ
        conn = await aio_pika.connect_robust( # aio_pika.connect_robust() kết nối lại tự động nếu mất kết nối
            config.RABBITMQ_URL,
            timeout=10,
            client_properties={"connection_name": "ai_worker"}
        )
        # tạo channel nhỏ trong kết nối lớn conn, đủ sử dụng và tiết kiệm tài nguyên 
        channel = await conn.channel() #tạo chanel trong conn, đường ống nhỏ 
        await channel.set_qos(prefetch_count=config.PREFETCH_COUNT) # thiết lập số lượng message tối đa mà worker có thể lấy cùng lúc, giúp phân tán message đều cho các worker
        # mỗi worker sẽ được rabbitMQ rót cho 10 message nhưng khi xử lý thì mới lock message đó ( tức là nếu worker khác xong cũng có thể lấy message của worker kia xử lý tiếp)
        
        # Declare DLX and DLQ
        dlx = await channel.declare_exchange( # bưu điện nhận tin nhắn lỗi
            config.DLX_NAME, # tên DLX
            ExchangeType.DIRECT, # kiểu direct để routing key khớp mới gửi đến queue
            durable=True # tồn tại lâu dài 
        )
        dlq = await channel.declare_queue(config.DLQ_NAME, durable=True) # tạo nơi nhận tin nhắn chết (dead letter queue)
        await dlq.bind(dlx, routing_key=config.DLQ_NAME) # gán địa chỉ cho bưu điện DLX giao tin nhắn chết đến DLQ
        
        # Redis
        redis = await aioredis.from_url(
            config.REDIS_URL,
            decode_responses=True,
            max_connections=50
        )
        await redis.ping()

        logger.info("✅ Infrastructure initialized: RabbitMQ, Redis")
        return conn, channel, redis
        
    except Exception as e:
        logger.error(f"❌ Infrastructure init failed: {e}", exc_info=True)
        raise

# ==================== MAIN CONSUMER ====================
async def start_consumer():
    """Main consumer loop with graceful shutdown"""
    global agent_executor, checkpointer, checkpointer_cm  
    
    # chứa workflow , thao tác với db, cổng kết nối
    agent_executor, checkpointer, checkpointer_cm = await create_agent_executor() 
    
    # Initialize các kết nối 
    conn, channel, redis = await init_infrastructure()
    
    # Stop event
    stop_event = asyncio.Event()
    
    def signal_handler(sig, frame):
        logger.info(f"🛑 Received signal {sig}, initiating shutdown...")
        stop_event.set()
    # sig mã tín hiệu SIGINT = 2, SIGTERM = 15| bắt buộc phải truyền đủ tham số sig, frame tương thích với thư viện signal của python
    signal.signal(signal.SIGINT, signal_handler) # bắt tín hiệu ctrl+c để dừng
    signal.signal(signal.SIGTERM, signal_handler) # bắt tín hiệu dừng từ hệ điều hành ( docker, hoặc hệ điều hành gọi kill)
    #signal_handler là callback function
    # Tạo worker cho mỗi shard (consumers)
    tasks = [] # giỏ đựng công việc 
    for shard in range(config.SHARD_COUNT): # tạo ra 8 task cho mỗi worker (worker là một lần python main.py, có thể tạo nhiều worker bằng docker) 
        queue_name = f"{config.AI_QUEUE_PREFIX}{shard}"
        queue = await channel.declare_queue( # tạo 8 queue như đã định nghĩa trong config nếu chưa có 
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": config.DLX_NAME,
                "x-dead-letter-routing-key": config.DLQ_NAME
            }
        )
        task = asyncio.create_task( # tạo 8 task chạy song song( trên 1 worker )
            consume_shard(redis, queue, shard, stop_event) # mỗi task lắng nghe một queue cố định
        )
        tasks.append(task)
    
    logger.info(f"🎯 Started {len(tasks)} shard consumers")
    
    try:
        await asyncio.gather(*tasks, return_exceptions=True) #return_exceptions=True nếu 1 task bị lỗi thì các task khác vẫn chạy tiếp
        # công dụng asyncio.gather() chạy nhiều task cùng lúc, và giữ nguyên chương trình, chỉ kết thúc và end task khi có tín hiệu dừng
    except asyncio.CancelledError:
        logger.info("X Consumer tasks cancelled")
    finally:
        logger.info("🧹 Cleaning up connections...")
        await channel.close()
        await conn.close()
        await redis.aclose()
        # Đóng checkpointer đúng cách qua context manager
        if checkpointer_cm:
            try:
                await checkpointer_cm.__aexit__(None, None, None)
                logger.info("✅ Checkpointer closed")
            except Exception as e:
                logger.warning(f"⚠️ Error closing checkpointer: {e}")
        logger.info("✅ Shutdown complete")
# ==================== FASTAPI APP ====================
@asynccontextmanager # định nghĩa hàm bất đồng bộ dùng làm context manager ( người giữ cửa kết nối)
async def lifespan(app: FastAPI):
    logger.info("🌟 FastAPI starting up...") # dòng này sẽ chạy khi khởi động vì nằm trước yield
    yield #app chạy ở đây và giữ chờ tin hiệu tắt mới chạy dòng dưới 
    logger.info("🌙 FastAPI shutting down...") 

app = FastAPI( # tạo kết nối FastAPI
    title="AI Message Gateway", # các thông tin này hiện lên UI swagger
    description="Production-grade LangGraph 1.0.0 agent system",
    version="2.0.0",
    lifespan=lifespan # gán context manager
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Cho phép TẤT CẢ website gọi 
    #"https://abdcjddahdaj.com",   ← Chỉ riêng domain này
    #"http://abdcjddahdaj.com"     ← Nếu cần cả HTTP

    allow_credentials=True, # Cho phép gửi cookie/token  
    allow_methods=["*"], # Cho phép TẤT CẢ method (GET, POST, PUT...)
    allow_headers=["*"], # Cho phép TẤT CẢ headers
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "2.0.0",
        "langgraph": "1.0.0"
    }

@app.post("/send_message")
async def send_message(request: Request): # nhận toàn bộ request từ client
    """Enqueue user message for processing"""
    try:
        cookie_jar = request.cookies
        jwt_token = cookie_jar.get("jwt")
        if not jwt_token:
            raise HTTPException(status_code=401, detail="authentication failed")       
        async with httpx.AsyncClient() as client:
            response = await client.post(
                config.SPRING_CHECK_TOKEN_URL,
                cookies={"jwt": jwt_token}
            )
        if response.status_code != 200:
            raise HTTPException(status_code=response.status_code, detail="authentication failed")
        else:
            userName = response.text.strip()

            data = await request.json()
            user_id = userName
            text = data.get("message")
            
            if not text:
                raise HTTPException(
                    status_code=400,
                    detail="Missing required fields: message"
                )
            
            conn, channel, redis = await init_infrastructure()
            
            try:
                # set JWT vào redis để worker sử dụng gọi API BE 
                await redis.setex(
                    f"jwt:{userName}", 
                    600, 
                    jwt_token
                )
                job_id = str(uuid.uuid4())
                payload = {
                    "job_id": job_id,
                    "user_id": user_id,
                    "text": text,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
                queue_name = user_shard_queue(user_id) # phân tán vào một queue cố định
                
                await channel.default_exchange.publish(
                    aio_pika.Message( # tạo object message để gửi
                        body=json.dumps(payload).encode(), #chuyển dict thành json rồi mã hóa thành bytes vì rabbitmq chỉ nhận đc dữ liệu bytes
                        delivery_mode=DeliveryMode.PERSISTENT, # lưu bền vững vào ổ đĩa
                        content_type="application/json"
                    ),
                    routing_key=queue_name # gửi đến queue đã phân shard ở trên
                )
                
                logger.info(f"📨 Enqueued job {job_id} for user {user_id} to {queue_name}")
                
                return JSONResponse({
                    "status": "ok",
                    "job_id": job_id,
                    "queue": queue_name,
                    "message": "Job enqueued successfully"
                })
                
            finally: # đóng kết nối, dù thành công hay lỗi, mỗi request sẽ đều tạo connect mới và đóng, tránh giữ kết nối lâu tốn tài nguyên
                await channel.close()
                await conn.close()
                await redis.aclose()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stream/{job_id}")
async def stream_result(job_id: str, request: Request):
    """
    SSE endpoint - Stream kết quả real-time từ AI worker
    Client sẽ nhận events liên tục cho đến khi job hoàn thành
    """
    try:
        # ✅ 1. Xác thực JWT
        cookie_jar = request.cookies
        jwt_token = cookie_jar.get("jwt")
        
        if not jwt_token:
            raise HTTPException(status_code=401, detail="Missing authentication token")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                config.SPRING_CHECK_TOKEN_URL,
                cookies={"jwt": jwt_token}
            )
        
        if response.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        
        user_email = response.text.strip()
        logger.info(f"📡 SSE stream started for job {job_id} (user: {user_email})")
        
        # ✅ 2. Generator function để stream events
        async def event_generator():
            redis = await aioredis.from_url(config.REDIS_URL, decode_responses=True)
            
            try:
                max_attempts = 120  # Tối đa 2 phút (120 giây)
                attempt = 0
                
                while attempt < max_attempts:
                    # ✅ Kiểm tra client còn kết nối không
                    if await request.is_disconnected():
                        logger.info(f"🔌 Client disconnected for job {job_id}")
                        break
                    
                    # ✅ Kiểm tra job đã hoàn thành chưa
                    is_completed = await redis.sismember("jobs:completed", job_id)
                    
                    if is_completed:
                        # Job đã xong - Lấy kết quả
                        result = await redis.get(f"job:{job_id}:result")
                        
                        if not result:
                            # Fallback: Lấy từ database nếu Redis không có
                            async with AsyncSessionLocal() as session:
                                db_result = await session.execute(
                                    text("""
                                        SELECT content, created_at
                                        FROM chat_messages 
                                        WHERE user_id = :user_id 
                                        AND role = 'assistant'
                                        ORDER BY created_at DESC 
                                        LIMIT 1
                                    """),
                                    {"user_id": user_email}
                                )
                                row = db_result.fetchone()
                                result = row[0] if row else "Lỗi: Không tìm thấy kết quả"
                        # Lấy action nếu có
                        action = "none"
                        actionId = "none"
                        rank = "none"
                        if(await redis.exists(job_id)):
                            action = await redis.hget(job_id, "action")
                            actionId = await redis.hget(job_id, "idAction")
                            rank = await redis.hget(job_id, "rank")
                            await redis.delete(job_id) 
                        # ✅ GỬI KẾT QUẢ CUỐI CÙNG
                        logger.info(f"✅ Sending final result for job {job_id}")
                        yield {
                            "event": "message",
                            "data": json.dumps({
                                "status": "completed",
                                "job_id": job_id,
                                "result": result,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "action": action,
                                "actionId": actionId,
                                "rank": rank
                            }, ensure_ascii=False)
                        }
                        
                        # Gửi event đóng kết nối
                        yield {
                            "event": "done",
                            "data": json.dumps({"status": "stream_ended"})
                        }
                        break
                    
                    # ✅ GỬI HEARTBEAT mỗi 3 giây để giữ kết nối
                    if attempt % 3 == 0:
                        logger.debug(f"💓 Heartbeat for job {job_id} (attempt {attempt})")
                        yield {
                            "event": "heartbeat",
                            "data": json.dumps({
                                "status": "processing",
                                "job_id": job_id,
                                "attempt": attempt,
                                "message": "Đang xử lý yêu cầu của bạn...",
                                "action": "processing"
                            })
                        }
                    
                    attempt += 1
                    await asyncio.sleep(1)  # Poll mỗi giây
                
                # ✅ TIMEOUT nếu quá lâu
                if attempt >= max_attempts:
                    logger.warning(f"⏱️ Job {job_id} timeout after {max_attempts}s")
                    yield {
                        "event": "error",
                        "data": json.dumps({
                            "status": "timeout",
                            "job_id": job_id,
                            "message": "Yêu cầu xử lý quá lâu. Vui lòng thử lại sau."
                        })
                    }
            
            except Exception as e:
                logger.error(f"❌ SSE generator error for job {job_id}: {e}", exc_info=True)
                yield {
                    "event": "error",
                    "data": json.dumps({
                        "status": "error",
                        "message": str(e)
                    })
                }
            
            finally:
                await redis.aclose()
                logger.info(f"🔚 SSE stream ended for job {job_id}")
        
        # ✅ Trả về SSE response
        return EventSourceResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no"  # Tắt buffering cho nginx
            }
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ SSE endpoint error for job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
@app.post("/update_location")
async def update_location(request: Request):
    """
    Cập nhật vị trí GPS của user (latitude, longitude)
    Frontend gọi API này mỗi khi user di chuyển >= 500m
    
    Request body:
    {
        "latitude": 10.762622,
        "longitude": 106.660172
    }
    """
    try:
        # ✅ 1. Xác thực JWT
        cookie_jar = request.cookies
        jwt_token = cookie_jar.get("jwt")
        
        if not jwt_token:
            raise HTTPException(status_code=401, detail="Missing authentication token")
        
        # Verify token với Spring backend
        async with httpx.AsyncClient() as client:
            response = await client.post(
                config.SPRING_CHECK_TOKEN_URL,
                cookies={"jwt": jwt_token}
            )
        
        if response.status_code != 200:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        
        user_email = response.text.strip()
        
        # ✅ 2. Parse và validate coordinates
        data = await request.json()
        latitude = data.get("latitude")
        longitude = data.get("longitude")
        print("✅✅✅Latitude:", latitude, "✅✅✅Longitude:", longitude)
        if latitude is None or longitude is None:
            raise HTTPException(
                status_code=400,
                detail="Missing required fields: latitude, longitude"
            )
        
        try:
            lat = float(latitude)
            lon = float(longitude)
            
            if not (-90 <= lat <= 90):
                raise ValueError("Latitude must be between -90 and 90")
            if not (-180 <= lon <= 180):
                raise ValueError("Longitude must be between -180 and 180")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"Invalid coordinates: {str(e)}")
        
        # ✅ 3. Lưu vào Redis
        redis = await aioredis.from_url(config.REDIS_URL, decode_responses=True)
        
        try:
            location_key = f"location:{user_email}"
            location_data = {
                "latitude": lat,
                "longitude": lon,
                "updated_at": datetime.now(timezone.utc).isoformat()
            }
            
            # Lưu với TTL 24 giờ
            await redis.setex(
                location_key,
                86400,  # 24 hours
                json.dumps(location_data)
            )
            
            logger.info(f"📍 Updated location for {user_email}: ({lat}, {lon})")
            
            return JSONResponse({
                "status": "success",
                "message": "Location updated successfully",
                "data": {
                    "latitude": lat,
                    "longitude": lon,
                    "timestamp": location_data["updated_at"]
                }
            })
        
        finally:
            await redis.aclose()
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Update location error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# ==================== DETECT CHARGING TYPE ENDPOINT ====================
@app.post("/api/detect-charging-type")
async def detect_charging_type_endpoint(request: Request):
    """
    API endpoint để phát hiện loại sạc xe điện dựa trên tên xe
    """
    try:
        # ✅ Parse request body
        data = await request.json()
        car_name = data.get("car_name", "").strip()
        
        if not car_name:
            raise HTTPException(
                status_code=400,
                detail="Missing required field: car_name"
            )
        
        if len(car_name) < 3:
            raise HTTPException(
                status_code=400,
                detail="Car name too short. Please provide full name (e.g., 'VinFast VF5')"
            )
        
        logger.info(f"🔍 Detecting charging type for: {car_name}")
        
        # ✅ Import function từ API_BE
        from tools.API_BE import detect_charging_type_by_car_name
        
        # ✅ Gọi function detect
        result = await detect_charging_type_by_car_name(car_name)
        
        logger.info(f"✅ Detected: {result['charging_type']} (confidence: {result['confidence']})")
        
        return JSONResponse({
            "status": "success",
            "data": result
        })
    
    except HTTPException:
        raise
    
    except Exception as e:
        logger.error(f"❌ Detect charging type error: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to detect charging type: {str(e)}"
        )
    
# ==================== ENTRY POINT ====================
if __name__ == "__main__": # chỉ chạy được khi run file này trực tiếp, không chạy được khi import
    try:
        logger.info("=" * 80)
        logger.info("🚀 Starting AI Worker (LangGraph 1.0.0)")
        logger.info(f"   Shards: {config.SHARD_COUNT}")
        logger.info(f"   LLM: {config.LLM_MODEL}")
        logger.info(f"   Concurrency: {config.LLM_CONCURRENCY}")
        logger.info("=" * 80)
        
        #asyncio.run(start_consumer())
        loop = asyncio.SelectorEventLoop(selectors.SelectSelector()) # ép window dùng SelectorEventLoop
        asyncio.set_event_loop(loop) # khởi tạo event loop
        loop.run_until_complete(start_consumer()) # chạy chính, chờ đến khi hàm start_consumer kết thúc 
        # hàm start_consumer sẽ chạy và gán task vào queue chờ tín hiệu dừng 
        #Windows mặc định dùng ProactorEventLoop, nhưng psycopg async không tương thích. ép buộc dùng SelectorEventLoop ( do không tương thích với psycopg thư viện PostGreSQL async)
    except KeyboardInterrupt:
        logger.info("🧹 Worker stopped by user")
    except Exception as e:
        logger.error(f"🔥 Worker crashed: {e}", exc_info=True)
        sys.exit(1)
    #thêm cơ chế xóa checkpointer khi đủ 1 ngày không sử dụng để tránh tốn dung lượng db


    # thực ra mỗi lần chạy (python main.py) đó mới là 1 worker, hợp lý khi sử dụng lock
    # 1 worker sẽ tạo ra 8 task, mỗi task lắng nghe 1 queue cố định, và nó sẽ xử lý request từ queue đó
    # mỗi task với cấu hình hiện tại đang được phép lấy 10 message cùng lúc, và xử lý đồng thời 5 request LLM cùng lúc ( nếu các task khác không sử dụng llm)
    # và 5 llm được khai báo đó, các task sẽ sử dụng chung với nhau 
    
    #Yield & Resume: đây là cơ chế giúp cho hàm bất đồng bộ hoạt động trên một thread ( luồng)