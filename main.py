
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
from typing import Dict, Any, List, Optional, Annotated
from datetime import datetime
from hashlib import md5
from contextlib import suppress, asynccontextmanager

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
    """Agent state for LangGraph 1.0.0 with message reducer"""
    messages: Annotated[List[BaseMessage], add]  # ✅ Use 'add' operator to APPEND messages
    user_id: str
    thread_id: str

# ==================== AGENT NODES ====================
async def call_model(state: AgentState): #state ở đây là dict giống như Map trong java
    """Trái tim của agent - gọi LLM với messages đã có"""
    messages = state["messages"] #lấy value của key messages trong state, tức là các tin nhắn mới được gửi đến agent
    
    # gán system prompt vào systemMessages (SystemMessage, HumanMessage, AIMessage)
    system_msg = SystemMessage(content=config.SYSTEM_PROMPT)
    
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
    if POSTGRES_AVAILABLE and config.DATABASE_URL: #khởi tạo checkpointer tăng tính bền vững
        try:
            checkpointer_cm = AsyncPostgresSaver.from_conn_string(config.DATABASE_URL) # tạo context manager cho checkpointer
            checkpointer = await checkpointer_cm.__aenter__() # khởi tạo checkpointer bất đồng bộ ( lúc này context manager đã vào trạng thái active) # __aenter__() là hàm để chuẩn bị tài nguyên, khởi động để sử dụng 
            await checkpointer.setup() # sẵn sàng sử dụng 
            logger.info("✅ PostgreSQL checkpointer enabled")
        except Exception as e:
            logger.warning(f"⚠️  Failed to setup PostgreSQL checkpointer: {e}")
            logger.info("📝 Running without persistence")
            checkpointer = None
    else:
        logger.info("📝 Running without checkpointer (no persistence)")
    
    # Tạo tool node 
    tool_node = ToolNode(TOOLS) 
    
    # ✅ CRITICAL FIX: Wrap tool node to maintain conversation flow
    async def tool_node_func(state: AgentState):
        print("\n" + "=" * 80)
        print("🔧 TOOL NODE CALLED")
        original_messages = state['messages'] # lấy toàn bộ messages hiện tại trước khi gọi tool
        print(f"📊 Messages before tools: {len(original_messages)}")
        
        # Debug: liệt kê tin nhắn đang có trước khi gọi tool
        for i, msg in enumerate(original_messages):
            msg_type = type(msg).__name__
            print(f"  [{i}] {msg_type}")
            ''' [0] SystemMessage
                [1] HumanMessage
                [2] AIMessage'''
        
        try:
            # gọi tools ( ở bước này LLM đã phân tích và đưa ra tools cần và tham số phục vụ cho tools rồi)
            result = await tool_node.ainvoke(state) # ainvoke này là hàm của thu viện langgraph
            tool_messages = result.get('messages', [])
            
            print(f"✅ Tool execution completed")
            print(f"📦 Tool returned {len(tool_messages)} new messages")
            
            # Debug tool results
            for msg in tool_messages:
                msg_type = type(msg).__name__ # in ra kiểu message "__name__" trã về tên lớp dạng string
                content_preview = str(msg.content)[:100] if hasattr(msg, 'content') else str(msg)[:100] # in ra nội dung của toolMessage nếu có hoặc cả msg, content là phần tin nhắn chính
                print(f"   → {msg_type}: {content_preview}...")
            
            
            print(f"🔗 Returning {len(tool_messages)} tool messages (LangGraph will merge)")
            print("=" * 80 + "\n")
            
            # # mô hình chạy bất đồng bộ này sẽ làm nhiều node (worker) chạy song song để xứ lý vấn đề
            # do đó node này cần trã đúng kết quả về cho node chính (agent) để nó tổng hợp, để tránh bị ghi đè chỉ gửi phần content toolMessage
            return {"messages": tool_messages}
            
        except Exception as e:
            print(f"❌ TOOL NODE ERROR: {e}")
            logger.error(f"Tool node error: {e}", exc_info=True)
            raise
    
    # Create workflow with Annotated reducer for messages
    from typing import Annotated
    from operator import add
    
    # ✅ CRITICAL FIX: Define state with proper reducer
    class FixedAgentState(TypedDict):
        """ Agent state với reducer để nối messages đúng cách """
        messages: Annotated[List[BaseMessage], add]  # Use 'add' to APPEND messages
        user_id: str
        thread_id: str
    
    workflow = StateGraph(FixedAgentState) # khai báo workflow với state đã fix
    #StateGraph đảm bảo luồng công việc của agent được quản lý đúng cách
    # Add nodes
    workflow.add_node("agent", call_model) #node này gọi LLM trã về câu trả lời và toolCalls nếu có
    workflow.add_node("tools", tool_node_func) #node này gọi tools nếu LLM yêu cầu
    
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
    return app, checkpointer 

# Tạo Global instance
agent_executor = None
checkpointer = None
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

# ==================== AGENT EXECUTION ====================
async def invoke_agent(user_id: str, user_input: str, redis: aioredis.Redis) -> str:
    """
    Invoke LangGraph agent with memory and checkpointing
    Updated for LangGraph 1.0.0
    """
    try:
        # Tải lịch sử chat
        history = await load_conversation_history(user_id)
        
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
        
        # Invoke agent
        result = await asyncio.wait_for(
            agent_executor.ainvoke(
                {
                    "messages": messages,
                    "user_id": user_id,
                    "thread_id": thread_id
                },
                config=config_dict
            ),
            timeout=config.JOB_TIMEOUT_SEC
        )
        
        # Extract final response
        final_message = result["messages"][-1]
        response_text = final_message.content if hasattr(final_message, 'content') else str(final_message)
        
        logger.info(f"✅ Agent response for user {user_id}: {response_text[:100]}...")
        return response_text
        
    except asyncio.TimeoutError:
        logger.error(f"⏱️ Agent timeout for user {user_id}")
        return "Xin lỗi, yêu cầu của bạn mất quá nhiều thời gian xử lý. Vui lòng thử lại."
    except Exception as e:
        logger.error(f"❌ Agent error for user {user_id}: {e}", exc_info=True)
        return "Xin lỗi, đã xảy ra lỗi khi xử lý yêu cầu của bạn."

# ==================== MESSAGE HANDLER ====================
async def handle_message(
    redis: aioredis.Redis,
    payload: Dict[str, Any],
    message: aio_pika.IncomingMessage
):
    """Core message handler with idempotency and retries"""
    job_id = payload["job_id"]
    user_id = payload["user_id"]
    user_message = payload["text"]
    
    # 1. Idempotency check
    if await is_job_completed(redis, job_id):
        logger.info(f"⏭️ Job {job_id} already completed (idempotent)")
        await message.ack()
        return
    
    # 2. Acquire distributed lock
    lock_key = f"lock:job:{job_id}"
    if not await acquire_lock(redis, lock_key, config.LOCK_TTL_MS):
        logger.info(f"🔒 Job {job_id} locked by another worker")
        await message.ack()
        return
    
    try:
        # 3. Double-check idempotency
        if await is_job_completed(redis, job_id):
            logger.info(f"⏭️ Job {job_id} completed during lock acquisition")
            await message.ack()
            return
        
        logger.info(f"🔄 Processing job {job_id} for user {user_id}")
        
        # 4. Invoke agent
        reply = await invoke_agent(user_id, user_message, redis)
        
        # 5. Save to database
        await save_message(user_id, "user", user_message)
        await save_message(user_id, "assistant", reply)
        
        # 6. Mark as completed
        await mark_job_completed(redis, job_id)
        
        # 7. ACK message
        await message.ack()
        logger.info(f"✅ Job {job_id} completed successfully")
        
    except Exception as e:
        logger.error(f"❌ Error processing job {job_id}: {e}", exc_info=True)
        
        # Retry logic
        headers = dict(message.headers) if message.headers else {}
        retries = int(headers.get("x-retries", 0))
        
        if retries < config.MAX_RETRIES:
            headers["x-retries"] = retries + 1
            headers["x-error"] = str(e)[:200]
            await message.nack(requeue=True)
            logger.warning(f"⚠️ Job {job_id} requeued (retry {retries + 1}/{config.MAX_RETRIES})")
        else:
            # Send to DLQ
            dlx_msg = aio_pika.Message(
                body=message.body,
                headers={**headers, "x-final-error": str(e)[:500]},
                delivery_mode=DeliveryMode.PERSISTENT
            )
            await message.channel.default_exchange.publish(
                dlx_msg,
                routing_key=config.DLQ_NAME
            )
            await message.ack()
            logger.error(f"☠️ Job {job_id} sent to DLQ after {config.MAX_RETRIES} retries")
    
    finally:
        await release_lock(redis, lock_key)

# ==================== CONSUMER ====================
async def consume_shard(
    redis: aioredis.Redis,
    queue: aio_pika.Queue,
    shard_id: int,
    stop_event: asyncio.Event
):
    """Consume messages from a specific queue shard"""
    logger.info(f"🚀 Worker shard-{shard_id} starting on queue '{queue.name}'")
    
    async def on_message(msg: aio_pika.IncomingMessage):
        async with msg.process(ignore_processed=True):
            try:
                payload = json.loads(msg.body.decode())
                logger.debug(f"[shard-{shard_id}] 📩 Received: {payload}")
                await handle_message(redis, payload, msg)
            except json.JSONDecodeError as e:
                logger.error(f"[shard-{shard_id}] ❌ Invalid JSON: {e}")
                await msg.ack()
            except Exception as e:
                logger.error(f"[shard-{shard_id}] ❌ Handler error: {e}", exc_info=True)
                raise
    
    consumer_tag = await queue.consume(on_message, no_ack=False)
    logger.info(f"✅ Shard-{shard_id} consumer registered (tag: {consumer_tag})")
    
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
    """Initialize RabbitMQ, Redis, PostgreSQL connections"""
    try:
        logger.info("🔌 Initializing infrastructure...")
        
        # RabbitMQ
        conn = await aio_pika.connect_robust(
            config.RABBITMQ_URL,
            timeout=10,
            client_properties={"connection_name": "ai_worker"}
        )
        channel = await conn.channel()
        await channel.set_qos(prefetch_count=config.PREFETCH_COUNT)
        
        # Declare DLX and DLQ
        dlx = await channel.declare_exchange(
            config.DLX_NAME,
            ExchangeType.DIRECT,
            durable=True
        )
        dlq = await channel.declare_queue(config.DLQ_NAME, durable=True)
        await dlq.bind(dlx, routing_key=config.DLQ_NAME)
        
        # Redis
        redis = await aioredis.from_url(
            config.REDIS_URL,
            decode_responses=True,
            max_connections=50
        )
        await redis.ping()
        
        logger.info("✅ Infrastructure initialized: RabbitMQ, Redis, PostgreSQL")
        return conn, channel, redis
        
    except Exception as e:
        logger.error(f"❌ Infrastructure init failed: {e}", exc_info=True)
        raise

# ==================== MAIN CONSUMER ====================
async def start_consumer():
    """Main consumer loop with graceful shutdown"""
    global agent_executor, checkpointer
    
    # Initialize agent
    agent_executor, checkpointer = await create_agent_executor()
    
    # Initialize infrastructure
    conn, channel, redis = await init_infrastructure()
    
    # Stop event
    stop_event = asyncio.Event()
    
    def signal_handler(sig, frame):
        logger.info(f"🛑 Received signal {sig}, initiating shutdown...")
        stop_event.set()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create consumer tasks
    tasks = []
    for shard in range(config.SHARD_COUNT):
        queue_name = f"{config.AI_QUEUE_PREFIX}{shard}"
        queue = await channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": config.DLX_NAME,
                "x-dead-letter-routing-key": config.DLQ_NAME
            }
        )
        task = asyncio.create_task(
            consume_shard(redis, queue, shard, stop_event)
        )
        tasks.append(task)
    
    logger.info(f"🎯 Started {len(tasks)} shard consumers")
    
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        logger.info("⏹ Consumer tasks cancelled")
    finally:
        logger.info("🧹 Cleaning up connections...")
        await channel.close()
        await conn.close()
        await redis.aclose()
        if checkpointer:
            await checkpointer.aclose()
        logger.info("✅ Shutdown complete")

# ==================== FASTAPI APP ====================
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🌟 FastAPI starting up...")
    yield
    logger.info("🌙 FastAPI shutting down...")

app = FastAPI(
    title="AI Message Gateway",
    description="Production-grade LangGraph 1.0.0 agent system",
    version="2.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "2.0.0",
        "langgraph": "1.0.0"
    }

@app.post("/send_message")
async def send_message(request: Request):
    """Enqueue user message for processing"""
    try:
        data = await request.json()
        user_id = data.get("user_id")
        text = data.get("message")
        
        if not user_id or not text:
            raise HTTPException(
                status_code=400,
                detail="Missing required fields: user_id, message"
            )
        
        conn, channel, redis = await init_infrastructure()
        
        try:
            job_id = str(uuid.uuid4())
            payload = {
                "job_id": job_id,
                "user_id": user_id,
                "text": text,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            queue_name = user_shard_queue(user_id)
            
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(payload).encode(),
                    delivery_mode=DeliveryMode.PERSISTENT,
                    content_type="application/json"
                ),
                routing_key=queue_name
            )
            
            logger.info(f"📨 Enqueued job {job_id} for user {user_id} to {queue_name}")
            
            return JSONResponse({
                "status": "ok",
                "job_id": job_id,
                "queue": queue_name,
                "message": "Job enqueued successfully"
            })
            
        finally:
            await channel.close()
            await conn.close()
            await redis.aclose()
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ API error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ==================== ENTRY POINT ====================
if __name__ == "__main__":
    try:
        logger.info("=" * 80)
        logger.info("🚀 Starting AI Worker (LangGraph 1.0.0)")
        logger.info(f"   Shards: {config.SHARD_COUNT}")
        logger.info(f"   LLM: {config.LLM_MODEL}")
        logger.info(f"   Concurrency: {config.LLM_CONCURRENCY}")
        logger.info("=" * 80)
        
        asyncio.run(start_consumer())
        #loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
        #asyncio.set_event_loop(loop)
        #loop.run_until_complete(start_consumer())
        #Windows mặc định dùng ProactorEventLoop, nhưng psycopg async không tương thích. ép buộc dùng SelectorEventLoop
    except KeyboardInterrupt:
        logger.info("🧹 Worker stopped by user")
    except Exception as e:
        logger.error(f"🔥 Worker crashed: {e}", exc_info=True)
        sys.exit(1)