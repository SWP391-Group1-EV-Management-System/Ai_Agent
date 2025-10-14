import asyncio, json, uuid, os, logging, sys, signal
import aio_pika
import redis.asyncio as aioredis
from fastapi import FastAPI, Request
from dotenv import load_dotenv
from langchain.agents import create_react_agent
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.chains import LLMMathChain
from langchain_community.tools import DuckDuckGoSearchRun
from langchain.tools import Tool
from tools.API_BE import listUser_api, add_user_to_api
from data.personality_config import SYSTEM_PROMPT, PERSONALITY_CONFIG
from data.training_examples import GREETING_EXAMPLES, SUPPORT_EXAMPLES, MATH_EXAMPLES
from langgraph.graph import StateGraph, START, END, MessagesState
from langgraph.prebuilt import ToolNode, create_react_agent
#from langgraph.checkpoint.memory import MemorySaver
from functools import partial
from hashlib import md5
from typing import Dict, Any, List
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select, text
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.output_parsers import StrOutputParser

load_dotenv()
RABBIT_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost/")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:12345@localhost:5432/DataAIAgent")
SHARD_COUNT = int(os.getenv("SHARD_COUNT", "8"))   # số queue shard
AI_QUEUE_PREFIX = "ai_jobs.shard_"
DLX = "ai_jobs.dlx"  # dead-letter exchange

# async DB
engine = create_async_engine(DATABASE_URL, pool_size=20, max_overflow=10)
AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
#executor = ThreadPoolExecutor(max_workers=5)
app = FastAPI(title="AI Message Gateway")
# LLM stub (replace with real client). If sync -> run_in_executor
class DummyLLM:
    def predict(self, prompt: str):
        return f"LLM reply to: {prompt[:120]}"
USE_DUMMY = os.getenv("USE_DUMMY", "false").lower() == "true"

if USE_DUMMY:
    llm = DummyLLM()
else:
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=0.3,
        google_api_key=os.getenv("GOOGLE_API_KEY"),
        max_tokens=2048,
        top_p=0.95,
    )
llm_sem = asyncio.Semaphore(int(os.getenv("LLM_CONCURRENCY", "4")))
# --- Thiết lập logger toàn cục ---
# ✅ Tạo LCEL Chain với history support
chat_prompt = ChatPromptTemplate.from_messages([
    ("system", "Bạn là trợ lý AI thông minh và thân thiện. Hãy trả lời câu hỏi của người dùng một cách chính xác và hữu ích."),
    MessagesPlaceholder(variable_name="chat_history"),
    ("human", "{user_input}")
])
# ✅ Chain: prompt | llm | parser
chat_chain = chat_prompt | llm | StrOutputParser()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("ai_worker.log", encoding="utf-8"),  # lưu file log
        logging.StreamHandler(sys.stdout)                        # in ra console
    ]
)

logger = logging.getLogger(__name__)
async def call_llm_with_history(user_input: str, history: List[Dict[str, str]]) -> str:
    """
    Gọi LLM với chat history sử dụng LCEL Chain
    
    Args:
        user_input: Tin nhắn mới từ user
        history: List các messages trước đó [{"role": "user/assistant", "content": "..."}]
    
    Returns:
        Câu trả lời từ LLM
    """
    try:
        if USE_DUMMY:
            # ✅ Thêm delay để simulate LLM call thật
            await asyncio.sleep(2)
            return f"Đây là câu trả lời giả cho: {user_input[:50]}..."
        
        # ✅ Convert history từ DB format sang LangChain messages
        chat_history = []
        for msg in history[-10:]:  # Chỉ lấy 10 tin nhắn gần nhất để tiết kiệm tokens
            if msg["role"] == "user":
                chat_history.append(HumanMessage(content=msg["content"]))
            elif msg["role"] == "assistant":
                chat_history.append(AIMessage(content=msg["content"]))
        
        # ✅ Invoke chain với ainvoke() - async native
        response = await chat_chain.ainvoke({
            "chat_history": chat_history,
            "user_input": user_input
        })
        
        return response
    
    except Exception as e:
        logger.error(f"❌ LLM call failed: {e}", exc_info=True)
        return "Xin lỗi, tôi không thể xử lý yêu cầu này lúc này."
# util: shard key -> queue name
def user_shard_queue(user_id: str) -> str:
    h = int(md5(user_id.encode()).hexdigest()[:8], 16)
    return f"{AI_QUEUE_PREFIX}{h % SHARD_COUNT}"

# initialize connections
async def init():
    """Khởi tạo kết nối RabbitMQ, Redis và PostgreSQL"""
    try:
        logger.info("🔌 Đang kết nối RabbitMQ & Redis...")

        # Kết nối RabbitMQ
        conn = await aio_pika.connect_robust(RABBIT_URL)
        channel = await conn.channel()
        await channel.set_qos(prefetch_count=10)  # Giới hạn mỗi consumer tối đa 10 task song song

        # Kết nối Redis
        redis = await aioredis.from_url(REDIS_URL, decode_responses=True)

        logger.info("✅ Kết nối RabbitMQ, Redis, PostgreSQL thành công.")
        return conn, channel, redis, engine

    except Exception as e:
        logger.error(f"❌ Lỗi khi khởi tạo kết nối: {e}", exc_info=True)
        raise

# idempotency check
async def is_done(redis, job_id):
    return await redis.sismember("jobs:completed", job_id)

async def mark_done(redis, job_id, ttl=86400):
    await redis.sadd("jobs:completed", job_id)
    await redis.expire("jobs:completed", ttl)

# Redlock-ish simple lock (use redis-py-redlock for production or Redis docs)
async def acquire_lock(redis, key, ttl=30_000):
    ok = await redis.set(key, "1", nx=True, px=ttl)
    return ok

async def release_lock(redis, key):
    await redis.delete(key)

# checkpointer operations
async def get_cp(redis, user_id):
    raw = await redis.get(f"checkpointer:{user_id}")
    return json.loads(raw) if raw else {"in_progress":[], "completed":[]}

async def save_cp(redis, user_id, cp):
    await redis.set(f"checkpointer:{user_id}", json.dumps(cp), ex=3600)

# core job handler
async def handle_message(redis, payload: Dict[str,Any], message: aio_pika.IncomingMessage):
    job_id = payload["job_id"]
    user_id = payload["user_id"]
    user_message = payload["text"]  # ✅ Đổi tên từ text → user_message
    
    # Idempotency: skip if already processed
    if await is_done(redis, job_id):
        logger.info(f"Skip completed job {job_id}")
        await message.ack()
        return
    
    # Acquire distributed lock
    lock_key = f"lock:job:{job_id}"
    if not await acquire_lock(redis, lock_key, ttl=30000):
        logger.info(f"Job {job_id} đang được xử lý bởi worker khác")
        await message.ack()
        return

    try:
        # Double-check idempotency sau khi có lock
        if await is_done(redis, job_id):
            logger.info(f"Skip completed job {job_id} (double-check)")
            await message.ack()
            return
        
        # Mark in-progress in checkpointer
        cp = await get_cp(redis, user_id)
        if job_id in cp.get("completed", []):
            await message.ack()
            return
        if job_id not in cp.get("in_progress", []):
            cp.setdefault("in_progress", []).append(job_id)
            await save_cp(redis, user_id, cp)

        # ✅ Load history from Postgres
        async with AsyncSessionLocal() as db:
            res = await db.execute(
                text("SELECT role, content FROM chat_messages WHERE user_id=:u ORDER BY created_at ASC LIMIT 100"),  # ✅ text() giờ hoạt động đúng
                {"u": user_id}
            )
            rows = res.fetchall()
            history = [{"role": r[0], "content": r[1]} for r in rows]

        # ✅ Gọi LLM với history sử dụng LCEL Chain (limit concurrency)
        async with llm_sem:
            reply = await call_llm_with_history(user_message, history)  # ✅ Dùng user_message
            logger.info(f"🤖 Gemini reply for job {job_id}: {reply[:100]}...")

        # ✅ Save to DB (user message + assistant reply)
        async with AsyncSessionLocal() as db:
            await db.execute(
                text("INSERT INTO chat_messages (user_id, role, content) VALUES (:u, :r, :c)"),
                {"u": user_id, "r": "user", "c": user_message}  # ✅ Dùng user_message
            )
            await db.execute(
                text("INSERT INTO chat_messages (user_id, role, content) VALUES (:u, :r, :c)"),
                {"u": user_id, "r": "assistant", "c": reply}
            )
            await db.commit()

        # Mark completed & idempotent
        cp = await get_cp(redis, user_id)
        if job_id in cp.get("in_progress", []):
            cp["in_progress"].remove(job_id)
        cp.setdefault("completed", []).append(job_id)
        await save_cp(redis, user_id, cp)
        await mark_done(redis, job_id)

        # If no in_progress left, clear cp key
        if not cp.get("in_progress"):
            await redis.delete(f"checkpointer:{user_id}")

        await message.ack()
        logger.info(f"✅ Job {job_id} hoàn thành thành công")
        
    except Exception as e:
        logger.error(f"❌ Lỗi xử lý job {job_id}: {e}", exc_info=True)
        
        # Retry logic with exponential backoff
        headers = dict(message.headers) if message.headers else {}
        retries = int(headers.get("x-retries", 0))
        
        if retries < 5:
            headers["x-retries"] = retries + 1
            await message.nack(requeue=True)
            logger.warning(f"⚠️ Job {job_id} retry {retries + 1}/5")
        else:
            # Publish to DLX after 5 retries
            dlx_msg = aio_pika.Message(body=message.body, headers=headers)
            await message.channel.default_exchange.publish(dlx_msg, routing_key="ai_jobs.dlq")
            await message.ack()
            logger.error(f"❌ Job {job_id} failed after 5 retries, sent to DLQ")
            
    finally:
        await release_lock(redis, lock_key)

# consumer starter: bind to the appropriate shard queue and consume with prefetch set
async def consume_shard(redis, q, shard_id, stop_event: asyncio.Event):
    logger.info(f"🚀 Worker shard-{shard_id} bắt đầu tiêu thụ hàng đợi '{q.name}'")
    #  Định nghĩa callback xử lý message
    async def on_message(msg: aio_pika.IncomingMessage):
        """Callback được gọi khi có message mới từ queue"""
        async with msg.process(ignore_processed=True):
            try:
                payload = json.loads(msg.body.decode())
                logger.info(f"[shard-{shard_id}] 📩 Nhận job: {payload}")
                
                # Xử lý message (handle_message sẽ tự ACK/NACK)
                await handle_message(redis, payload, msg)
                
            except json.JSONDecodeError:
                logger.error(f"[shard-{shard_id}] ❌ Lỗi JSON: {msg.body}")
                raise
                
            except Exception as e:
                logger.error(f"[shard-{shard_id}] ❌ Lỗi xử lý: {e}", exc_info=True)
                raise
    
    #  Đăng ký consumer với callback
    consumer_tag = await q.consume(on_message, no_ack=False)
    logger.info(f"✅ Consumer shard-{shard_id} đã đăng ký (tag: {consumer_tag})")
    
    try:
        #  Chờ cho đến khi stop_event được set
        await stop_event.wait()
        logger.info(f"[shard-{shard_id}] 🛑 Nhận tín hiệu dừng")
        
    except asyncio.CancelledError:
        logger.info(f"[shard-{shard_id}] 🛑 Task bị cancel")
        raise
        
    finally:
        #  Hủy consumer khi dừng
        try:
            await q.cancel(consumer_tag)
            logger.info(f"[shard-{shard_id}] ✅ Đã hủy consumer")
        except Exception as e:
            logger.warning(f"[shard-{shard_id}] ⚠️ Lỗi khi hủy consumer: {e}")
        
        logger.info(f"🛑 Worker shard-{shard_id} dừng tiêu thụ '{q.name}'")
async def start_consumer():
    conn, channel, redis, engine = await init()

    stop_event = asyncio.Event()
    tasks = []
    # Tạo consumer cho từng shard
    for shard in range(SHARD_COUNT):
        qname = f"{AI_QUEUE_PREFIX}{shard}"
        queue = await channel.declare_queue(qname, durable=True)
        tasks.append(asyncio.create_task(consume_shard(redis, queue, shard, stop_event)))

    def signal_handler(sig, frame):
        logger.info("🛑 Nhận tín hiệu dừng, đang shutdown...")
        stop_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # --- chạy cho tới khi bị dừng ---
    '''
    await stop_event.wait()
    logger.info("⏹ Đang hủy các consumer tasks...")

    for t in tasks:
        t.cancel()

    with suppress(asyncio.CancelledError):
        await asyncio.gather(*tasks, return_exceptions=True)
    '''
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        logger.info("⏹ Đang hủy các consumer tasks...")
    # --- cleanup ---
    logger.info("💾 Đang đóng kết nối...")
    await channel.close()
    await conn.close()
    #await redis.close()
    await redis.aclose()
    logger.info("✅ Worker shutdown hoàn tất.")

# FastAPI app cho UI gửi message
@app.post("/send_message")
async def send_message(request: Request):
    """Nhận message từ UI và gửi vào RabbitMQ shard queue"""
    data = await request.json()
    user_id = data.get("user_id")
    text = data.get("message")

    if not user_id or not text:
        return {"status": "error", "detail": "Thiếu user_id hoặc message"}

    try:
        # Khởi tạo kết nối nhanh tới RabbitMQ + Redis
        conn, channel, redis, _ = await init()

        # Sinh job_id duy nhất
        job_id = str(uuid.uuid4())

        # Tạo payload gửi cho worker
        payload = {"job_id": job_id, "user_id": user_id, "text": text}
        body = json.dumps(payload).encode()

        # Xác định shard queue
        queue_name = user_shard_queue(user_id)

        # Gửi message vào queue
        await channel.default_exchange.publish(
            aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key=queue_name
        )

        logger.info(f"📨 Gửi job {job_id} của user {user_id} vào queue '{queue_name}'")

        # Đóng kết nối sau khi publish
        await channel.close()
        await conn.close()
        await redis.close()

        return {"status": "ok", "job_id": job_id, "queue": queue_name}

    except Exception as e:
        logger.error(f"❌ Lỗi khi gửi message: {e}", exc_info=True)
        return {"status": "error", "detail": str(e)}
   
    

if __name__ == "__main__":
    try:
        asyncio.run(start_consumer())
    except KeyboardInterrupt:
        logger.info("🧹 Dừng worker theo yêu cầu người dùng.")
    except Exception as e:
        logger.error(f"🔥 Worker crashed: {e}", exc_info=True)
