import os
from dotenv import load_dotenv
from langchain.agents import AgentType, Tool
from langchain.agents.format_scratchpad import format_to_openai_function_messages
from langchain.agents.output_parsers import ReActSingleInputOutputParser
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.memory import ConversationBufferMemory
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain.chains import LLMMathChain
from langchain_community.tools import DuckDuckGoSearchRun
from langchain_core.messages import SystemMessage, HumanMessage, AIMessage
from tools.API_BE import listUser_api
from tools.API_BE import add_user_to_api
from history_user import MemoryManager
from data.personality_config import SYSTEM_PROMPT, PERSONALITY_CONFIG
from data.training_examples import GREETING_EXAMPLES, SUPPORT_EXAMPLES, MATH_EXAMPLES

# Tải biến môi trường từ file .env
load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# --- Khởi tạo memory trước ---
memory_manager = MemoryManager()
memory = memory_manager.get_memory("Bao")  # Sử dụng tên người dùng để lưu lịch sử chat 

# --- Khởi tạo mô hình Gemini ---
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",  # Sử dụng model đúng theo môi trường của bạn
    temperature=0.3,
    convert_system_message_to_human=True
)

# --- Tạo các tool ---
from langchain.agents import initialize_agent, AgentType

# Tool 1: Máy tính
llm_math = LLMMathChain.from_llm(llm=llm)
calculator_tool = Tool(
    name="Calculator",
    func=llm_math.run,
    description="Công cụ tính toán số học."
)

# Tool 2: List Users
list_users_tool = Tool(
    name="list_users",
    func=listUser_api,
    description="Hiển thị danh sách người dùng trong hệ thống."
)

# Tool 3: Add User
add_user_tool = Tool(
    name="add_user",
    func=add_user_to_api,
    description="Thêm người dùng mới vào hệ thống."
)

# Tool 4: Tìm kiếm web
search_tool = Tool(
    name="search",
    func=DuckDuckGoSearchRun().run,
    description="Tìm kiếm thông tin trên internet."
)

# Tạo danh sách tools
tools = [calculator_tool, list_users_tool, add_user_tool, search_tool]

# Tạo system message với hướng dẫn sử dụng tools
AGENT_SYSTEM_MESSAGE = SYSTEM_PROMPT + "\n\nCông cụ có sẵn:\n" + \
    "- list_users: Xem danh sách người dùng\n" + \
    "- add_user: Thêm người dùng mới\n" + \
    "- Calculator: Tính toán\n" + \
    "- search: Tìm kiếm thông tin"

# --- Khởi tạo agent với tools ---
agent = initialize_agent(
    tools,
    llm,
    agent=AgentType.CHAT_CONVERSATIONAL_REACT_DESCRIPTION,
    verbose=True,
    memory=memory,
    handle_parsing_errors=True,
    max_iterations=3,
    system_message=AGENT_SYSTEM_MESSAGE
)

# Đăng ký tools với agent
agent.tools = tools

# --- Chạy thử ---
print("🤖 Gemini Agent sẵn sàng! Hãy nhập câu hỏi của bạn.")
print("Ví dụ: 'Tính căn bậc hai của 2500 chia 5' hoặc 'Thủ đô của Nhật Bản là gì?'")
print("Nhập 'exit' để thoát.\n")

while True:
    user_input = input("🟢 Bạn: ")
    if user_input.lower() == "exit":
        memory_manager.save_to_file("Bao")
        print("Tạm biệt 👋")
        break
    try:
        response = agent.run(user_input)
        print("🤖 Gemini:", response)
    except Exception as e:
        print("⚠️ Lỗi:", e)
        # cập nhật lịch sử chat
    finally:
        memory_manager.update_and_save("Bao", user_input, response)
