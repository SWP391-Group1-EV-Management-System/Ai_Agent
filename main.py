import os
import sqlite3

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
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.sqlite import SqliteSaver
# Tải biến môi trường từ file .env
load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

system_prompt = """
Bạn là trợ lý ảo của tôi tên là EV staff 🤖.
Xưng "em" khi nói chuyện và gọi người dùng là "anh hoặc chị".
Nhiệm vụ chính: hỗ trợ CRUD dữ liệu người dùng qua API backend.
Nếu bạn cần thêm user thì hãy gọi tool 'add_user'.
Nếu không chắc thông tin, hãy hỏi lại tôi.
"""
# --- Khởi tạo mô hình LLM ---
llm = ChatGoogleGenerativeAI(
    model="gemini-2.5-flash",  # Sử dụng model đúng theo môi trường của bạn
    temperature=0.3,
    convert_system_message_to_human=True,
   # system_message=system_prompt,
    #verbose=True
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

# --- Khởi tạo agent với tools ---
'''
agent = initialize_agent(
    tools
    llm,
    agent=AgentType.CHAT_CONVERSATIONAL_REACT_DESCRIPTION,
    verbose=True,
   memory=memory,
    handle_parsing_errors=True,
    max_iterations=3,
    #system_message=
)
'''
#tool_executor = ToolExecutor(tools)
tool_node = ToolNode(tools=tools)


#memory_saver = MemorySaver() # Dùng checkpointer
# khởi tạo một agent với llm và tools
conn = sqlite3.connect("memory.db", check_same_thread=False)
memory = SqliteSaver(conn)
agent = create_react_agent(
    model=llm,
    tools=tools,
)
# --- Khởi tạo StateGraph và MessagesState ---
graph = StateGraph(MessagesState)
# Tạo node tools và agent
tool_node = ToolNode(tools)
graph.add_node("agent", agent)
graph.add_node("tools", tool_node)

# Kết nối luồng xử lý
# nếu muốn lọc dữ liệu hoặc valid trước khi cho agent xử lý 
#graph.add_node("preprocess", preprocess_node)
#graph.add_edge(START, "preprocess")
#graph.add_edge("preprocess", "agent")
'''
graph.add_edge(START, "agent")
graph.add_edge("agent", "tools")
graph.add_edge("tools", "agent")
graph.add_edge("agent", END)
'''
graph.add_edge(START, "agent")

graph.add_conditional_edges(
    "agent",
    lambda state: "tools" if state["messages"][-1].tool_calls else END,
    {"tools": "tools", END: END},
)

graph.add_edge("tools", "agent")
#app = graph.compile()
app = graph.compile(checkpointer =memory)

# --- Chạy thử ---
print("🤖 Trợ lý ảo sẵn sàng! Hỗ trợ anh/chị.")
print("Nhập 'exit' để thoát.\n")
thread_id = "Bao_thread"
while True:
    user_input = input("🟢 Bạn: ")
    if user_input.lower() == "exit":
        #memory_manager.save_to_file("Bao")
        print("Tạm biệt 👋")
        break
    try:
        #response = agent.invoke({
        #    "messages": memory.chat_memory.messages + [
        #        {"role": "system", "content": system_prompt},  # thêm dòng này
        #        {"role": "user", "content": user_input}
        #    ]
        #})
        # Agent invoke
        #full_messages = memory.chat_memory.messages + [
        #    {"role": "system", "content": system_prompt},
        #   {"role": "user", "content": user_input}
        #]
        #response = app.invoke({"messages": full_messages})
        response = app.invoke(
        {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_input}
            ]
        },
        config={"configurable": {"thread_id": thread_id}}
        )
        ai_msg = response["messages"][-1].content
        print("🤖 Trợ lý ảo:", ai_msg)
    
    except Exception as e:
        print("⚠️ Lỗi:", e)
        # cập nhật lịch sử chat
    #finally:
    #    memory_manager.update_and_save("Bao", user_input, ai_msg)
