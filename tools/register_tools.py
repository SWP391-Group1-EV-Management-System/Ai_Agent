from langchain.tools import Tool
from tools.API_BE import listUser_api, add_user_to_api

# Dùng sync_wrapper nếu hàm async
def async_tool(fn):
    async def wrapper(*args, **kwargs):
        return await fn(*args, **kwargs)
    return wrapper

TOOLS = [
    Tool(
        name="List_Users",
        func=async_tool(listUser_api),
        description="Liệt kê danh sách người dùng từ hệ thống backend."
    ),
    Tool(
        name="Add_User",
        func=async_tool(add_user_to_api),
        description="Thêm người dùng mới vào hệ thống backend. Dữ liệu gồm tên, vai trò, email..."
    )
]
