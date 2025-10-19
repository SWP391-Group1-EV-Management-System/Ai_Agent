"""
Tool Registration Module - FIXED (No Recursion)
Register all available tools for the LangGraph agent
"""

from langchain_core.tools import tool
from typing import List
import json
import asyncio

# ✅ Import API functions với alias để tránh conflict
from tools.API_BE import (
    listUser_api,  # ← API function thực
    add_user_to_api  # ← API function thực
)

# ==================== WRAPPED API TOOLS ====================

@tool
async def list_users(query: str = "") -> str:
    """
    Liệt kê danh sách người dùng từ hệ thống backend.
    
    Sử dụng tool này KHI user hỏi về:
    - "danh sách người dùng"
    - "có bao nhiêu user"
    - "liệt kê users"
    - "xem thông tin người dùng"
    
    Args:
        query: Tìm kiếm người dùng (optional, không sử dụng)
    
    Returns:
        Danh sách người dùng đầy đủ
    """
    try:
        print("=" * 80)
        print("🔧 TOOL CALLED: list_users")
        print("Đang gọi API lấy danh sách user...")

        # ✅ Gọi API function (không cần limit)
        result = await listUser_api(query)

        print(f"📦 API Response type: {type(result)}")
        print(f"📦 API Response length: {len(result) if result else 0}")
        print(f"📦 API Response preview: {result[:200] if result else 'EMPTY'}")
        print("=" * 80)
        
        # ✅ Return result trực tiếp (đã format sẵn từ API)
        if result and result.strip():
            return result
        else:
            return "❌ Không thể lấy danh sách người dùng từ hệ thống."
            
    except Exception as e:
        error_msg = f"❌ Lỗi khi gọi API lấy danh sách user: {str(e)}"
        print(f"❌ TOOL ERROR: {error_msg}")
        return error_msg


@tool
async def add_user(userName: str, password: str, role: str = "USER") -> str:
    """
    Thêm người dùng mới vào hệ thống backend.
    
    Sử dụng tool này KHI user muốn:
    - "thêm user mới"
    - "tạo tài khoản"
    - "đăng ký người dùng"
    
    Args:
        userName: Tên đăng nhập (bắt buộc)
        password: Mật khẩu (bắt buộc)
        role: Vai trò (USER hoặc ADMIN, mặc định: USER)
    
    Returns:
        Kết quả thêm người dùng
    """
    try:
        print("=" * 80)
        print(f"🔧 TOOL CALLED: add_user")
        print(f"📝 Parameters: userName={userName}, role={role}")
        
        # ✅ Call API function
        result = await add_user_to_api(
            userName=userName,
            password=password,
            role=role
        )
        
        print(f"📦 API Response: {result[:200] if result else 'EMPTY'}")
        print("=" * 80)
        
        # ✅ Return result trực tiếp
        return result
            
    except Exception as e:
        error_msg = f"❌ Lỗi khi thêm user {userName}: {str(e)}"
        print(f"❌ TOOL ERROR: {error_msg}")
        return error_msg


# ==================== UTILITY TOOLS ====================

@tool
def get_current_time() -> str:
    """
    Lấy thời gian hiện tại.
    
    Sử dụng khi user hỏi:
    - "mấy giờ rồi"
    - "bây giờ là thời gian nào"
    - "cho em biết giờ"
    
    Returns:
        Thời gian hiện tại theo định dạng dễ đọc
    """
    from datetime import datetime
    now = datetime.now()
    weekdays = ["Chủ Nhật", "Thứ Hai", "Thứ Ba", "Thứ Tư", "Thứ Năm", "Thứ Sáu", "Thứ Bảy"]
    weekday = weekdays[now.weekday() if now.weekday() != 6 else 0]
    return f"⏰ Bây giờ là {now.strftime('%H:%M:%S')}, ngày {now.strftime('%d/%m/%Y')} ({weekday})"


@tool
def calculate(expression: str) -> str:
    """
    Tính toán biểu thức toán học đơn giản.
    
    Sử dụng khi user hỏi:
    - "tính giúp em..."
    - "2 + 2 bằng mấy"
    - "10 nhân 5"
    
    Args:
        expression: Biểu thức như "2 + 2", "10 * 5", "100 / 4"
    
    Returns:
        Kết quả tính toán
    """
    try:
        # Safe eval với whitelist functions
        allowed_names = {
            "abs": abs, "round": round, "min": min, "max": max,
            "pow": pow, "sum": sum
        }
        result = eval(expression, {"__builtins__": {}}, allowed_names)
        return f"🔢 Kết quả của {expression} = {result}"
    except Exception as e:
        return f"❌ Lỗi tính toán: {str(e)}"


@tool
def get_random_number(min_val: int = 1, max_val: int = 100) -> str:
    """
    Tạo số ngẫu nhiên trong khoảng min đến max.
    
    Sử dụng khi user hỏi:
    - "cho em một số ngẫu nhiên"
    - "random số từ 1 đến 100"
    
    Args:
        min_val: Giá trị nhỏ nhất (mặc định: 1)
        max_val: Giá trị lớn nhất (mặc định: 100)
    
    Returns:
        Số ngẫu nhiên
    """
    import random
    num = random.randint(min_val, max_val)
    return f"🎲 Số ngẫu nhiên từ {min_val} đến {max_val}: **{num}**"


@tool
def get_weather(city: str) -> str:
    """
    Lấy thông tin thời tiết cho một thành phố (dữ liệu mô phỏng).
    
    Sử dụng khi user hỏi về thời tiết:
    - "thời tiết hôm nay"
    - "thời tiết ở Hà Nội"
    
    Args:
        city: Tên thành phố
    
    Returns:
        Thông tin thời tiết
    """
    import random
    weathers = [
        ("Nắng ☀️", "Trời quang đãng, ít mây"),
        ("Mây ☁️", "Nhiều mây, không mưa"),
        ("Mưa 🌧️", "Có mưa rào và dông"),
        ("Gió 💨", "Gió nhẹ đến trung bình")
    ]
    weather, desc = random.choice(weathers)
    temp = random.randint(22, 35)
    humidity = random.randint(60, 90)
    
    return f"""🌤️ Thời tiết tại {city}:
━━━━━━━━━━━━━━━━━━━━
• Trạng thái: {weather}
• Mô tả: {desc}
• Nhiệt độ: {temp}°C
• Độ ẩm: {humidity}%
━━━━━━━━━━━━━━━━━━━━"""


@tool
def search_info(query: str) -> str:
    """
    Tìm kiếm thông tin (mock - giả lập).
    
    Sử dụng khi user muốn tìm hiểu về một chủ đề.
    
    Args:
        query: Câu truy vấn tìm kiếm
    
    Returns:
        Kết quả tìm kiếm mô phỏng
    """
    results = [
        f"📄 Thông tin chi tiết về {query}",
        f"📚 Hướng dẫn sử dụng {query}",
        f"🔗 Tài liệu tham khảo {query}"
    ]
    return "🔍 Kết quả tìm kiếm:\n\n" + "\n".join([f"{i+1}. {r}" for i, r in enumerate(results)])


# ==================== TOOL REGISTRY ====================

# ✅ Danh sách tất cả tools (Priority order)
TOOLS: List = [
    # API Tools (Primary - Ưu tiên cao nhất)
    list_users,      # Danh sách người dùng
    add_user,        # Thêm người dùng
    
    # Utility Tools (Secondary - Thứ yếu)
    get_current_time,  # Thời gian
    calculate,         # Tính toán
    get_random_number, # Random
    get_weather,       # Thời tiết
    search_info        # Tìm kiếm
]

# Tool names for reference
TOOL_NAMES = [tool.name for tool in TOOLS]


# ==================== TOOL INFO ====================

def print_tool_info():
    """Print all registered tools"""
    print("\n" + "=" * 80)
    print("✅ REGISTERED TOOLS")
    print("=" * 80)
    for i, tool in enumerate(TOOLS, 1):
        print(f"\n{i}. {tool.name}")
        print(f"   Description: {tool.description[:100]}...")
        
        # Print args if available
        if hasattr(tool, 'args_schema') and tool.args_schema:
            fields = list(tool.args_schema.__fields__.keys())
            print(f"   Arguments: {', '.join(fields) if fields else 'None'}")
    
    print("\n" + "=" * 80)


# ==================== VALIDATION ====================

def validate_tools():
    """Validate all tools are properly configured"""
    print("\n🔍 Validating tools...")
    
    errors = []
    warnings = []
    
    for tool in TOOLS:
        # Check required attributes
        if not hasattr(tool, 'name'):
            errors.append(f"Tool missing 'name' attribute")
            continue
            
        if not hasattr(tool, 'description'):
            warnings.append(f"Tool {tool.name} missing 'description'")
        
        # Check for recursion issues
        if tool.name in ['list_users', 'add_user']:
            import inspect
            try:
                source = inspect.getsource(tool.func)
                if f"await {tool.name}.ainvoke" in source:
                    errors.append(f"⚠️  Tool {tool.name} has recursion issue!")
            except Exception as e:
                warnings.append(f"Could not inspect {tool.name}: {e}")
    
    # Print results
    if errors:
        print("❌ Validation FAILED:")
        for err in errors:
            print(f"   • {err}")
        return False
    
    if warnings:
        print("⚠️  Validation warnings:")
        for warn in warnings:
            print(f"   • {warn}")
    
    print(f"✅ All {len(TOOLS)} tools validated successfully")
    return True


# ==================== STATISTICS ====================

def get_tool_stats():
    """Get statistics about registered tools"""
    api_tools = [t for t in TOOLS if t.name in ['list_users', 'add_user']]
    util_tools = [t for t in TOOLS if t not in api_tools]
    
    return {
        "total": len(TOOLS),
        "api_tools": len(api_tools),
        "utility_tools": len(util_tools),
        "names": TOOL_NAMES
    }


if __name__ == "__main__":
    print_tool_info()
    
    if validate_tools():
        stats = get_tool_stats()
        print(f"\n📊 Statistics:")
        print(f"   • Total tools: {stats['total']}")
        print(f"   • API tools: {stats['api_tools']}")
        print(f"   • Utility tools: {stats['utility_tools']}")
        print(f"   • Tool names: {', '.join(stats['names'])}")
    
    print("\n✅ Tool registration module ready!")

    