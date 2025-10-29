"""
Tool Registration Module - FIXED (No Recursion)
Register all available tools for the LangGraph agent
"""

from fastapi import HTTPException
from langchain_core.tools import tool
from typing import List
import json
import asyncio


# ✅ Import API functions với alias để tránh conflict
from tools.API_BE import (
    create_booking_api
)

# =================== BOOKING TOOLS ====================
@tool
async def create_booking(user: str, charging_post: str, car: str, jwt: str) -> str:
    """
    Tạo booking đặt chỗ cho trụ sạc xe điện
    
    Sử dụng tool này KHI user muốn:
    - "đặt chỗ trụ sạc"
    - "book trụ sạc"
    - "đặt lịch sạc xe"
    - "tôi muốn sạc xe tại trụ X"
    - "tôi muốn đặt trạm sạc"
    LƯU Ý: phải xác nhận với người dùng thông tin và yêu cầu người dùng nhập "xác nhận" xác nhận trước khi gọi tool này
            khi user nhập "xác nhận", "ok", "đồng ý", "đặt chỗ" thì mới gọi tool này
    Kết quả có thể là:
    - Booking thành công: Người dùng có thể đến trạm ngay
    - Vào hàng chờ: Người dùng phải chờ đến lượt (sẽ có vị trí trong hàng chờ)
    
    Args:
        user (str): email người dùng đặt chỗ (lấy tên của user_id đang chat với bot)
        charging_post (str): Mã trụ sạc - ví dụ: CP001, CP002 (bắt buộc)
        car (str): Mã xe - ví dụ: CAR_A1, CAR_B2 (bắt buộc)
        bạn phải gắn chuỗi jwt hợp lệ vào tham số jwt để xác thực người dùng khi gọi API (lấy từ context của cuộc hội thoại, bắt buộc)

    Returns:
        str: Kết quả đặt chỗ (thành công hoặc vị trí hàng chờ)
    
    Examples:
        User: "Tôi muốn đặt chỗ trụ CP001 cho xe CAR_A1"
        >>> create_booking("email@gmail.com", "CP001", "CAR_A1")
        "✅ Đặt chỗ thành công! Có thể đến trạm ngay"
        
        User: "Book trụ CP002"
        >>> create_booking("email@gmail.com", "CP002", "CAR_B1")
        "⏳ Đã thêm vào hàng chờ! Vị trí: #3"
    """
    # ✅ CRITICAL FIX: BỎ try/catch để HTTPException thoát ra ngoài
    print("=" * 80)
    print(f"🔧 TOOL CALLED: create_booking")
    print(f"📝 Parameters: user={user}, charging_post={charging_post}, car={car}")
    
    # Gọi API function (không wrap try/catch)
    print(f"🆔 Retrieved JWT for user {user}")
    print(f"🔑 Using JWT: {jwt}")
    result = await create_booking_api(
        user=user,
        charging_post=charging_post,
        car=car,
        jwt=jwt
    )
    
    print(f"📦 API Response: {result[:200] if result else 'EMPTY'}")
    print("=" * 80)
    
    return result
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
    create_booking,  # Tạo booking trụ sạc
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

    #agent dường như đang không nhớ context hội thoại trước đó nữa, cần fix lại