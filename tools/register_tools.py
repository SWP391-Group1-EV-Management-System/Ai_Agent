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
    create_booking_api,
    finish_charging_session,
    view_available_stations_and_post,
    view_car_of_driver
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
     # 🧠 Bước 1: Check danh sách xe thật từ backend
    print("🔍 Kiểm tra quyền sở hữu xe trước khi tạo booking...")
    car_list_json = await view_car_of_driver(user=user, jwt=jwt)
    
    # Nếu API trả về JSON dạng chuỗi, cần parse
    if isinstance(car_list_json, str):
        try:
            car_list = json.loads(car_list_json)
        except Exception:
            raise HTTPException(status_code=500, detail="Dữ liệu xe trả về không hợp lệ")
    else:
        car_list = car_list_json

    # 🧠 Bước 2: Kiểm tra xem xe người dùng yêu cầu có tồn tại không
    owned_car_ids = [c.get("car_id") for c in car_list]
    print(f"🚗 Danh sách xe người dùng: {owned_car_ids}")

    if car not in owned_car_ids:
        print(f"❌ Xe {car} không thuộc user {user}")
        raise HTTPException(
            status_code=400,
            detail=f"Xe {car} không thuộc quyền sở hữu của anh/chị. Vui lòng kiểm tra lại ạ."
        )
    print(f"✅ Xe {car} thuộc quyền sở hữu của user {user}, tiếp tục tạo booking...")
    result = await create_booking_api(
        user=user,
        charging_post=charging_post,
        car=car,
        jwt=jwt
    )
    
    print(f"📦 API Response: {result[:200] if result else 'EMPTY'}")
    print("=" * 80)
    
    return result
# =================== VIEW LIST CAR OF USER TOOLS ====================
@tool
async def view_list_car_of_user(user: str, jwt: str) -> str:
    """
    Xem danh sách xe của người dùng
    
    Sử dụng tool này KHI user muốn:
    - "tôi không nhớ xe của tôi là gì"
    - "liệt kê các xe đã đăng ký"
    - "cho tôi biết các xe tôi có"
    - "hình như xe của tôi là biển số 29A-123.45"
    Args:
        user (str): email người dùng (lấy tên của user_id đang chat với bot)
        bạn phải gắn chuỗi jwt hợp lệ vào tham số jwt để xác thực người dùng khi gọi API (lấy từ context của cuộc hội thoại, bắt buộc)

    Returns:
        str: Danh sách xe của người dùng
    
    Examples:
        EXAMPLE_1:
        User: "Cho tôi xem danh sách xe của tôi"
        >>> view_list_car_of_user("email@gmail.com", "jwt_token")
        [{"car_id": "CAR_A1", "car_name": "Xe điện 1", "license_plate": "29A-123.45", "chassis_number": "VN123456", "charging_type": "fast"},
         {"car_id": "CAR_B1", "car_name": "Xe điện 2", "license_plate": "29A-678.90", "chassis_number": "VN654321", "charging_type": "normal"}] 
        EXAMPLE_2:
        User: "tôi muốn đặt chỗ với xe biển số 29A-123.45"
        >>> view_list_car_of_user("email@gmail.com", "jwt_token")
        [{"car_id": "CAR_A1", "car_name": "Xe điện 1", "license_plate": "29A-123.45", "chassis_number": "VN123456", "charging_type": "fast"},
        "Đã tìm thấy xe của anh chị, có phải tên xe là 'Xe điện 1' không ạ?"
        EXAMPLE_3:
        User: "tôi muốn đặt chỗ với xe biển số 29A-123.45"
        >>> view_list_car_of_user("email@gmail.com", "jwt_token")
        [{"car_id": "CAR_A1", "car_name": "Xe điện 1", "license_plate": "29A-99999", "chassis_number": "VN123456", "charging_type": "fast"},
        "Chưa tìm thấy xe của anh chị, ý anh chị là xe biển số '29A-99999' tên Xe điện 1 đúng không ạ, em thấy mình đang sỡ hữu xe này"
        
    """
    # ✅ CRITICAL FIX: BỎ try/catch để HTTPException thoát ra ngoài
    print("=" * 80)
    print(f"🔧 TOOL CALLED: view_list_car_of_user")
    print(f"📝 Parameters: user={user}")
    
    # Gọi API function (không wrap try/catch)
    print(f"🆔 Retrieved JWT for user {user}")
    print(f"🔑 Using JWT: {jwt}")
    result = await view_car_of_driver(
        user=user,
        jwt=jwt
    )
    
    print(f"📦 API Response: {result[:200] if result else 'EMPTY'}")
    print("=" * 80)
    
    return result
# =================== FINISH SESSION TOOL ====================
@tool
async def finish_charging(user: str, sessionId: str, kWh: float, jwt: str) -> str:
    """
    Kết thúc phiên sạc cho xe điện

    Sử dụng tool này KHI user muốn:
    - "kết thúc phiên sạc"
    - "hoàn tất sạc xe"
    - "thanh toán cho phiên sạc"
    - "tôi muốn kết thúc sạc tại trụ này"
    - "tôi muốn đặt trạm sạc"
    LƯU Ý: phải xác nhận với người dùng thông tin và yêu cầu người dùng nhập "xác nhận" xác nhận trước khi gọi tool này
            khi user nhập "xác nhận", "ok", "đồng ý", "đặt chỗ" thì mới gọi tool này
    Kết quả có thể là:
    - Kết thúc phiên sạc thành công! anh/chị có thể thanh toán rồi ạ...!
    - Kết thúc phiên sạc không thành công! xin lỗi anh/chị vì sự bất tiện này...!
    
    Args:
        user (str): email người dùng đặt chỗ (lấy tên của user_id đang chat với bot)
        sessionId (str): Mã phiên sạc cần kết thúc (bắt buộc)
        kWh (float): Số kWh đã sạc trong phiên (bắt buộc)
        bạn phải gắn chuỗi jwt hợp lệ vào tham số jwt để xác thực người dùng khi gọi API (lấy từ context của cuộc hội thoại, bắt buộc)

    Returns:
        str: Kết thúc phiên sạc thành công hoặc thất bại (xin lỗi vì bất tiện này khi thất bại)
    
    Examples:
        User: "Tôi muốn kết thúc phiên sạc"
        >>> finish_charging_session("email@gmail.com", "session_123", float("10.5"), "jwt_token")
        "✅ Kết thúc phiên sạc thành công! anh/chị có thể thanh toán rồi ạ...!"

        User: "Kết thúc phiên sạc không thành công"
        >>> finish_charging_session("email@gmail.com", "session_123", float("10.5"), "jwt_token")
        "❌ Kết thúc phiên sạc không thành công! xin lỗi anh/chị vì sự bất tiện này...!"
    """
    # ✅ CRITICAL FIX: BỎ try/catch để HTTPException thoát ra ngoài
    print("=" * 80)
    print(f"🔧 TOOL CALLED: finish_charging_session")
    print(f"📝 Parameters: user={user}, sessionId={sessionId}, kWh={kWh}")
    
    # Gọi API function (không wrap try/catch)
    print(f"🆔 Retrieved JWT for user {user}")
    print(f"🔑 Using JWT: {jwt}")
    result = await finish_charging_session(
        user=user,
        sessionId=sessionId,
        kWh=kWh,
        jwt=jwt
    )
    
    print(f"📦 API Response: {result[:200] if result else 'EMPTY'}")
    print("=" * 80)
    
    return result
# =================== AVAILABLE POST AND STATION ====================
# =================== AVAILABLE STATIONS AND POSTS TOOL ====================
@tool
async def view_available_stations(user: str, jwt: str) -> str:
    """
    Xem danh sách các trạm sạc và cột sạc khả dụng để đặt chỗ.
    
    🎯 SỬ DỤNG TOOL NÀY KHI:
    - User muốn "gợi ý trạm có chỗ trống"
    - User hỏi "trạm nào đang trống?"
    - User nói "cho tôi xem các trạm sạc"
    - User muốn đặt chỗ nhưng chưa biết trạm nào
    
    ⚠️ QUAN TRỌNG - ĐỌC KỸ:
    Tool này trả về JSON với cấu trúc:
    [
        {
            "station_id": "STA001",
            "station_name": "Trạm A1", 
            "address": "123 Test Street",
            "number_of_posts": 3,
            "available_posts": ["POST001", "POST003"],  # ← Danh sách trụ TRỐNG
            "total_available": 2
        },
        ...
    ]
    
    📋 SAU KHI NHẬN KẾT QUẢ, AGENT PHẢI:
    
    1️⃣ PHÂN TÍCH available_posts:
       • Nếu available_posts = [] (rỗng) → Trạm KHÔNG CÒN CHỖ
       • Nếu available_posts = ["POST001", ...] → Trạm CÒN CHỖ
    
    2️⃣ HIỂN THỊ CHO USER:
       
       ✅ NẾU CÓ TRẠM CÓ CHỖ:
       "Dạ, đây là các trạm sạc đang có chỗ trống ạ:
       
       1. 🏢 Trạm A1 (ID: STA001)
          📍 Địa chỉ: 123 Test Street
          🔌 Số cột sạc: 3
          ✅ Cột khả dụng: 2 cột (POST001, POST003)
       
       2. 🏢 Trạm A2 (ID: STA002)
          📍 Địa chỉ: 531 Trường Chinh
          🔌 Số cột sạc: 4
          ✅ Cột khả dụng: 2 cột (POST005, POST004)
       
       Anh/chị muốn chọn trạm nào ạ? (Trả lời số thứ tự hoặc tên trạm)"
       
       ❌ NẾU TẤT CẢ TRẠM ĐỀU KHÔNG CÒN CHỖ:
       "⚠️ Dạ, hiện tại tất cả các trạm sạc đều đã kín chỗ ạ.
       
       📋 Các trạm hiện có:
       1. 🏢 Trạm A1 - ❌ Không còn chỗ trống
       2. 🏢 Trạm A2 - ❌ Không còn chỗ trống
       
       Anh/chị có thể:
       1️⃣ Vào hàng chờ tại trạm bất kỳ
       2️⃣ Chờ em kiểm tra lại sau vài phút
       3️⃣ Liên hệ tổng đài: 1900-xxxx
       
       Anh/chị muốn chọn phương án nào ạ?"
    
    3️⃣ KHI USER CHỌN TRẠM:
       • Lấy station_id và available_posts
       • TỰ ĐỘNG chọn trụ đầu tiên: post_id = available_posts[0]
       • KHÔNG HỎI user chọn trụ nào
       • KHÔNG GỌI thêm API get_available_post_auto()
       
       Thông báo:
       "✅ Dạ, em đã tự động chọn trụ [post_id] cho anh/chị ạ.
       Tiếp tục sang bước chọn xe ạ."
    
    4️⃣ NẾU USER NÓI TÊN TRẠM CỤ THỂ:
       Ví dụ: "Tôi muốn đặt Trạm A1"
       
       → Tìm trạm có station_name = "Trạm A1" trong kết quả
       
       • NẾU TÌM THẤY VÀ available_posts.length > 0:
         "✅ Dạ, Trạm A1 đang có [X] trụ trống.
         Em đã tự động chọn trụ [post_id] cho anh/chị ạ."
       
       • NẾU TÌM THẤY NHƯNG available_posts = []:
         "⚠️ Dạ, Trạm A1 hiện không còn chỗ trống ạ.
         Anh/chị muốn vào hàng chờ hay chọn trạm khác ạ?"
       
       • NẾU KHÔNG TÌM THẤY:
         "❌ Dạ, em không tìm thấy trạm [tên] trong hệ thống ạ.
         Anh/chị có thể kiểm tra lại tên trạm không ạ?"
    
    Args:
        user (str): Email người dùng (tự động lấy từ user_id trong state)
        jwt (str): Token xác thực (tự động inject từ context hội thoại)
    
    Returns:
        str: JSON string chứa danh sách trạm và trụ khả dụng
        
    Example Response:
        Trường hợp có trạm trống:
        >>> view_available_stations("user@email.com", "jwt_token")
        '''
        📍 Tìm thấy 2 trạm sạc khả dụng:
        
        1. 🏢 Trạm A1 (ID: STA001)
           📍 Địa chỉ: 123 Test Street
           🔌 Số cột sạc: 3 cột
           ✅ Cột khả dụng: 2 cột (POST001, POST003)
           📅 Thành lập: 2025-10-23T21:50:26.540258
           🟢 Trạng thái: Đang hoạt động
        
        2. 🏢 Trạm A2 (ID: STA002)
           📍 Địa chỉ: 531 Trường Chinh
           🔌 Số cột sạc: 4 cột
           ✅ Cột khả dụng: 2 cột (POST005, POST004)
           📅 Thành lập: 2025-10-23T21:50:26.576015
           🟢 Trạng thái: Đang hoạt động
        '''
        
        Trường hợp không có trạm trống:
        >>> view_available_stations("user@email.com", "jwt_token")
        '''
        ⚠️ Hiện tại không có trạm sạc nào có chỗ trống.
        
        📋 Các trạm hiện có (tất cả đã kín):
        1. 🏢 Trạm A1 - ❌ 0/3 trụ trống
        2. 🏢 Trạm A2 - ❌ 0/4 trụ trống
        '''
    
    Raises:
        HTTPException: 
            - 401: Không có JWT hoặc JWT không hợp lệ
            - 403: User không có quyền truy cập
            - 500: Lỗi server backend
            - 503: Không kết nối được backend
            - 504: Backend timeout
    
    🔒 Bảo mật:
        Tool tự động sử dụng JWT từ context để xác thực với backend.
        Không bao giờ tự tạo hoặc giả mạo JWT.
    
    💡 WORKFLOW HOÀN CHỈNH:
        
        User: "Tôi muốn đặt chỗ sạc xe"
        
        Agent: "Dạ, anh/chị muốn:
                1️⃣ Đặt chỗ tại trạm cụ thể
                2️⃣ Để em gợi ý trạm có chỗ trống"
        
        User: "Gợi ý cho tôi"
        
        Agent: [GỌI view_available_stations()]
        
        [NHẬN KẾT QUẢ]
        
        Agent: [PHÂN TÍCH available_posts của từng trạm]
               [HIỂN THỊ danh sách trạm CÓ CHỖ TRỐNG]
               [HỎI user chọn trạm]
        
        User: "Chọn trạm số 1"
        
        Agent: [LẤY station_id và available_posts[0]]
               [TỰ ĐỘNG chọn trụ - KHÔNG HỎI user]
               "✅ Em đã chọn trụ POST001 cho anh/chị ạ."
               [CHUYỂN sang bước chọn xe]
    
    ⚠️ LƯU Ý TUYỆT ĐỐI:
        - KHÔNG bao giờ hỏi user "Anh/chị chọn trụ nào?"
        - LUÔN LUÔN tự động chọn trụ đầu tiên trong available_posts
        - CHỈ hiển thị thông tin trụ đã chọn, không liệt kê tất cả trụ
        - NẾU available_posts rỗng → Thông báo không còn chỗ + gợi ý hàng chờ
    """
    print("=" * 80)
    print(f"🔧 TOOL CALLED: view_available_stations")
    print(f"📝 Parameters:")
    print(f"   • User: {user}")
    print(f"   • JWT prefix: {jwt[:20] if jwt else 'MISSING'}...")
    
    # ✅ Validate JWT
    if not jwt:
        error_msg = "❌ Thiếu token xác thực. Vui lòng đăng nhập lại."
        print(f"⚠️  {error_msg}")
        raise HTTPException(status_code=401, detail=error_msg)
    
    # ✅ Call API (không wrap try/catch để HTTPException propagate)
    print(f"🌐 Calling backend API to get available stations...")
    
    result = await view_available_stations_and_post(
        user=user,
        jwt=jwt
    )
    
    print(f"📦 API Response: {result[:300] if isinstance(result, str) else str(result)[:300]}...")
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
    finish_charging,  # Kết thúc phiên sạc
    view_list_car_of_user,  # Xem danh sách xe của user
    view_available_stations,  # Xem trạm và trụ sạc khả dụng
    # Utility Tools (Secondary - Thứ yếu)
    get_current_time,  # Thời gian
    calculate,         # Tính toán
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