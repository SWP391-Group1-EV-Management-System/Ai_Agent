"""
Tool Registration Module - FIXED (No Recursion)
Register all available tools for the LangGraph agent
"""

from fastapi import HTTPException
from langchain_core.tools import tool
from typing import List
import json
import logging
from datetime import datetime, timezone
logger = logging.getLogger(__name__)

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
    Xem danh sách các trạm sạc và cột sạc khả dụng, tự động sắp xếp theo khoảng cách từ vị trí hiện tại.
    
    🎯 SỬ DỤNG TOOL NÀY KHI:
    - User muốn "gợi ý trạm có chỗ trống"
    - User hỏi "trạm nào đang trống?"
    - User nói "cho tôi xem các trạm sạc"
    - User muốn "tìm trạm gần nhất"
    - User muốn đặt chỗ nhưng chưa biết trạm nào
    
    ⚠️ CÁCH HOẠT ĐỘNG:
    1. Tool TỰ ĐỘNG lấy GPS từ Redis (nếu có)
    2. Gọi API Spring Boot với GPS để tính khoảng cách
    3. Trả về danh sách trạm đã sắp xếp theo khoảng cách gần → xa
    4. Hiển thị thông tin chi tiết: địa chỉ, khoảng cách, trụ trống
    
    📋 CẤU TRÚC DỮ LIỆU TRẢ VỀ:
    
    Khi CÓ GPS:
    '''
    📍 Tìm thấy 2 trạm sạc (đã sắp xếp theo khoảng cách):
    
    1. 🏢 Trạm A1 (ID: STA001)
       📍 Địa chỉ: 123 Test Street
       🚗 Khoảng cách: 5.74km
       🔌 Số cột sạc: 3
       ✅ Cột khả dụng: 3 cột (POST001, POST003, POST002)
       📅 Thành lập: 2025-10-23
    
    2. 🏢 Trạm A2 (ID: STA002)
       📍 Địa chỉ: 531 Trường Chinh
       🚗 Khoảng cách: 8.20km
       🔌 Số cột sạc: 4
       ❌ Không còn cột trống (tất cả 4 cột đã đặt)
       📅 Thành lập: 2025-10-23
    '''
    
    Khi KHÔNG CÓ GPS:
    '''
    📍 Tìm thấy 2 trạm sạc khả dụng:
    
    1. 🏢 Trạm A1 (ID: STA001)
       📍 Địa chỉ: 123 Test Street
       🔌 Số cột sạc: 3
       ✅ Cột khả dụng: 2 cột (POST001, POST003)
    
    💡 Lưu ý: Em chưa có vị trí GPS của anh/chị nên không tính được khoảng cách.
    Anh/chị vui lòng cho phép truy cập vị trí để được gợi ý trạm gần nhất ạ.
    '''
    
    🔹 SAU KHI NHẬN KẾT QUẢ, AGENT PHẢI:
    
    1️⃣ PHÂN TÍCH available_posts trong mỗi trạm:
       • available_posts = [] → Trạm ĐÃ HẾT CHỖ
       • available_posts = ["POST001", ...] → Trạm CÒN CHỖ
    
    2️⃣ HIỂN THỊ CHO USER (dựa vào text đã format sẵn):
       
       ✅ NẾU CÓ TRẠM CÓ CHỖ TRỐNG:
       - Đọc và hiển thị thông tin từ response
       - Nhấn mạnh trạm GẦN NHẤT (nếu có khoảng cách)
       - Hỏi: "Anh/chị muốn chọn trạm nào ạ?"
       
       ❌ NẾU TẤT CẢ TRẠM ĐỀU HẾT CHỖ:
       "⚠️ Dạ, hiện tại tất cả các trạm đều đã kín chỗ ạ.
       
       Anh/chị có thể:
       1️⃣ Vào hàng chờ tại trạm [gần nhất]
       2️⃣ Chờ 10-15 phút rồi thử lại
       3️⃣ Liên hệ tổng đài: 1900-xxxx
       
       Anh/chị muốn chọn phương án nào ạ?"
    
    3️⃣ KHI USER CHỌN TRẠM:
       
       User có thể chọn bằng:
       - Số thứ tự: "Chọn trạm số 1"
       - Tên trạm: "Tôi chọn Trạm A1"
       - ID trạm: "Chọn STA001"
       
       Agent phải:
       • Parse thông tin trạm từ response text
       • Tìm available_posts của trạm đó
       • TỰ ĐỘNG chọn trụ đầu tiên: post_id = available_posts[0]
       • KHÔNG HỎI user "Anh/chị muốn chọn trụ nào?"
       • KHÔNG GỌI thêm API
       
       Thông báo:
       "✅ Dạ, em đã tự động chọn trụ [post_id] tại Trạm [name] cho anh/chị ạ.
       
       [Nếu có khoảng cách]: Trạm này cách anh/chị [X]km ạ.
       
       Tiếp tục sang bước chọn xe ạ."
    
    4️⃣ NẾU USER NÓI TRỰC TIẾP TÊN TRẠM (không gọi tool trước):
       
       Ví dụ: "Tôi muốn đặt Trạm A1"
       
       → GỌI tool view_available_stations() trước
       → Tìm "Trạm A1" trong response
       
       • NẾU TRẠM CÓ CHỖ (available_posts.length > 0):
         "✅ Dạ, Trạm A1 đang có [X] trụ trống.
         [Nếu có khoảng cách]: Trạm này cách anh/chị [Y]km.
         Em đã tự động chọn trụ [post_id] cho anh/chị ạ."
       
       • NẾU TRẠM HẾT CHỖ (available_posts = []):
         "⚠️ Dạ, Trạm A1 hiện không còn chỗ trống ạ.
         Anh/chị muốn:
         1️⃣ Vào hàng chờ tại trạm này
         2️⃣ Chọn trạm khác đang có chỗ"
       
       • NẾU KHÔNG TÌM THẤY TRẠM:
         "❌ Dạ, em không tìm thấy trạm [tên] trong hệ thống ạ.
         Anh/chị có thể xem danh sách trạm hiện có không ạ?"
    
    5️⃣ XỬ LÝ TRƯỜNG HỢP ĐẶC BIỆT:
       
       • NẾU KHÔNG CÓ GPS:
         - Vẫn hiển thị danh sách trạm
         - Thêm thông báo: "Em chưa có vị trí GPS..."
         - Gợi ý user bật GPS để được sắp xếp theo khoảng cách
       
       • NẾU TRẠM GẦN NHẤT < 1km:
         - Nhấn mạnh: "Trạm này rất gần anh/chị (chỉ [X]m)"
         - Khuyến khích: "Anh/chị có thể đến ngay ạ"
       
       • NẾU TẤT CẢ TRẠM ĐỀU > 10km:
         - Cảnh báo: "Các trạm đều khá xa (> 10km)"
         - Gợi ý: "Anh/chị có muốn em tìm trong bán kính rộng hơn không?"
    
    Args:
        user (str): Email người dùng (tự động lấy từ user_id trong AgentState)
        jwt (str): Token xác thực (tự động inject từ context hội thoại)
    
    Returns:
        str: Text đã format sẵn, bao gồm:
             - Danh sách trạm với thông tin chi tiết
             - Khoảng cách (nếu có GPS)
             - Trụ khả dụng hoặc thông báo hết chỗ
             - Lưu ý nếu không có GPS
    
    Raises:
        HTTPException: 
            - 401: Không có JWT hoặc JWT không hợp lệ
            - 500: Lỗi server backend
            - 503: Không kết nối được backend
            - 504: Backend timeout
    
    💡 WORKFLOW HOÀN CHỈNH:
        
        ┌─────────────────────────────────────────────┐
        │ User: "Tôi muốn đặt chỗ sạc xe"             │
        └─────────────────┬───────────────────────────┘
                          ▼
        ┌─────────────────────────────────────────────┐
        │ Agent: "Anh/chị muốn:                       │
        │ 1️⃣ Đặt tại trạm cụ thể                      │
        │ 2️⃣ Gợi ý trạm gần nhất"                     │
        └─────────────────┬───────────────────────────┘
                          ▼
        ┌─────────────────────────────────────────────┐
        │ User: "Gợi ý cho tôi"                       │
        └─────────────────┬───────────────────────────┘
                          ▼
        ┌─────────────────────────────────────────────┐
        │ Agent: [GỌI view_available_stations()]      │
        │        → Tool lấy GPS từ Redis              │
        │        → Gọi API với GPS                    │
        │        → Trả về danh sách đã sort          │
        └─────────────────┬───────────────────────────┘
                          ▼
        ┌─────────────────────────────────────────────┐
        │ Agent: "Dạ, đây là các trạm gần nhất:      │
        │                                             │
        │ 1. Trạm A1 - 5.74km - Còn 3 trụ           │
        │ 2. Trạm A2 - 8.20km - Hết chỗ             │
        │                                             │
        │ Anh/chị chọn trạm nào ạ?"                  │
        └─────────────────┬───────────────────────────┘
                          ▼
        ┌─────────────────────────────────────────────┐
        │ User: "Chọn trạm số 1"                      │
        └─────────────────┬───────────────────────────┘
                          ▼
        ┌─────────────────────────────────────────────┐
        │ Agent: [Parse response → Lấy Trạm A1]      │
        │        available_posts = [POST001, ...]     │
        │        post_id = POST001 (tự động)          │
        │                                             │
        │ "✅ Đã chọn trụ POST001 tại Trạm A1 ạ.     │
        │ Trạm cách anh/chị 5.74km.                  │
        │ Sang bước chọn xe ạ."                      │
        └─────────────────┬───────────────────────────┘
                          ▼
        [Tiếp tục workflow chọn xe...]
    
    ⚠️ LƯU Ý TUYỆT ĐỐI - BẮT BUỘC TUÂN THỦ:
    
    ✅ PHẢI LÀM:
    1. Đọc kỹ response text để extract thông tin trạm
    2. Tự động chọn trụ đầu tiên từ available_posts
    3. Nhấn mạnh khoảng cách nếu < 2km (rất gần)
    4. Thông báo rõ ràng nếu không có GPS
    5. Gợi ý hàng chờ nếu trạm hết chỗ
    
    ❌ KHÔNG ĐƯỢC LÀM:
    1. Hỏi user "Anh/chị muốn chọn trụ nào?"
    2. Liệt kê tất cả trụ để user chọn
    3. Gọi thêm API get_available_post_auto()
    4. Bỏ qua việc kiểm tra available_posts
    5. Cho user đặt chỗ tại trạm không còn trụ trống
    
    🔒 Bảo mật:
        - Tool tự động lấy JWT từ context (không cần user cung cấp)
        - Tool tự động lấy GPS từ Redis (không cần user nhập tọa độ)
        - Không bao giờ tự tạo hoặc giả mạo JWT
    
    📊 Performance:
        - Response time: ~500-1000ms (bao gồm Redis + API call)
        - Cache: Không cache (dữ liệu realtime)
        - Retry: Tự động retry 3 lần nếu API fail
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
    
    # ✅ Call API với GPS từ Redis
    print(f"🌐 Calling backend API (with GPS from Redis) to get available stations...")
    
    result = await view_available_stations_and_post(
        user=user,
        jwt=jwt
    )
    
    print(f"📦 API Response preview: {result[:300] if isinstance(result, str) else str(result)[:300]}...")
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