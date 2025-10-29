"""
Backend API Integration - Async Version
Sử dụng httpx thay vì requests để tương thích với async/await
"""

from fastapi import HTTPException
import httpx
import os
from typing import Dict, Any

# Backend API configuration
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8080/api")
API_TIMEOUT = 30

# =================== Booking API ====================
async def create_booking_api(user: str, charging_post: str, car: str, jwt: str) -> str:
    """
    Gọi API tạo booking - NÉM HTTPException khi có lỗi
    """
    try:
        print(f"🌐 Đang gọi API tạo booking cho user {user} tại trạm {charging_post}...")
        booking_data = {
            "user": user,
            "chargingPost": charging_post,
            "car": car
        }
        print(f"📤 Dữ liệu gửi: {booking_data}")
        print(f"🔑 JWT: {jwt}")
        async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
            response = await client.post(
                f"{BACKEND_URL}/booking/create",
                json=booking_data,
                cookies={"jwt": jwt}
            )

            # ✅ CRITICAL: Raise HTTPException cho mọi lỗi HTTP
            if response.status_code != 200:
                error_detail = response.text or f"HTTP {response.status_code}"
                print(f"❌ API trả lỗi {response.status_code}: {error_detail}")
                raise HTTPException(
                    status_code=response.status_code, 
                    detail=f"API Error: {error_detail}"
                )

            # Xử lý response thành công
            result = response.json()
            print(f"✅ API Response: {result}")

            if result == -1:
                success_msg = (
                    f"✅ Đặt chỗ thành công!\n"
                    f"   • Người dùng: {user}\n"
                    f"   • Trạm sạc: {charging_post}\n"
                    f"   • Xe: {car}\n"
                    f"   • Trạng thái: Có thể đến trạm ngay ✨\n"
                    f"\n💡 Anh/chị có thể đến trạm sạc ngay bây giờ!"
                )
                return success_msg
            else:
                waiting_msg = (
                    f"⏳ Đã thêm vào hàng chờ!\n"
                    f"   • Người dùng: {user}\n"
                    f"   • Trạm sạc: {charging_post}\n"
                    f"   • Xe: {car}\n"
                    f"   • Vị trí trong hàng chờ: #{result} 📋\n"
                    f"\n💡 Anh/chị vui lòng chờ đến lượt."
                )
                return waiting_msg

    except HTTPException:
        # ✅ Ném lại HTTPException để tool không catch
        raise

    except httpx.ConnectError as e:
        print(f"❌ Không kết nối được server: {e}")
        raise HTTPException(status_code=503, detail="Không thể kết nối đến server backend")

    except httpx.TimeoutException as e:
        print(f"❌ Timeout: {e}")
        raise HTTPException(status_code=504, detail="Server phản hồi quá chậm")

    except Exception as e:
        print(f"❌ Lỗi không xác định: {e}")
        raise HTTPException(status_code=500, detail=f"Lỗi hệ thống: {str(e)}")

