"""
Backend API Integration - Async Version
Sử dụng httpx thay vì requests để tương thích với async/await
"""

import json
from fastapi import HTTPException
import httpx
import os
from decimal import Decimal
from typing import Dict, Any

# Backend API configuration
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8080")
API_TIMEOUT = 30

# =================== BOOKING CHARGING ====================
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
                f"{BACKEND_URL}/api/booking/create",
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
            result = response.json().get("rank")
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
# ==================== FINISH CHARGING SESSION ====================
async def finish_charging_session(user: str, sessionId: str, kWh: float, jwt: str) -> str:
    """
    Gọi API kết thúc phiên sạc - NÉM HTTPException khi có lỗi
    """
    try:
        print(f"🌐 Đang gọi API kết thúc phiên sạc cho user {user} session_id {sessionId}...")

        print(f"📤 Dữ liệu gửi: {kWh}")
        print(f"🔑 JWT: {jwt}")
        async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
            response = await client.post(
                f"{BACKEND_URL}/api/charging/session/finish/{sessionId}",
                json=kWh,
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
            result = response.text
            print(f"✅ API Response: {result}")
    
            if "completed successfully" in result:
                success_msg = (
                    "Kết thúc phiên sạc thành công! anh/chị có thể thanh toán rồi ạ...!"
                )
                return success_msg
            else:
                waiting_msg = (
                   "Kết thúc phiên sạc không thành công! xin lỗi anh/chị vì sự bất tiện này...!"
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
# =================== VIEW PROFILE DRIVER ====================
async def view_car_of_driver(user: str, jwt: str) -> str:
    """
    Xem thông tin xe để hỗ trợ đặt chỗ
    - NÉM HTTPException khi có lỗi
    """
    try:
        print(f"🌐 Đang gọi API xem thông tin xe của user {user}...")

        async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
            response = await client.get(
                f"{BACKEND_URL}/api/car/all/{user}",
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

            # ✅ Xử lý response thành công
            cars = response.json()
            success_msg = []

            for car in cars:
                user_info = {
                    "car_id": car.get("carID"),
                    "car_name": car.get("typeCar"),
                    "license_plate": car.get("licensePlate"),
                    "chassis_number": car.get("chassisNumber"),
                    "charging_type": car.get("chargingType")
                }
                success_msg.append(user_info)

            print(f"✅ API Response: {success_msg}")
            return success_msg

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
# =================== VIEW AVAILABLE STATION ====================
async def view_available_stations_and_post(user: str, jwt: str) -> str:
    """
    Xem thông tin các trạm sạc có sẵn để hỗ trợ đặt chỗ
    - NÉM HTTPException khi có lỗi
    """
    try:
        print(f"🌐 Đang gọi API xem thông tin các trạm sạc của user {user}...")

        async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
            response = await client.get(
                f"{BACKEND_URL}/api/charging/station/available",
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

            # ✅ Xử lý response thành công
            stations = response.json()
            
            if not stations:
                return "⚠️ Hiện tại không có trạm sạc nào khả dụng."
            
            success_msg = []

            for station in stations:
                # Lấy thông tin trạm sạc
                station_info = {
                    "station_id": station.get("idChargingStation"),
                    "station_name": station.get("nameChargingStation"),
                    "address": station.get("address"),
                    "established_time": station.get("establishedTime"),
                    "number_of_posts": station.get("numberOfPosts"),
                    "latitude": station.get("latitude"),
                    "longitude": station.get("longitude"),
                    "active": station.get("active")
                }
                
                # Lấy thông tin các cột sạc khả dụng
                available_posts = station.get("postAvailable", {})
                available_post_ids = [
                    post_id for post_id, is_available in available_posts.items() 
                    if is_available
                ]
                
                station_info["available_posts"] = available_post_ids
                station_info["total_available"] = len(available_post_ids)
                
                success_msg.append(station_info)

            print(f"✅ API Response: Tìm thấy {len(success_msg)} trạm sạc")
            
            # Format response thành dạng text dễ đọc cho LLM
            formatted_response = f"📍 Tìm thấy {len(success_msg)} trạm sạc khả dụng:\n\n"
            
            for idx, station in enumerate(success_msg, 1):
                formatted_response += f"{idx}. 🏢 {station['station_name']} (ID: {station['station_id']})\n"
                formatted_response += f"   📍 Địa chỉ: {station['address']}\n"
                formatted_response += f"   🔌 Số cột sạc: {station['number_of_posts']}\n"
                formatted_response += f"   ✅ Cột khả dụng: {station['total_available']} cột ({', '.join(station['available_posts'])})\n"
                formatted_response += f"   📅 Thành lập: {station['established_time']}\n"
                formatted_response += f"   🟢 Trạng thái: {'Đang hoạt động' if station['active'] else 'Ngừng hoạt động'}\n\n"
            
            return formatted_response

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