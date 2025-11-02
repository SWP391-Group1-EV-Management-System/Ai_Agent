"""
Backend API Integration - Async Version
"""

import json
from fastapi import HTTPException
import httpx
import os
from decimal import Decimal
from typing import Dict, Any
import redis.asyncio as aioredis  # ✅ THÊM IMPORT

# Backend API configuration
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8080")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")  # ✅ THÊM REDIS_URL
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
            if result == -2:
                fail_msg = (
                    f"❌ Đặt chỗ không thành công!\n"
                    f"   • Người dùng: {user}\n"
                    f"   • Trạm sạc: {charging_post}\n"
                    f"   • Xe: {car}\n"
                    f"   • Lý do: Bạn đã đặt chỗ trước đó\n"
                    f"\n💡 Anh/chị vui lòng thử lại sau hoặc chọn trạm sạc khác."
                )
                return fail_msg
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
    Xem thông tin các trạm sạc có sẵn, sắp xếp theo khoảng cách từ vị trí hiện tại
    - Tự động lấy GPS từ Redis
    - Gọi API Spring Boot với latitude, longitude để tính khoảng cách
    - NÉM HTTPException khi có lỗi
    """
    try:
        print(f"🌐 Đang xem thông tin các trạm sạc cho user {user}...")

        # ✅ BƯỚC 1: LẤY GPS TỪ REDIS
        print(f"📍 Bước 1: Lấy vị trí GPS từ Redis...")
        redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
        
        try:
            location_key = f"location:{user}"
            location_json = await redis.get(location_key)
            
            if not location_json:
                print(f"⚠️ Không tìm thấy GPS trong Redis cho user {user}")
                # Không có GPS → Gọi API không có tọa độ (sắp xếp mặc định)
                latitude = None
                longitude = None
            else:
                location_data = json.loads(location_json)
                latitude = location_data.get("latitude")
                longitude = location_data.get("longitude")
                print(f"✅ GPS từ Redis: lat={latitude}, lng={longitude}")
        
        finally:
            await redis.aclose()

        # ✅ BƯỚC 2: GỌI API SPRING BOOT
        print(f"🌐 Bước 2: Gọi API Spring Boot để lấy danh sách trạm...")
        
        async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
            # ✅ CẤU TRÚC REQUEST BODY
            request_body = {}
            
            if latitude is not None and longitude is not None:
                # Có GPS → Gửi kèm tọa độ để tính khoảng cách
                request_body = {
                    "latitude": latitude,
                    "longitude": longitude,
                    "radiusKm": 30.0,  # Bán kính mặc định 10km
                    "limit": 10        # Giới hạn 10 trạm
                }
                print(f"📤 Gửi với GPS: {request_body}")
            else:
                # Không có GPS → Gửi request rỗng hoặc giá trị mặc định
                request_body = {
                    "latitude": 0.0,
                    "longitude": 0.0,
                    "radiusKm": 10.0,
                    "limit": 10
                }
                print(f"📤 Gửi không có GPS (sẽ không tính khoảng cách)")
            
            # GỌI API
            response = await client.post(
                f"{BACKEND_URL}/api/charging/station/available",
                json=request_body,
                cookies={"jwt": jwt}
            )

            # ✅ XỬ LÝ LỖI HTTP
            if response.status_code != 200:
                error_detail = response.text or f"HTTP {response.status_code}"
                print(f"❌ API trả lỗi {response.status_code}: {error_detail}")
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"API Error: {error_detail}"
                )

            # ✅ BƯỚC 3: XỬ LÝ RESPONSE
            stations = response.json()
            print(f"✅ Nhận được {len(stations) if stations else 0} trạm từ API")
            
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
                    "active": station.get("active"),
                    "distance_km": station.get("distanceKm")  # ✅ Khoảng cách từ API
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

            print(f"✅ Xử lý xong: {len(success_msg)} trạm")
            
            # ✅ BƯỚC 4: FORMAT RESPONSE DỄ ĐỌC CHO LLM
            has_distance = success_msg[0].get("distance_km") is not None
            
            if has_distance:
                # Có khoảng cách → Hiển thị kèm khoảng cách
                formatted_response = f"📍 Tìm thấy {len(success_msg)} trạm sạc (đã sắp xếp theo khoảng cách):\n\n"
            else:
                # Không có khoảng cách
                formatted_response = f"📍 Tìm thấy {len(success_msg)} trạm sạc khả dụng:\n\n"
            
            for idx, station in enumerate(success_msg, 1):
                formatted_response += f"{idx}. 🏢 {station['station_name']} (ID: {station['station_id']})\n"
                formatted_response += f"   📍 Địa chỉ: {station['address']}\n"
                
                # ✅ HIỂN THỊ KHOẢNG CÁCH NẾU CÓ
                if station.get('distance_km') is not None:
                    distance = station['distance_km']
                    if distance < 1:
                        formatted_response += f"   🚗 Khoảng cách: {distance * 1000:.0f}m (rất gần)\n"
                    else:
                        formatted_response += f"   🚗 Khoảng cách: {distance:.2f}km\n"
                
                formatted_response += f"   🔌 Số cột sạc: {station['number_of_posts']}\n"
                
                # ✅ XỬ LÝ HIỂN THỊ TRỤ TRỐNG
                if station['total_available'] > 0:
                    formatted_response += f"   ✅ Cột khả dụng: {station['total_available']} cột ({', '.join(station['available_posts'])})\n"
                else:
                    formatted_response += f"   ❌ Không còn cột trống (tất cả {station['number_of_posts']} cột đã đặt)\n"
                
                formatted_response += f"   📅 Thành lập: {station['established_time']}\n"
                formatted_response += f"   🟢 Trạng thái: {'Đang hoạt động' if station['active'] else 'Ngừng hoạt động'}\n\n"
            
            # ✅ THÊM LƯU Ý NẾU KHÔNG CÓ GPS
            if not has_distance:
                formatted_response += "💡 Lưu ý: Em chưa có vị trí GPS của anh/chị nên không tính được khoảng cách. "
                formatted_response += "Anh/chị vui lòng cho phép truy cập vị trí để được gợi ý trạm gần nhất ạ.\n"
            
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

    except json.JSONDecodeError as e:
        print(f"❌ Lỗi parse JSON từ Redis: {e}")
        raise HTTPException(status_code=500, detail="Lỗi dữ liệu GPS không hợp lệ")

    except Exception as e:
        print(f"❌ Lỗi không xác định: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Lỗi hệ thống: {str(e)}")