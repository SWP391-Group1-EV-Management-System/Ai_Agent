"""
Backend API Integration - Async Version
Sử dụng httpx thay vì requests để tương thích với async/await
"""

import httpx
import os
from typing import Dict, Any

# Backend API configuration
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8080/api")
API_TIMEOUT = 30

# ==================== ASYNC API FUNCTIONS ====================

async def listUser_api(query: str = None) -> str:
    """
    Gọi API để lấy danh sách người dùng (ASYNC version)
    
    Args:
        query: Tìm kiếm người dùng (optional)
    
    Returns:
        Danh sách người dùng dạng string
    """
    try:
        print("Đang gọi API lấy danh sách user...")
        
        async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
            response = await client.get(f"{BACKEND_URL}/users/list")
            
            if response.status_code == 200:
                users = response.json()
                
                if not users:
                    return "📋 Danh sách người dùng: Chưa có người dùng nào trong hệ thống."
                
                result = "📋 Danh sách người dùng trong hệ thống:\n\n"
                for i, user in enumerate(users, 1):
                    username = user.get('userName') or "Chưa đặt tên"
                    role = user.get('role', {}).get('roleName', 'Unknown')
                    result += f"{i}. Username: {username}\n"
                    result += f"   Role: {role}\n\n"
                
                return result
            else:
                error_msg = f"❌ Lỗi {response.status_code}: {response.text}"
                print(error_msg)
                return error_msg
                
    except httpx.ConnectError:
        error_msg = "❌ Không thể kết nối đến server. Vui lòng kiểm tra server đã chạy chưa."
        print(error_msg)
        return error_msg
    except httpx.TimeoutException:
        error_msg = "❌ Timeout: Server mất quá nhiều thời gian để phản hồi."
        print(error_msg)
        return error_msg
    except Exception as e:
        error_msg = f"❌ Lỗi: {str(e)}"
        print(error_msg)
        return error_msg


async def add_user_to_api(input_str: str = None, **kwargs) -> str:
    """
    Gọi API để thêm user mới (ASYNC version)
    
    Args:
        input_str: Format "userName=xxx, password=xxx, role=xxx"
        **kwargs: Hoặc dùng kwargs trực tiếp (userName, password, role)
    
    Returns:
        Kết quả thêm user
    """
    try:
        # Parse input
        if input_str:
            # Parse từ string
            data = {}
            for pair in input_str.split(','):
                if '=' in pair:
                    key, value = pair.strip().split('=', 1)
                    key = 'userName' if key.lower() == 'username' else key
                    value = value.strip("'").strip('"')
                    data[key.strip()] = value.strip()
        else:
            # Dùng kwargs
            data = {
                'userName': kwargs.get('userName') or kwargs.get('username'),
                'password': kwargs.get('password'),
                'role': kwargs.get('role', 'USER')
            }
        
        # Validate required fields
        if not data.get('userName'):
            return "❌ Lỗi: Thiếu userName"
        if not data.get('password'):
            return "❌ Lỗi: Thiếu password"
        
        print(f"Đang thêm user mới: {data.get('userName')}")
        
        # Chuẩn bị dữ liệu cho API
        role_name = data.get('role', 'USER').upper()
        role_id = 1 if role_name == 'ADMIN' else 2  # 1 for ADMIN, 2 for USER
        
        user_data = {
            "userName": data.get('userName'),
            "password": data.get('password'),
            "role": {
                "roleId": role_id,
                "roleName": role_name
            }
        }
        
        print(f"Dữ liệu gửi lên API: {user_data}")
        
        # Call API
        async with httpx.AsyncClient(timeout=API_TIMEOUT) as client:
            response = await client.post(
                f"{BACKEND_URL}/users/add",
                json=user_data
            )
            
            if response.status_code == 200:
                success_msg = f"✅ Đã thêm thành công user '{data.get('userName')}' với role {role_name}!"
                print(success_msg)
                return success_msg
            else:
                error_msg = f"❌ Lỗi {response.status_code}: {response.text}"
                print(error_msg)
                return error_msg
                
    except ValueError as e:
        error_msg = "❌ Lỗi format: Vui lòng nhập theo định dạng 'userName=xxx, password=xxx, role=xxx'"
        print(error_msg)
        return error_msg
    except httpx.ConnectError:
        error_msg = "❌ Không thể kết nối đến server. Vui lòng kiểm tra server đã chạy chưa."
        print(error_msg)
        return error_msg
    except httpx.TimeoutException:
        error_msg = "❌ Timeout: Server mất quá nhiều thời gian để phản hồi."
        print(error_msg)
        return error_msg
    except Exception as e:
        error_msg = f"❌ Lỗi: {str(e)}"
        print(error_msg)
        return error_msg


# ==================== MOCK DATA (Backup nếu server không chạy) ====================

async def listUser_api_mock(query: str = None) -> str:
    """Mock data for testing"""
    import asyncio
    await asyncio.sleep(0.1)
    
    return """📋 Danh sách người dùng trong hệ thống:

1. Username: admin
   Role: ADMIN

2. Username: user1
   Role: USER

3. Username: user2
   Role: USER
"""


async def add_user_to_api_mock(input_str: str = None, **kwargs) -> str:
    """Mock function for adding user"""
    import asyncio
    await asyncio.sleep(0.1)
    
    username = kwargs.get('userName') or kwargs.get('username', 'test_user')
    return f"✅ Đã thêm thành công user '{username}' với role USER! (MOCK MODE)"


# ==================== SWITCH BETWEEN REAL/MOCK ====================

USE_MOCK = os.getenv("USE_MOCK_API", "false").lower() == "true"

if USE_MOCK:
    print("⚠️  WARNING: Using MOCK API (no real backend calls)")
    # Ghi đè functions với mock versions
    listUser_api = listUser_api_mock
    add_user_to_api = add_user_to_api_mock


# ==================== TEST ====================

if __name__ == "__main__":
    import asyncio
    
    async def test():
        print("=" * 60)
        print("Testing API Functions")
        print("=" * 60)
        
        # Test list users
        print("\n1. Testing listUser_api()...")
        result = await listUser_api()
        print(result)
        
        # Test add user
        print("\n2. Testing add_user_to_api()...")
        result = await add_user_to_api(
            input_str="userName=testuser, password=123456, role=USER"
        )
        print(result)
        
        # Test with kwargs
        print("\n3. Testing add_user_to_api() with kwargs...")
        result = await add_user_to_api(
            userName="testuser2",
            password="password123",
            role="ADMIN"
        )
        print(result)
    
    asyncio.run(test())