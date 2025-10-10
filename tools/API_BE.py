import requests

def listUser_api(query: str = None) -> str:
    """
    Gọi API để lấy danh sách người dùng
    """
    try:
        print("Đang gọi API lấy danh sách user...")
        response = requests.get("http://localhost:8080/api/users/list")
        if response.status_code == 200:
            users = response.json()
            result = "📋 Danh sách người dùng trong hệ thống:\n"
            for i, user in enumerate(users, 1):
                username = user.get('userName') or "Chưa đặt tên"
                role = user.get('role', {}).get('roleName', 'Unknown')
                result += f"{i}. Username: {username}\n"
                result += f"   Role: {role}\n"
            return result
        else:
            return f"Lỗi {response.status_code}: {response.text}"
    except requests.exceptions.ConnectionError:
        return "Không thể kết nối đến server. Vui lòng kiểm tra server đã chạy chưa."
    except Exception as e:
        return f"Lỗi: {str(e)}"
def add_user_to_api(input_str: str) -> str:
    """
    Gọi API để thêm user mới
    Input format: "userName=xxx, password=xxx, role=xxx"
    """
    try:
        # Parse input string to dictionary
        data = {}
        for pair in input_str.split(','):
            key, value = pair.strip().split('=')
            # Convert username to userName if provided
            key = 'userName' if key.lower() == 'username' else key
            # Remove any surrounding quotes
            value = value.strip("'").strip('"')
            data[key.strip()] = value.strip()

        print(f"Đang thêm user mới với dữ liệu: {data}")
        
        # Chuẩn bị dữ liệu cho API
        role_id = 1 if data.get('role').lower() == 'admin' else 2  # 1 for ADMIN, 2 for USER
        
        user_data = {
            "userName": data.get('userName'),
            "password": data.get('password'),
            "role": {
                "roleId": role_id,
                "roleName": data.get('role', 'USER').upper()
            }
        }
        
        print(f"Dữ liệu gửi lên API: {user_data}")
        
        response = requests.post(
            "http://localhost:8080/api/users/add",
            json=user_data
        )
        
        if response.status_code == 200:
            return f"✅ Đã thêm thành công user {data.get('userName')}!"
        else:
            return f"❌ Lỗi {response.status_code}: {response.text}"
    except ValueError as e:
        return "❌ Lỗi format: Vui lòng nhập theo định dạng 'userName=xxx, password=xxx, role=xxx'"
    except requests.exceptions.ConnectionError:
        return "❌ Không thể kết nối đến server. Vui lòng kiểm tra server đã chạy chưa."
    except Exception as e:
        return f"❌ Lỗi: {str(e)}"