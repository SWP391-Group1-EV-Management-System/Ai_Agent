import json
from typing import Any, Dict, List, Union
from langchain.memory import ConversationBufferMemory  # Use the stable import path

class MemoryManager:
    def __init__(self):
        self.user_memories = {}

    def _to_text(self, content: Any) -> str:
        """Normalize message content to a readable string.
        Handles plain strings, multimodal lists, and dicts.
        """
        if isinstance(content, str):
            return content
        # Common multimodal shape: list of {"type": "text", "text": "..."}
        if isinstance(content, list):
            parts: List[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    # Prefer 'text' if present; otherwise dump json
                    if "text" in item and isinstance(item["text"], str):
                        parts.append(item["text"]) 
                    else:
                        parts.append(json.dumps(item, ensure_ascii=False))
                else:
                    parts.append(str(item))
            return "\n".join(parts)
        if isinstance(content, dict):
            return json.dumps(content, ensure_ascii=False)
        return str(content)

    def load_from_file(self, username):
        file_path = f"{username}_history.json"
        memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
            
            if not content:  # File rỗng
                self.user_memories[username] = memory
                return
            
            data = json.loads(content)
            
            # Kiểm tra version và format mới
            if isinstance(data, dict) and data.get("version") == 1:
                stored_username = data.get("username")
                if stored_username != username:
                    print(f"⚠️ Warning: File contains history for {stored_username}, but loading for {username}")
                
                messages = data.get("messages", [])
                for m in messages:
                    if not isinstance(m, dict):
                        continue
                    
                    msg_type = m.get("type")
                    content = self._to_text(m.get("content", ""))
                    
                    if msg_type == "human":
                        memory.chat_memory.add_user_message(content)
                    elif msg_type == "ai":
                        memory.chat_memory.add_ai_message(content)
            
            # Fallback cho format cũ
            elif isinstance(data, dict) and "messages" in data:
                for m in data["messages"]:
                    if not isinstance(m, dict):
                        continue
                    role = m.get("type")
                    content = self._to_text(m.get("content", ""))
                    if role == "human":
                        memory.chat_memory.add_user_message(content)
                    elif role == "ai":
                        memory.chat_memory.add_ai_message(content)
            
            self.user_memories[username] = memory
            
        except FileNotFoundError:
            # Nếu chưa có file thì tạo mới
            self.user_memories[username] = ConversationBufferMemory(memory_key="chat_history", return_messages=True)

    def save_to_file(self, username):
        from datetime import datetime
        memory = self.user_memories.get(username)
        if memory:
            # Tạo schema mới với version và timestamp
            messages = []
            for m in memory.chat_memory.messages:
                messages.append({
                    "type": m.type,
                    "content": self._to_text(m.content),
                    "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                })

            data = {
                "version": 1,
                "username": username,
                "messages": messages
            }
            
            file_path = f"{username}_history.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    def get_memory(self, username):
        if username not in self.user_memories:
            self.load_from_file(username)
        return self.user_memories[username]

    def clear_memory(self, username):
        if username in self.user_memories:
            del self.user_memories[username]    
    # hàm cập nhật liên tục lịch sử chat trong RAM ở vòng lặp
    def update_and_save(self, username, human_msg, ai_msg):
        from datetime import datetime
        
        # Lấy memory hiện tại hoặc tạo mới
        memory = self.user_memories.get(username)
        if not memory:
            memory = ConversationBufferMemory(memory_key="chat_history", return_messages=True)
            self.user_memories[username] = memory

        current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        # ✅ Thêm câu nói của người dùng và AI vào bộ nhớ RAM
        if human_msg:
            memory.chat_memory.add_user_message(human_msg)
        if ai_msg:
            memory.chat_memory.add_ai_message(ai_msg)

        # ✅ Lưu theo format mới với version và timestamp
        messages = []
        for m in memory.chat_memory.messages:
            messages.append({
                "type": m.type,
                "content": self._to_text(m.content),
                "timestamp": current_time
            })

        data = {
            "version": 1,
            "username": username,
            "messages": messages
        }

        with open(f"{username}_history.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
