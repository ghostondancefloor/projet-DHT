import hashlib

class Data:
    """Represents a message with a unique hash."""
    
    def __init__(self, content):
        self.content = content
        self.msg_id = self.hash_data(content)

    @staticmethod
    def hash_data(content):
        """Generate a SHA-1 hash for the message."""
        return hashlib.sha1(content.encode()).hexdigest()[:8] 