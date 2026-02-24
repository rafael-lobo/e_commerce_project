import duckdb
import logging
from datetime import datetime

class IdempotencyHandler:
    def __init__(self) -> None:
        self.logger = logging.getLogger('IdempotencyHandler')

        self.conn = duckdb.connect(database='./processed_messages.db', read_only=False)
        # statement below for dev only
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_messages (
                order_id VARCHAR PRIMARY KEY, 
                message_id VARCHAR, 
                message_data JSON, 
                acknowledged BOOLEAN,
                created_at TIMESTAMP
            )
        """)

    def store_message(self, order_id: str, message_id: str, message_data: dict) -> None:
        try:
            self.conn.execute(f"""
                INSERT INTO processed_messages (order_id, message_id, message_data, acknowledged, created_at)
                VALUES ('{order_id}', '{message_id}', '{message_data}', NULL, '{datetime.now().isoformat()}')
            """)
            self.conn.commit()
            self.logger.info(f'Message with order_id={order_id} stored successfully!')
        except Exception:
            self.logger.exception(f'Unexpected error storing message') 
            raise
    
    def check_ack_message_exists(self, order_id: str) -> bool:
        try:
            result = self.conn.execute(f"""
                SELECT order_id FROM processed_messages WHERE order_id = '{order_id}' AND acknowledged = TRUE
            """).fetchone()
            return result is not None
        except Exception:
            self.logger.exception(f'Unexpected error checking message') 
            raise
    
    def update_message_ack_status(self, order_id: str, ack_status: bool) -> None:
        try:
            self.conn.execute(f"""
                UPDATE processed_messages SET acknowledged = {ack_status} WHERE order_id = '{order_id}'
            """)
            self.conn.commit()
            self.logger.info(f'Message with order_id={order_id} updated with acknowledgment status: {ack_status}')
        except Exception:
            self.logger.exception(f'Unexpected error updating message') 
            raise
    

        
        