"""
Create QuestDB tables using Python
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psycopg2
from config.db_config import PG_CONNECTION_STRING
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_tables():
    """Create all EODHD tables in QuestDB"""
    
    # Read schema file
    schema_file = os.path.join(os.path.dirname(__file__), 'schemas.sql')
    with open(schema_file, 'r') as f:
        schema_sql = f.read()
    
    # Split into individual statements
    statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
    
    try:
        # Connect to QuestDB
        logger.info(f"Connecting to QuestDB...")
        conn = psycopg2.connect(PG_CONNECTION_STRING)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Execute each statement
        for i, statement in enumerate(statements, 1):
            # Skip empty statements and comments
            if statement.strip() and not statement.strip().startswith('--'):
                try:
                    logger.info(f"Executing statement {i}/{len(statements)}...")
                    cursor.execute(statement)
                    logger.info(f"✅ Statement {i} executed successfully")
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.warning(f"⚠️  Statement {i} - Table/Index already exists, skipping")
                    else:
                        logger.error(f"❌ Statement {i} failed: {e}")
                        raise
        
        cursor.close()
        conn.close()
        
        logger.info("\n" + "="*70)
        logger.info("✅ All tables created successfully!")
        logger.info("="*70)
        
        # List created tables
        logger.info("\nCreated tables:")
        logger.info("  1. eodhd_stock_data")
        logger.info("  2. eodhd_fundamentals")
        logger.info("  3. eodhd_corporate_actions")
        logger.info("  4. eodhd_calendar_events")
        logger.info("  5. eodhd_metadata")
        logger.info("  6. eodhd_stock_metadata (for update mode)")
        
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")
        raise

if __name__ == "__main__":
    create_tables()
