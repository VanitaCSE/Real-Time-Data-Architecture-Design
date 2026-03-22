import sqlite3
import os
from datetime import datetime

def setup_database():
    """Create SQLite database with demand/price schema"""
    
    # Remove existing database if it exists
    if os.path.exists('pricing_data.db'):
        os.remove('pricing_data.db')
        print("Removed existing database")
    
    # Create new database connection
    conn = sqlite3.connect('pricing_data.db')
    cursor = conn.cursor()
    
    # Create pricing_events table
    cursor.execute('''
        CREATE TABLE pricing_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            demand INTEGER NOT NULL,
            price REAL NOT NULL,
            pricing_tier TEXT NOT NULL,
            event_id INTEGER NOT NULL,
            source TEXT DEFAULT 'demand_sensor_001',
            trend TEXT,
            processed_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            moving_avg REAL,
            volatility REAL,
            trend_strength REAL,
            window_min INTEGER,
            window_max INTEGER
        )
    ''')
    
    # Create indexes for better query performance
    cursor.execute('CREATE INDEX idx_timestamp ON pricing_events(timestamp)')
    cursor.execute('CREATE INDEX idx_demand ON pricing_events(demand)')
    cursor.execute('CREATE INDEX idx_pricing_tier ON pricing_events(pricing_tier)')
    cursor.execute('CREATE INDEX idx_event_id ON pricing_events(event_id)')
    cursor.execute('CREATE INDEX idx_moving_avg ON pricing_events(moving_avg)')
    
    # Create aggregated_daily_stats table for pre-computed daily stats
    cursor.execute('''
        CREATE TABLE aggregated_daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            avg_demand REAL,
            avg_price REAL,
            min_demand INTEGER,
            max_demand INTEGER,
            min_price REAL,
            max_price REAL,
            total_events INTEGER,
            tier_low_count INTEGER,
            tier_medium_count INTEGER,
            tier_high_count INTEGER,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(date)
        )
    ''')
    
    conn.commit()
    conn.close()
    
    print("✅ Database setup complete!")
    print("📊 Created tables: pricing_events, aggregated_daily_stats")
    print("🔍 Created indexes for optimal query performance")

if __name__ == "__main__":
    setup_database()
