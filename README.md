# Real-time Streaming Pricing System

A Kafka-style streaming architecture simulation using Python queues and Flask for real-time dynamic pricing.

## 🏗️ Architecture Overview

### Real-world Streaming Analogy

This system mimics enterprise streaming architectures like **Apache Kafka**, **Apache Pulsar**, or **AWS Kinesis**:

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│   Message Queue  │───▶│  Data Consumer  │
│  (IoT Sensors)  │    │    (Kafka)       │    │ (Pricing Engine)│
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
   User Events            Buffered Messages      Processed Data
   Market Data            Durable Storage        Real-time API
   System Metrics         High Throughput        Analytics
```

### Components

#### 1. **Data Producer** 📡
- **Simulates**: IoT sensors, user clickstreams, market data feeds
- **Function**: Generates demand events every second
- **Real-world analogy**: 
  - IoT devices sending telemetry
  - User interaction tracking
  - Financial market data feeds
  - Social media event streams

#### 2. **Message Queue** 📦
- **Technology**: Python `queue.Queue`
- **Simulates**: Apache Kafka topics/partitions
- **Features**:
  - **Buffering**: Handles producer/consumer rate differences
  - **Durability**: Messages persist until consumed
  - **Scalability**: Can handle multiple consumers
  - **Backpressure**: Prevents system overload

#### 3. **Data Consumer** ⚙️
- **Simulates**: Stream processing applications
- **Function**: Processes events in real-time, calculates pricing
- **Real-world analogy**:
  - Real-time analytics engines
  - Fraud detection systems
  - Dynamic pricing engines
  - Monitoring and alerting systems

#### 4. **REST API** 🌐
- **Function**: Provides real-time access to processed data
- **Endpoints**:
  - `/price` - Current pricing data
  - `/stream/stats` - Streaming statistics
  - `/stream/events` - Recent processed events
  - `/health` - System health check

## 🚀 Getting Started

### Prerequisites
```bash
pip install -r requirements.txt
```

### Running the System

1. **Start the streaming server**:
```bash
python streaming_app.py
```

2. **Open the dashboard**:
   - Open `streaming_dashboard.html` in your browser
   - Or serve it: `python -m http.server 8000`

### What You'll See

- **Real-time pricing updates** every 2 seconds
- **Streaming statistics** showing message throughput
- **Live event stream** with recent processed events
- **Connection status** indicating system health

## 📊 Key Features

### Real-time Data Processing
- **Producer**: Generates demand events with realistic patterns
- **Queue**: Buffers up to 1000 messages
- **Consumer**: Processes events with pricing calculations
- **API**: Serves latest processed data

### Demand Simulation
- **Trend-based**: Demand follows trends with random variations
- **Realistic patterns**: Mimics real market behavior
- **Event metadata**: Timestamps, event IDs, source tracking

### Streaming Statistics
- **Throughput**: Messages per second
- **Queue depth**: Current buffer size
- **Event counts**: Produced vs consumed messages
- **System uptime**: Runtime tracking

## 🔧 Technical Details

### Threading Model
```python
# Main Thread: Flask API server
# Producer Thread: Generates events every 1 second
# Consumer Thread: Processes events from queue
```

### Data Flow
1. **Producer** creates demand event → puts in queue
2. **Consumer** gets event from queue → processes with pricing logic
3. **Processed data** stored in latest messages buffer
4. **Flask API** serves data from buffer to web dashboard

### Queue Management
- **Max size**: 1000 messages (prevents memory overflow)
- **Latest buffer**: 10 messages (for API access)
- **Thread-safe**: All queue operations are atomic

## 🌍 Real-world Applications

This architecture pattern is used in:

### **E-commerce**
- Real-time inventory updates
- Dynamic pricing based on demand
- User behavior tracking

### **Finance**
- Stock price streaming
- Fraud detection
- Risk calculations

### **IoT**
- Sensor data processing
- Real-time monitoring
- Predictive maintenance

### **Social Media**
- Live feed updates
- Trend analysis
- Content recommendations

## 📈 Scaling Considerations

### Production Enhancements
- **Replace Python queue** with Apache Kafka/RabbitMQ
- **Add multiple consumers** for parallel processing
- **Implement persistence** for data recovery
- **Add monitoring** with Prometheus/Grafana
- **Containerize** with Docker/Kubernetes

### Performance Optimizations
- **Batch processing** for higher throughput
- **Compression** for network efficiency
- **Partitioning** for horizontal scaling
- **Caching** for frequently accessed data

## 🔍 Monitoring & Debugging

### Log Messages
The system logs key events:
- Producer: "Produced event: {event_id} - Demand: {demand}"
- Consumer: "Consumed event: {event_id}"
- System: "Streaming system started/stopped"

### Health Checks
- `/health` endpoint shows system status
- Connection status in dashboard
- Queue size monitoring
- Error tracking in logs

## 🎯 Learning Outcomes

This system demonstrates:
- **Producer-Consumer pattern** in streaming systems
- **Queue-based architecture** for decoupled systems
- **Real-time data processing** concepts
- **Threading** for concurrent operations
- **REST API** integration with streaming backends

Perfect for understanding modern streaming architectures before implementing with enterprise tools like Kafka, Pulsar, or cloud streaming services!
