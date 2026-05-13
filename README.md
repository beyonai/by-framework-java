# 🚀 by-framework-java

[English](README.md) | [中文](README_zh.md)

[![Version](https://img.shields.io/badge/version-0.2.7-blue.svg)](pom.xml)
[![Java CI with Maven](https://github.com/beyonai/by-framework-java/actions/workflows/ci.yml/badge.svg)](https://github.com/beyonai/by-framework-java/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/java-21+-orange.svg)](pom.xml)
[![Redis](https://img.shields.io/badge/redis-7.0+-red.svg)](pom.xml)

## 📖 Overview

**by-framework-java** is a Java implementation of a distributed, high-performance Agent scheduling engine built on Redis Streams. It provides a standardized development framework and runtime environment for agent services such as "Super Assistants" and "Digital Employees" with self-driven orchestration and sandbox isolation capabilities.

---

## 📋 Contents

- [✨ Features](#-features)
- [🏗️ Architecture](#️-architecture)
- [📦 Installation](#-installation)
- [🚀 Quick Start](#-quick-start)
- [💡 Deep Dive](#-deep-dive)
- [📡 Sending Tasks](#-sending-tasks)
- [🧪 Examples](#-examples)
- [🛠️ Configuration](#️-configuration)

---

## ✨ Features

- ⚡ **Modern Java Support**: Built on Java 21 with full support for virtual threads to meet high-concurrency agent task requirements.
- 🧩 **Highly Extensible**: Built-in extension system, supporting dynamic registration of custom commands, tools, and prompts.
- 📊 **State Management**: Robust `AgentContext` support for easy streaming output, state transitions, and artifact handling.
- 🔄 **Decoupled Architecture**: Features a "Control Flow - Data Flow Separation" design, supporting horizontal scaling of multi-language Worker clusters.

---

## 🏗️ Architecture

The system adopts an event-driven design, highly decoupled:

```
┌─────────────┐       ┌──────────────┐       ┌──────────────┐
│   Client    │──────▶│  Redis Input │──────▶│   Gateway    │
│ (Java SDK)  │       │     MQ       │       │   Worker     │
└─────────────┘       └──────────────┘       └──────┬───────┘
        ▲                                              │
        │                                              │
        │                                              ▼
┌─────────────┐       ┌──────────────┐       ┌──────────────┐
│   Backend   │◀─────│  Redis Data   │◀─────│   Business   │
│  (WebSocket)│       │     MQ       │       │    Logic     │
└─────────────┘       └──────────────┘       └──────────────┘
```

---

## 📦 Installation

### Prerequisites

- Java 21+
- Maven 3.8+
- Redis 7.0+

### Maven Configuration

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.iwhaleai.byai</groupId>
    <artifactId>gateway-sdk-java</artifactId>
    <version>0.2.7</version>
</dependency>
```

---

## 🚀 Quick Start

### 1. Create a Simple Agent Worker

Extend `GatewayWorker` and implement the core logic:

```java
import com.iwhaleai.byai.gateway.sdk.core.protocol.AskAgentCommand;
import com.iwhaleai.byai.gateway.sdk.core.protocol.GatewayCommand;
import com.iwhaleai.byai.gateway.sdk.worker.AgentContext;
import com.iwhaleai.byai.gateway.sdk.worker.GatewayWorker;
import com.iwhaleai.byai.gateway.sdk.worker.WorkerRunner;

import java.util.List;

public class MyAssistant extends GatewayWorker {

    public MyAssistant(String workerId) {
        super(workerId);
    }

    @Override
    public List<String> getAgentTypes() {
        return List.of("chat_agent");
    }

    @Override
    public Object processCommand(GatewayCommand command, AgentContext context) {
        if (command instanceof AskAgentCommand askCommand) {
            context.emitChunk("Processing your request...\n");
            return "Task completed";
        }
        return null;
    }

    public static void main(String[] args) {
        new WorkerRunner(new MyAssistant("worker-01")).start();
    }
}
```

---

## 📡 Sending Tasks

```java
ByaiGatewayClient client = new ByaiGatewayClient(RedisClient.getInstance());
client.sendMessage("chat_agent", "session-123", "How is the weather?", "tenant-001", ActionType.ASK_AGENT, null, null, null, null, null);
```

---

## 🛠️ Configuration

| Property | Environment Variable | Description | Default |
| :--- | :--- | :--- | :--- |
| `gateway.redis.host` | `REDIS_HOST` | Redis server address | `localhost` |
| `gateway.redis.port` | `REDIS_PORT` | Redis port | `6379` |
| `gateway.worker.concurrency` | `WORKER_CONCURRENCY` | Maximum worker concurrency | `50` |

---

## 📄 License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
