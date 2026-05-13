# 🚀 by-framework-java

[English](README.md) | [中文](README_zh.md)

[![Version](https://img.shields.io/badge/version-0.2.7-blue.svg)](pom.xml)
[![Java CI with Maven](https://github.com/beyonai/by-framework-java/actions/workflows/ci.yml/badge.svg)](https://github.com/beyonai/by-framework-java/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/java-21+-orange.svg)](pom.xml)
[![Redis](https://img.shields.io/badge/redis-7.0+-red.svg)](pom.xml)

## 📖 概述

**by-framework-java** 是一个基于 Redis Streams 构建的分布式、高性能 Agent 调度引擎的 Java 实现。它为【超级助手】、【数字员工】等具备自驱编排、沙箱隔离能力的智能体服务提供了标准化的开发框架和运行环境。

---

## 📋 目录

- [✨ 核心特性](#-核心特性)
- [🏗️ 核心架构](#️-核心架构)
- [📦 安装](#-安装)
- [🚀 快速上手](#-快速上手)
- [💡 深入理解](#-深入理解)
- [📡 发送任务](#-发送任务)
- [🧪 示例](#-示例)
- [🛠️ 配置参考](#️-配置参考)

---

## ✨ 核心特性

- ⚡ **现代 Java 支持**: 基于 Java 21 构建，完美支持虚拟线程，满足高并发 Agent 任务需求。
- 🧩 **高度可扩展**: 内置扩展系统，支持动态注册自定义命令、工具和提示词。
- 📊 **状态管控**: 完善的 `AgentContext` 支持，轻松实现流式输出、状态流转和结果处理。
- 🔄 **解耦架构**: 采用"控制流-数据流分离"设计，支持多语言 Worker 混合集群水平扩展。

---

## 🏗️ 核心架构

系统采用事件驱动设计，高度解耦：

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

## 📦 安装

### 前置要求

- Java 21+
- Maven 3.8+
- Redis 7.0+

### Maven 配置

```xml
<dependency>
    <groupId>com.iwhaleai.byai</groupId>
    <artifactId>gateway-sdk-java</artifactId>
    <version>0.2.7</version>
</dependency>
```

---

## 🚀 快速上手

### 1. 创建一个简单的 Agent Worker

继承 `GatewayWorker` 并实现核心逻辑：

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
            context.emitChunk("正在处理您的请求...\n");
            return "任务完成";
        }
        return null;
    }

    public static void main(String[] args) {
        new WorkerRunner(new MyAssistant("worker-01")).start();
    }
}
```

---

## 📡 发送任务

```java
ByaiGatewayClient client = new ByaiGatewayClient(RedisClient.getInstance());
client.sendMessage("chat_agent", "session-123", "北京今天天气如何？", "tenant-001", ActionType.ASK_AGENT, null, null, null, null, null);
```

---

## 🛠️ 配置参考

| 配置项 | 环境变量 | 描述 | 默认值 |
| :--- | :--- | :--- | :--- |
| `gateway.redis.host` | `REDIS_HOST` | Redis 服务器地址 | `localhost` |
| `gateway.redis.port` | `REDIS_PORT` | Redis 端口 | `6379` |
| `gateway.worker.concurrency` | `WORKER_CONCURRENCY` | Worker 最大并发数 | `50` |

---

## 📄 许可证

本项目采用 Apache 2.0 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。
