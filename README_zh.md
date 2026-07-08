# By-Framework for Java

<div align="center">

[![Version](https://img.shields.io/badge/version-0.2.8-blue.svg)](pom.xml)
[![Java CI with Maven](https://github.com/beyonai/by-framework-java/actions/workflows/ci.yml/badge.svg)](https://github.com/beyonai/by-framework-java/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/java-21+-orange.svg)](pom.xml)
[![Redis](https://img.shields.io/badge/redis-7.0+-red.svg)](pom.xml)
</div>

<div align="center">

[English](README.md) | [中文](README_zh.md)

**重要链接:** [文档](https://beyonai.github.io/by-framework-docs) · [Python 版本](https://beyonai.github.io/by-framework-python) · [TypeScript 版本](https://beyonai.github.io/by-framework-ts)

</div>

## 📖 概述

**By-Framework** 是一个基于 Redis Streams 构建标的分布式高性能 Agent 调度引擎，专为多 Agent 系统设计。

## 传统架构的困境

传统 AI 应用架构在面对 Agent 场景时常面临三大挑战：

- **全链路同步阻塞 $\rightarrow$ 迫使用户“人肉盯看”** — 前端与后端强绑定，页面关闭即任务中断。用户无法跨端切换，工作流极易因网络波动或意外打断而前功尽弃。
- **无法支撑超长任务 $\rightarrow$ 导致系统“全程陪同”** — 面对数分钟甚至小时级的推理，调用方必须持续阻塞线程等待，不仅面临网关超时截断，更造成了严重的计算资源空转与浪费。
- **多 Agent 编排的中断恢复困局** — 在复杂级联调用中，一旦出现超时或中断，系统难以精准定位状态并恢复，开发者往往需自建极其复杂的持久化状态机。

## By-Framework 的方案

![Architecture Overview](./assets/img/architecture_zh.png)

By-Framework 通过**控制与数据平面分离**的异步架构解决上述问题：

- **指令异步化**：APP 通过 **Gateway Client** 将用户请求转化为控制指令并投入 **Control Queue**。由于是异步解耦，APP 无需阻塞等待，后端线程（完美配合 Java 21 虚拟线程）立即释放。
- **Agent 集群消费**：分布式的 **Agents** 集群竞争消费控制队列中的消息。通过逻辑寻址（Agent Type）自动实现负载均衡，天然支持动态扩缩容。
- **过程数据回传**：Agent 在执行过程中，将流式文本（Chunk）、状态变更（State）及产物（Artifact）异步推送到 **Data Queue**。APP 通过 **Gateway Client** 实时监听以获取任务进度，从而原生支持超长任务。
- **原生编排与中断恢复**：当 Agent 需要调用其他 Agent（编排）时，它会将新指令发往 **Control Queue**。这种基于消息的机制允许 Agent 在等待期间释放资源，并在收到回复后精准恢复上下文。

## 亮点

- 🚀 **异步与事件驱动** — 控制与数据分离于独立 Redis Stream，Worker 扩缩容与数据投递路径解耦
- 🧩 **现代 Java 支持** — 基于 Java 21 构建，完美支持虚拟线程，满足高并发 Agent 任务需求
- 🔌 **插件系统** — 支持热加载插件机制，提供生命周期钩子、工具、提示词和子 Agent 配置
- 🤝 **多 Agent 编排** — 内置 call_agent、scatter-gather 扇出和人机交互模式
- 🛡️ **生产就绪** — 竞争消费、优雅退出、消息持久化与执行状态追踪

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
    <artifactId>by-framework</artifactId>
    <version>0.2.8</version>
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

### Redis Cluster 模式

`RedisClient.getInstance()`（`ByaiWorker`/`GatewayClient` 在未显式传入 `RedisClient` 时使用的默认初始化路径）可以连接 Redis Cluster 而非单机 Redis。默认仍为单机模式——设置 `REDIS_MODE=cluster`，或者只配置 `REDIS_CLUSTER_HOST`，都会启用 Cluster 模式，因此现有 `gateway.redis.*` 用户不受影响。

| 环境变量 | 描述 | 默认值 |
| :--- | :--- | :--- |
| `REDIS_MODE` | `standalone` 或 `cluster`；未设置时，若配置了 `REDIS_CLUSTER_HOST` 则自动推断为 `cluster` | `standalone` |
| `REDIS_CLUSTER_HOST` | 逗号分隔的 `host:port` 节点列表，例如 `h1:6379,h2:6379`；只要配置了这个变量就足以切换到 Cluster 模式 | *(空)* |
| `REDIS_CLUSTER_NODES` | 格式同 `REDIS_CLUSTER_HOST`，在未设置 `REDIS_CLUSTER_HOST` 时使用 | *(空)* |
| `REDIS_USERNAME` / `REDIS_PASSWORD` | Cluster 认证凭据 | *(无)* |
| `REDIS_KEY_SCHEMA_VERSION` | 必须为 `v2` 才能使用 Cluster 模式 | `v1` |

Cluster 模式要求 `REDIS_KEY_SCHEMA_VERSION=v2`——v1 key 格式没有 Cluster hash tag，在 Cluster 下会触发 `CROSSSLOT` 错误。若选择 Cluster 模式但未设置 v2，`RedisClient` 会在构造时立即失败（不会尝试任何网络 I/O）。

需要强制刷新实例而非使用 `getInstance()` 懒加载单例的调用方（例如在框架重启时重置连接池），可以使用 `RedisClient.init(RedisConnectionConfig)`——与 `getInstance()` 相同的单机/集群选择逻辑和 v2 校验，但会像 `init(host, port, ...)` 系列重载一样，始终替换当前实例。

---

## 📄 许可证

本项目采用 Apache 2.0 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。
